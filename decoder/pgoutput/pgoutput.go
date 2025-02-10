package pgoutput

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"time"

	"repo.nusatek.id/sugeng/walstreamer/model"
)

// PostgreSQL OIDs for common types
const (
	OIDJson        = 114
	OIDJsonb       = 3802
	OIDArray       = 2277
	OIDBoolean     = 16
	OIDInt8        = 20
	OIDInt4        = 23
	OIDText        = 25
	OIDFloat4      = 700
	OIDFloat8      = 701
	OIDTimestamp   = 1114
	OIDTimestamptz = 1184
	OIDDate        = 1082
)

type relationInfo struct {
	schema  string
	table   string
	columns []model.ColumnDefinition
}

type PgOutputDecoder struct {
	relations map[uint32]relationInfo
}

func (d *PgOutputDecoder) Decode(data []byte) (*model.Message, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty data")
	}

	msgType := data[0]
	switch msgType {
	case 'B': // Begin
		return d.parseBegin(data)
	case 'C': // Commit
		return d.parseCommit(data)
	case 'R': // Relation
		return d.parseRelation(data)
	case 'I': // Insert
		return d.parseInsert(data)
	case 'U': // Update
		return d.parseUpdate(data)
	case 'D': // Delete
		return d.parseDelete(data)
	case 'T': // Truncate
		return d.parseTruncate(data)
	default:
		return nil, fmt.Errorf("unknown message type: %c", msgType)
	}
}

func (d *PgOutputDecoder) parseRelation(data []byte) (*model.Message, error) {
	if len(data) < 11 {
		return nil, fmt.Errorf("invalid relation message length: %d", len(data))
	}

	offset := 1
	relationID := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4
	schemaName := string(data[offset : offset+bytes.IndexByte(data[offset:], 0)])
	offset += len(schemaName) + 1
	tableName := string(data[offset : offset+bytes.IndexByte(data[offset:], 0)])
	offset += len(tableName) + 1
	_ = data[offset] // Skip replica identity
	offset++
	numColumns := binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	columns := make([]model.ColumnDefinition, 0, numColumns)

	for i := 0; i < int(numColumns); i++ {
		if len(data) < offset+6 {
			return nil, fmt.Errorf("invalid relation message: incomplete column data")
		}
		flags := data[offset]
		offset++
		columnName := string(data[offset : offset+bytes.IndexByte(data[offset:], 0)])
		offset += len(columnName) + 1
		dataTypeID := binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
		typeMod := binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4

		column := model.ColumnDefinition{
			Name:     columnName,
			TypeOid:  dataTypeID,
			Type:     d.getTypeName(dataTypeID),
			Order:    i,
			Optional: typeMod == 0,
			IsKey:    flags&1 != 0,
		}
		columns = append(columns, column)
	}

	d.relations[relationID] = relationInfo{
		schema:  schemaName,
		table:   tableName,
		columns: columns,
	}

	return &model.Message{
		Operation: "RELATION",
		Schema:    schemaName,
		Table:     tableName,
		Columns:   columns,
		Timestamp: time.Now().UnixNano(),
		Raw:       data,
	}, nil
}

func (d *PgOutputDecoder) parseBegin(data []byte) (*model.Message, error) {
	if len(data) < 21 {
		return nil, fmt.Errorf("invalid begin message length: %d", len(data))
	}

	xid := binary.BigEndian.Uint32(data[17:21])

	return &model.Message{
		TransactionID: fmt.Sprintf("%d", xid),
		Operation:     "BEGIN",
		Timestamp:     time.Now().UnixNano(),
		Raw:           data,
	}, nil
}

func (d *PgOutputDecoder) parseCommit(data []byte) (*model.Message, error) {
	return &model.Message{
		Operation: "COMMIT",
		Timestamp: time.Now().UnixNano(),
		Raw:       data,
	}, nil
}

func (d *PgOutputDecoder) parseInsert(data []byte) (*model.Message, error) {
	if len(data) < 6 {
		return nil, fmt.Errorf("invalid insert message length: %d", len(data))
	}
	relationID := binary.BigEndian.Uint32(data[1:5])
	tupleType := data[5]

	if tupleType != 'N' {
		return nil, fmt.Errorf("invalid insert message tuple type: %c", tupleType)
	}

	tupleData, err := d.parseTupleData(data[6:], relationID)
	if err != nil {
		return nil, err
	}

	relation, ok := d.relations[relationID]
	if !ok {
		return nil, fmt.Errorf("relation with ID %d not found", relationID)
	}

	return &model.Message{
		Operation: "INSERT",
		Schema:    relation.schema,
		Table:     relation.table,
		After:     tupleData,
		Columns:   relation.columns,
		Timestamp: time.Now().UnixNano(),
		Raw:       data,
	}, nil
}

func (d *PgOutputDecoder) parseUpdate(data []byte) (*model.Message, error) {
	if len(data) < 6 {
		return nil, fmt.Errorf("invalid update message length: %d", len(data))
	}

	relationID := binary.BigEndian.Uint32(data[1:5])
	tupleType := data[5]

	var oldTuple, newTuple map[string]interface{}
	var err error

	offset := 6
	if tupleType == 'K' || tupleType == 'O' {
		oldTuple, err = d.parseTupleData(data[offset:], relationID)
		if err != nil {
			return nil, fmt.Errorf("failed to parse old tuple: %w", err)
		}
		offset += d.getTupleDataLength(data[offset:])
	}

	if len(data) > offset {
		newTuple, err = d.parseTupleData(data[offset:], relationID)
		if err != nil {
			return nil, fmt.Errorf("failed to parse new tuple: %w", err)
		}
	}

	relation, ok := d.relations[relationID]
	if !ok {
		return nil, fmt.Errorf("relation with ID %d not found", relationID)
	}

	return &model.Message{
		Operation: "UPDATE",
		Schema:    relation.schema,
		Table:     relation.table,
		Before:    oldTuple,
		After:     newTuple,
		Columns:   relation.columns,
		Timestamp: time.Now().UnixNano(),
		Raw:       data,
	}, nil
}

func (d *PgOutputDecoder) parseDelete(data []byte) (*model.Message, error) {
	if len(data) < 6 {
		return nil, fmt.Errorf("invalid delete message length: %d", len(data))
	}

	relationID := binary.BigEndian.Uint32(data[1:5])
	tupleType := data[5]

	if tupleType != 'K' && tupleType != 'O' {
		return nil, fmt.Errorf("invalid delete message tuple type: %c", tupleType)
	}

	oldTuple, err := d.parseTupleData(data[6:], relationID)
	if err != nil {
		return nil, err
	}

	relation, ok := d.relations[relationID]
	if !ok {
		return nil, fmt.Errorf("relation with ID %d not found", relationID)
	}

	return &model.Message{
		Operation: "DELETE",
		Schema:    relation.schema,
		Table:     relation.table,
		Before:    oldTuple,
		Columns:   relation.columns,
		Timestamp: time.Now().UnixNano(),
		Raw:       data,
	}, nil
}

func (d *PgOutputDecoder) parseTupleData(data []byte, relationID uint32) (map[string]interface{}, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("invalid tuple data length: %d", len(data))
	}

	relation, ok := d.relations[relationID]
	if !ok {
		return nil, fmt.Errorf("relation with ID %d not found", relationID)
	}

	numColumns := binary.BigEndian.Uint16(data[0:2])
	offset := 2

	columns := make(map[string]interface{})

	for i := 0; i < int(numColumns); i++ {
		if len(data) < offset+1 {
			return nil, fmt.Errorf("invalid tuple data: missing column type at offset %d", offset)
		}

		colType := data[offset]
		offset++

		switch colType {
		case 'n': // Null value
			columns[relation.columns[i].Name] = nil
		case 'u': // Unchanged TOASTed value
			columns[relation.columns[i].Name] = nil
		case 't': // Text formatted value
			if len(data) < offset+4 {
				return nil, fmt.Errorf("invalid tuple data: missing column length at offset %d", offset)
			}
			colLen := binary.BigEndian.Uint32(data[offset : offset+4])
			offset += 4
			if len(data) < offset+int(colLen) {
				return nil, fmt.Errorf("invalid tuple data: missing column value at offset %d", offset)
			}
			value, err := d.parseValue(data[offset:offset+int(colLen)], relation.columns[i].TypeOid)
			if err != nil {
				return nil, fmt.Errorf("failed to parse column value: %w", err)
			}
			columns[relation.columns[i].Name] = value
			offset += int(colLen)
		default:
			return nil, fmt.Errorf("invalid tuple data column type: %c", colType)
		}
	}

	return columns, nil
}

func (d *PgOutputDecoder) getTupleDataLength(data []byte) int {
	if len(data) < 2 {
		return 0
	}

	numColumns := binary.BigEndian.Uint16(data[0:2])
	offset := 2

	for i := 0; i < int(numColumns); i++ {
		if len(data) < offset+1 {
			return offset
		}

		colType := data[offset]
		offset++

		if colType == 't' {
			if len(data) < offset+4 {
				return offset
			}
			colLen := binary.BigEndian.Uint32(data[offset : offset+4])
			offset += 4 + int(colLen)
		}
	}

	return offset
}

func (d *PgOutputDecoder) parseValue(data []byte, oid uint32) (interface{}, error) {
	if len(data) == 0 {
		return nil, nil
	}

	switch oid {
	case OIDJson, OIDJsonb:
		var v interface{}
		if err := json.Unmarshal(data, &v); err != nil {
			return nil, fmt.Errorf("failed to parse JSON: %w", err)
		}
		return v, nil
	case OIDBoolean:
		return data[0] == 1, nil
	case OIDInt8:
		return binary.BigEndian.Uint64(data), nil
	case OIDInt4:
		return binary.BigEndian.Uint32(data), nil
	case OIDFloat4:
		bits := binary.BigEndian.Uint32(data)
		return float32(bits), nil
	case OIDFloat8:
		bits := binary.BigEndian.Uint64(data)
		return float64(bits), nil
	case OIDTimestamp, OIDTimestamptz:
		microsecs := binary.BigEndian.Uint64(data)
		return time.Unix(0, int64(microsecs)*1000).Format(time.RFC3339Nano), nil
	case OIDDate:
		days := binary.BigEndian.Uint32(data)
		return time.Unix(int64(days*86400), 0).Format("2006-01-02"), nil
	case OIDText:
		return string(data), nil
	default:
		// For unknown types, return as string
		return string(data), nil
	}
}

func (d *PgOutputDecoder) getTypeName(oid uint32) string {
	switch oid {
	case OIDJson:
		return "json"
	case OIDJsonb:
		return "jsonb"
	case OIDBoolean:
		return "boolean"
	case OIDInt8:
		return "bigint"
	case OIDInt4:
		return "integer"
	case OIDFloat4:
		return "real"
	case OIDFloat8:
		return "double precision"
	case OIDTimestamp:
		return "timestamp"
	case OIDTimestamptz:
		return "timestamptz"
	case OIDDate:
		return "date"
	case OIDText:
		return "text"
	default:
		return fmt.Sprintf("unknown_%d", oid)
	}
}

func (d *PgOutputDecoder) parseTruncate(data []byte) (*model.Message, error) {
	if len(data) < 7 {
		return nil, fmt.Errorf("invalid truncate message length: %d", len(data))
	}

	offset := 1
	numRelations := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4
	_ = data[offset] // Skip options
	offset++

	relationIDs := make([]uint32, numRelations)
	for i := 0; i < int(numRelations); i++ {
		if len(data) < offset+4 {
			return nil, fmt.Errorf("invalid truncate message: incomplete relation IDs")
		}
		relationIDs[i] = binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
	}

	if len(relationIDs) == 0 {
		return nil, fmt.Errorf("no relations in truncate message")
	}

	relation, ok := d.relations[relationIDs[0]]
	if !ok {
		return nil, fmt.Errorf("relation with ID %d not found", relationIDs[0])
	}

	return &model.Message{
		Operation: "TRUNCATE",
		Schema:    relation.schema,
		Table:     relation.table,
		Columns:   relation.columns,
		Timestamp: time.Now().UnixNano(),
		Raw:       data,
	}, nil
}

func NewPgOutputDecoder() *PgOutputDecoder {
	return &PgOutputDecoder{
		relations: make(map[uint32]relationInfo),
	}
}
