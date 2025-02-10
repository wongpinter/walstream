package pgoutput

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

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
	OIDInt2        = 21
	OIDVarchar     = 1043
	OIDBpchar      = 1042
	OIDByteArray   = 17
	OIDNumeric     = 1700
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
		//typeMod := binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4

		column := model.ColumnDefinition{
			Name: columnName,
			Type: model.DataType{
				Name:      d.getTypeName(dataTypeID),
				TypeOid:   dataTypeID,
				ArrayType: false, // TODO: Detect array types
			},
			Order:    i,
			Optional: true, // TODO: Get from pg_attribute
			IsKey:    (flags&1 != 0),
		}
		columns = append(columns, column)
	}

	d.relations[relationID] = relationInfo{
		schema:  schemaName,
		table:   tableName,
		columns: columns,
	}

	return &model.Message{
		ID:        uuid.New().String(),
		Operation: "RELATION",
		Schema:    schemaName,
		Table:     tableName,
		Object: model.TableSchema{
			Columns: columns,
		},
		LSN:       0,
		Timestamp: time.Now(),
		EventType: "DDL",
	}, nil
}

func (d *PgOutputDecoder) parseBegin(data []byte) (*model.Message, error) {
	if len(data) < 21 {
		return nil, fmt.Errorf("invalid begin message length: %d", len(data))
	}

	xid := binary.BigEndian.Uint32(data[17:21])

	return &model.Message{
		ID:            uuid.New().String(),
		Operation:     "BEGIN",
		TransactionID: fmt.Sprintf("%d", xid),
		LSN:           uint64(binary.BigEndian.Uint64(data[5:13])),
		Timestamp:     time.Now(),
		EventType:     "TRANSACTION",
	}, nil
}

func (d *PgOutputDecoder) parseCommit(data []byte) (*model.Message, error) {
	return &model.Message{
		ID:        uuid.New().String(),
		Operation: "COMMIT",
		LSN:       uint64(binary.BigEndian.Uint64(data[5:13])),
		Timestamp: time.Now(),
		EventType: "TRANSACTION",
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
		ID:        uuid.New().String(),
		Operation: "INSERT",
		Schema:    relation.schema,
		Table:     relation.table,
		Object: model.TableSchema{
			Columns: relation.columns,
		},
		After:     tupleData,
		LSN:       uint64(binary.BigEndian.Uint64(data[13:21])),
		Timestamp: time.Now(),
		EventType: "DML",
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
		ID:        uuid.New().String(),
		Operation: "UPDATE",
		Schema:    relation.schema,
		Table:     relation.table,
		Object: model.TableSchema{
			Columns: relation.columns,
		},
		Before:    oldTuple,
		After:     newTuple,
		LSN:       uint64(binary.BigEndian.Uint64(data[13:21])),
		Timestamp: time.Now(),
		EventType: "DML",
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
		ID:        uuid.New().String(),
		Operation: "DELETE",
		Schema:    relation.schema,
		Table:     relation.table,
		Object: model.TableSchema{
			Columns: relation.columns,
		},
		Before:    oldTuple,
		LSN:       uint64(binary.BigEndian.Uint64(data[13:21])),
		Timestamp: time.Now(),
		EventType: "DML",
	}, nil
}

func (d *PgOutputDecoder) parseTupleData(data []byte, relationId uint32) (map[string]interface{}, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("tuple data too short")
	}

	// First byte is tuple type ('N' for new tuple, 'K' for key tuple, etc)
	// Skip it if present
	pos := 0
	if data[0] == 'N' || data[0] == 'K' || data[0] == 'O' {
		pos = 1
	}

	// Next 2 bytes are number of columns
	if pos+2 > len(data) {
		return nil, fmt.Errorf("tuple data truncated at column count")
	}
	numColumns := int(binary.BigEndian.Uint16(data[pos:]))
	pos += 2

	relation, ok := d.relations[relationId]
	if !ok {
		return nil, fmt.Errorf("relation %d not found", relationId)
	}

	// For debugging
	fmt.Printf("Tuple data: %v\n", data[:min(len(data), 32)])
	fmt.Printf("Relation ID: %d, Expected columns: %d, Got columns: %d\n", relationId, len(relation.columns), numColumns)

	// Check if column count matches
	if numColumns != len(relation.columns) {
		return nil, fmt.Errorf("column count mismatch: got %d, expected %d (first 32 bytes: %v)",
			numColumns, len(relation.columns), data[:min(len(data), 32)])
	}

	values := make(map[string]interface{})

	for i := 0; i < numColumns; i++ {
		if pos >= len(data) {
			return nil, fmt.Errorf("unexpected end of tuple data at column %d", i)
		}

		colType := data[pos]
		pos++

		var colData []byte
		var err error

		switch colType {
		case 'n': // null
			values[relation.columns[i].Name] = nil
			continue
		case 'u': // unchanged toast
			// Skip unchanged toast value
			continue
		case 't': // text
			if pos+4 > len(data) {
				return nil, fmt.Errorf("insufficient data for text length at column %d", i)
			}
			length := int(binary.BigEndian.Uint32(data[pos:]))
			pos += 4
			if pos+length > len(data) {
				return nil, fmt.Errorf("insufficient data for text value at column %d", i)
			}
			colData = data[pos : pos+length]
			pos += length
		case 'b': // binary
			if pos+4 > len(data) {
				return nil, fmt.Errorf("insufficient data for binary length at column %d", i)
			}
			length := int(binary.BigEndian.Uint32(data[pos:]))
			pos += 4
			if pos+length > len(data) {
				return nil, fmt.Errorf("insufficient data for binary value at column %d", i)
			}
			colData = data[pos : pos+length]
			pos += length
		default:
			return nil, fmt.Errorf("invalid tuple data column type at column %d: %q (hex: %x)", i, colType, colType)
		}

		val, err := d.parseValue(colData, relation.columns[i].Type.TypeOid)
		if err != nil {
			return nil, fmt.Errorf("failed to parse column %s value: %w", relation.columns[i].Name, err)
		}

		values[relation.columns[i].Name] = val
	}

	return values, nil
}

func (d *PgOutputDecoder) getTupleDataLength(data []byte) int {
	if len(data) < 2 {
		return 0
	}

	numColumns := int(binary.BigEndian.Uint16(data))
	pos := 2

	for i := 0; i < numColumns; i++ {
		if pos >= len(data) {
			return pos
		}

		colType := data[pos]
		pos++

		if colType == 't' {
			if pos+4 > len(data) {
				return pos
			}
			length := int(binary.BigEndian.Uint32(data[pos:]))
			pos += 4 + length
		} else if colType == 'b' {
			if pos+4 > len(data) {
				return pos
			}
			length := int(binary.BigEndian.Uint32(data[pos:]))
			pos += 4 + length
		}
	}

	return pos
}

func (d *PgOutputDecoder) parseValue(data []byte, typeOid uint32) (interface{}, error) {
	if len(data) == 0 {
		return nil, nil
	}

	// Try parsing as text first
	strVal := string(data)

	switch typeOid {
	case OIDBoolean:
		if len(data) == 1 {
			return data[0] == 't', nil
		}
		switch strings.ToLower(strVal) {
		case "t", "true", "1":
			return true, nil
		case "f", "false", "0":
			return false, nil
		default:
			return nil, fmt.Errorf("invalid boolean value: %s", strVal)
		}

	case OIDInt4:
		// Try text format first
		var i int32
		if _, err := fmt.Sscanf(strVal, "%d", &i); err == nil {
			return i, nil
		}
		// Try binary format
		if len(data) == 4 {
			return int32(binary.BigEndian.Uint32(data)), nil
		}
		return nil, fmt.Errorf("invalid int4 value: %s", strVal)

	case OIDVarchar:
		return strVal, nil

	case OIDTimestamp:
		// Try text format first - PostgreSQL timestamp formats
		formats := []string{
			"2006-01-02 15:04:05.999999999", // With microseconds
			"2006-01-02 15:04:05.999999",    // With microseconds (6 digits)
			"2006-01-02 15:04:05.999",       // With milliseconds
			"2006-01-02 15:04:05",           // Without fraction
			time.RFC3339,                    // ISO format
			time.RFC3339Nano,                // ISO format with nanoseconds
		}

		for _, format := range formats {
			if t, err := time.Parse(format, strVal); err == nil {
				return t, nil
			}
		}

		// Try binary format
		if len(data) == 8 {
			microsecSinceY2K := int64(binary.BigEndian.Uint64(data))
			return time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).
				Add(time.Duration(microsecSinceY2K) * time.Microsecond), nil
		}

		return nil, fmt.Errorf("invalid timestamp format: %s (supported formats: YYYY-MM-DD HH:MM:SS[.NNNNNN], RFC3339)", strVal)

	case OIDNumeric:
		// Try text format first
		if _, err := fmt.Sscanf(strVal, "%f", new(float64)); err == nil {
			return strVal, nil
		}
		// Try binary format
		if len(data) < 8 {
			return nil, fmt.Errorf("invalid numeric length: %d", len(data))
		}

		numDigits := binary.BigEndian.Uint16(data[0:2])
		weight := int16(binary.BigEndian.Uint16(data[2:4]))
		sign := binary.BigEndian.Uint16(data[4:6])
		dscale := binary.BigEndian.Uint16(data[6:8])
		digits := make([]int16, numDigits)

		for i := 0; i < int(numDigits); i++ {
			start := 8 + (i * 2)
			if start+2 > len(data) {
				return nil, fmt.Errorf("invalid numeric data length for digits")
			}
			digits[i] = int16(binary.BigEndian.Uint16(data[start : start+2]))
		}

		// Convert to string representation
		var result strings.Builder
		if sign == 0x4000 {
			result.WriteString("-")
		}

		if numDigits == 0 {
			return "0", nil
		}

		// Calculate the position of decimal point
		decimalPoint := int((weight + 1) * 4)

		// Build the number string
		digitsAdded := 0
		for i, d := range digits {
			currentPos := i * 4
			if currentPos == decimalPoint {
				result.WriteString(".")
			}
			result.WriteString(fmt.Sprintf("%04d", d))
			digitsAdded += 4
		}

		// Add trailing zeros if needed
		for digitsAdded < decimalPoint {
			result.WriteString("0000")
			digitsAdded += 4
		}

		// Add decimal point and trailing zeros if dscale > 0
		if dscale > 0 && digitsAdded <= decimalPoint {
			result.WriteString(".")
		}

		// Trim leading zeros (except the last one before decimal)
		numStr := result.String()
		numStr = strings.TrimLeft(numStr, "0")
		if numStr == "" || numStr[0] == '.' {
			numStr = "0" + numStr
		}

		// Trim trailing zeros after decimal
		if strings.Contains(numStr, ".") {
			numStr = strings.TrimRight(numStr, "0")
			numStr = strings.TrimRight(numStr, ".")
		}

		return numStr, nil

	default:
		// For unknown types, return as string
		return strVal, nil
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
	case OIDInt2:
		return "smallint"
	case OIDVarchar:
		return "varchar"
	case OIDBpchar:
		return "bpchar"
	case OIDByteArray:
		return "bytea"
	case OIDNumeric:
		return "numeric"
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
		ID:        uuid.New().String(),
		Operation: "TRUNCATE",
		Schema:    relation.schema,
		Table:     relation.table,
		Object: model.TableSchema{
			Columns: relation.columns,
		},
		LSN:       uint64(binary.BigEndian.Uint64(data[13:21])),
		Timestamp: time.Now(),
		EventType: "DDL",
	}, nil
}

func NewPgOutputDecoder() *PgOutputDecoder {
	return &PgOutputDecoder{
		relations: make(map[uint32]relationInfo),
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
