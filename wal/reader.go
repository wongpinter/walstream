// Package wal provides functionality for reading and processing PostgreSQL WAL (Write-Ahead Log) entries
package wal

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/rs/zerolog"

	"repo.nusatek.id/sugeng/walstreamer/model"
)

// Reader is responsible for reading and processing WAL entries
type Reader struct {
	conn            *pgconn.PgConn
	logger          zerolog.Logger
	relations       map[uint32]*pglogrepl.RelationMessageV2
	clientXLogPos   pglogrepl.LSN
	typeMap         *pgtype.Map
	standbyTimeout  time.Duration
	publicationName string
	slotName        string
	config          Config
	messageHandler  func(*model.Message) error
}

// Config holds the configuration for the WAL reader
type Config struct {
	ConnString      string
	PublicationName string
	SlotName        string
	StandbyTimeout  time.Duration
	Logger          zerolog.Logger
}

// NewReader creates a new WAL reader
func NewReader(config Config, handler func(*model.Message) error) *Reader {
	if config.StandbyTimeout == 0 {
		config.StandbyTimeout = 10 * time.Second
	}

	return &Reader{
		relations:       make(map[uint32]*pglogrepl.RelationMessageV2),
		typeMap:         pgtype.NewMap(),
		standbyTimeout:  config.StandbyTimeout,
		publicationName: config.PublicationName,
		slotName:        config.SlotName,
		logger:          config.Logger,
		config:          config,
		messageHandler:  handler,
	}
}

// Start begins reading WAL entries
func (r *Reader) Start(ctx context.Context) error {
	var err error
	r.conn, err = pgconn.Connect(ctx, r.config.ConnString)
	if err != nil {
		return fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}
	defer r.conn.Close(ctx)

	// Identify system
	sysident, err := pglogrepl.IdentifySystem(ctx, r.conn)
	if err != nil {
		return fmt.Errorf("failed to identify system: %w", err)
	}
	r.clientXLogPos = sysident.XLogPos

	// Start replication
	err = pglogrepl.StartReplication(ctx, r.conn, r.slotName, sysident.XLogPos, pglogrepl.StartReplicationOptions{
		PluginArgs: []string{
			"proto_version '2'",
			fmt.Sprintf("publication_names '%s'", r.publicationName),
			"messages 'true'",
			"streaming 'true'",
		},
	})
	if err != nil {
		return fmt.Errorf("failed to start replication: %w", err)
	}

	r.logger.Info().
		Str("slot", r.slotName).
		Str("publication", r.publicationName).
		Uint64("xlogpos", uint64(r.clientXLogPos)).
		Msg("started WAL streaming")

	return r.processWALMessages(ctx)
}

func (r *Reader) processWALMessages(ctx context.Context) error {
	nextStandbyMessageDeadline := time.Now().Add(r.standbyTimeout)
	inStream := false

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		if time.Now().After(nextStandbyMessageDeadline) {
			err := pglogrepl.SendStandbyStatusUpdate(ctx, r.conn, pglogrepl.StandbyStatusUpdate{
				WALWritePosition: r.clientXLogPos,
			})
			if err != nil {
				return fmt.Errorf("failed to send standby status update: %w", err)
			}
			nextStandbyMessageDeadline = time.Now().Add(r.standbyTimeout)
		}

		messageCtx, cancel := context.WithDeadline(ctx, nextStandbyMessageDeadline)
		rawMsg, err := r.conn.ReceiveMessage(messageCtx)
		cancel()

		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			return fmt.Errorf("failed to receive message: %w", err)
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			return fmt.Errorf("received Postgres WAL error: %+v", errMsg)
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			r.logger.Debug().Msgf("received unexpected message: %T", rawMsg)
			continue
		}

		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			if err := r.handleKeepaliveMessage(msg.Data[1:]); err != nil {
				return err
			}

		case pglogrepl.XLogDataByteID:
			if err := r.handleXLogData(ctx, msg.Data[1:], &inStream); err != nil {
				return err
			}
		}
	}
}

func (r *Reader) handleKeepaliveMessage(data []byte) error {
	pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(data)
	if err != nil {
		return fmt.Errorf("failed to parse keepalive message: %w", err)
	}

	r.logger.Debug().
		Str("server_wal_end", pkm.ServerWALEnd.String()).
		Time("server_time", pkm.ServerTime).
		Bool("reply_requested", pkm.ReplyRequested).
		Msg("received keepalive message")

	if pkm.ServerWALEnd > r.clientXLogPos {
		r.clientXLogPos = pkm.ServerWALEnd
	}

	return nil
}

func (r *Reader) handleXLogData(_ context.Context, data []byte, inStream *bool) error {
	xld, err := pglogrepl.ParseXLogData(data)
	if err != nil {
		return fmt.Errorf("failed to parse XLog data: %w", err)
	}

	r.logger.Debug().
		Str("wal_start", xld.WALStart.String()).
		Str("server_wal_end", xld.ServerWALEnd.String()).
		Time("server_time", xld.ServerTime).
		Msg("received XLog data")

	if err := r.processMessage(xld.WALData, inStream); err != nil {
		return fmt.Errorf("failed to process message: %w", err)
	}

	if xld.WALStart > r.clientXLogPos {
		r.clientXLogPos = xld.WALStart
	}

	return nil
}

func (r *Reader) processMessage(walData []byte, inStream *bool) error {
	logicalMsg, err := pglogrepl.ParseV2(walData, *inStream)
	if err != nil {
		return fmt.Errorf("failed to parse logical replication message: %w", err)
	}

	switch msg := logicalMsg.(type) {
	case *pglogrepl.RelationMessageV2:
		r.relations[msg.RelationID] = msg
		return r.handleRelationMessage(msg)

	case *pglogrepl.BeginMessage:
		return r.handleBeginMessage(msg)

	case *pglogrepl.CommitMessage:
		return r.handleCommitMessage(msg)

	case *pglogrepl.InsertMessageV2:
		return r.handleInsertMessage(msg)

	case *pglogrepl.UpdateMessageV2:
		return r.handleUpdateMessage(msg)

	case *pglogrepl.DeleteMessageV2:
		return r.handleDeleteMessage(msg)

	case *pglogrepl.TruncateMessageV2:
		return r.handleTruncateMessage(msg)

	case *pglogrepl.StreamStartMessageV2:
		*inStream = true
		return nil

	case *pglogrepl.StreamStopMessageV2:
		*inStream = false
		return nil

	default:
		r.logger.Debug().Msgf("unhandled message type: %T", msg)
		return nil
	}
}

func (r *Reader) handleRelationMessage(msg *pglogrepl.RelationMessageV2) error {
	columns := make([]model.ColumnDefinition, len(msg.Columns))
	for i, col := range msg.Columns {
		pgtype, ok := r.typeMap.TypeForOID(col.DataType)
		if !ok {
			return fmt.Errorf("unknown type OID for column %s: %d", col.Name, col.DataType)
		}
		columns[i] = model.ColumnDefinition{
			Name:    col.Name,
			Type:    pgtype.Name,
			TypeOid: col.DataType,
			Order:   i,
		}
	}

	return r.messageHandler(&model.Message{
		Operation: "RELATION",
		Schema:    msg.Namespace,
		Table:     msg.RelationName,
		Columns:   columns,
		LSN:       uint64(r.clientXLogPos),
		Timestamp: time.Now().UnixNano(),
	})
}

func (r *Reader) handleBeginMessage(msg *pglogrepl.BeginMessage) error {
	return r.messageHandler(&model.Message{
		Operation:     "BEGIN",
		TransactionID: fmt.Sprintf("%d", msg.Xid),
		LSN:           uint64(msg.FinalLSN),
		Timestamp:     time.Now().UnixNano(),
	})
}

func (r *Reader) handleCommitMessage(msg *pglogrepl.CommitMessage) error {
	return r.messageHandler(&model.Message{
		Operation: "COMMIT",
		LSN:       uint64(msg.CommitLSN),
		Timestamp: time.Now().UnixNano(),
	})
}

func (r *Reader) handleInsertMessage(msg *pglogrepl.InsertMessageV2) error {
	rel, ok := r.relations[msg.RelationID]
	if !ok {
		return fmt.Errorf("unknown relation ID %d", msg.RelationID)
	}

	values, err := r.decodeTupleData(msg.Tuple, rel)
	if err != nil {
		return err
	}

	return r.messageHandler(&model.Message{
		Operation: "INSERT",
		Schema:    rel.Namespace,
		Table:     rel.RelationName,
		After:     values,
		LSN:       uint64(r.clientXLogPos),
		Timestamp: time.Now().UnixNano(),
	})
}

func (r *Reader) handleUpdateMessage(msg *pglogrepl.UpdateMessageV2) error {
	rel, ok := r.relations[msg.RelationID]
	if !ok {
		return fmt.Errorf("unknown relation ID %d", msg.RelationID)
	}

	oldValues, err := r.decodeTupleData(msg.OldTuple, rel)
	if err != nil {
		return err
	}

	newValues, err := r.decodeTupleData(msg.NewTuple, rel)
	if err != nil {
		return err
	}

	return r.messageHandler(&model.Message{
		Operation: "UPDATE",
		Schema:    rel.Namespace,
		Table:     rel.RelationName,
		Before:    oldValues,
		After:     newValues,
		LSN:       uint64(r.clientXLogPos),
		Timestamp: time.Now().UnixNano(),
	})
}

func (r *Reader) handleDeleteMessage(msg *pglogrepl.DeleteMessageV2) error {
	rel, ok := r.relations[msg.RelationID]
	if !ok {
		return fmt.Errorf("unknown relation ID %d", msg.RelationID)
	}

	oldValues, err := r.decodeTupleData(msg.OldTuple, rel)
	if err != nil {
		return err
	}

	return r.messageHandler(&model.Message{
		Operation: "DELETE",
		Schema:    rel.Namespace,
		Table:     rel.RelationName,
		Before:    oldValues,
		LSN:       uint64(r.clientXLogPos),
		Timestamp: time.Now().UnixNano(),
	})
}

func (r *Reader) handleTruncateMessage(msg *pglogrepl.TruncateMessageV2) error {
	for _, relID := range msg.RelationIDs {
		rel, ok := r.relations[relID]
		if !ok {
			return fmt.Errorf("unknown relation ID %d", relID)
		}

		if err := r.messageHandler(&model.Message{
			Operation: "TRUNCATE",
			Schema:    rel.Namespace,
			Table:     rel.RelationName,
			LSN:       uint64(r.clientXLogPos),
			Timestamp: time.Now().UnixNano(),
		}); err != nil {
			return err
		}
	}

	return nil
}

func (r *Reader) decodeTupleData(tuple *pglogrepl.TupleData, rel *pglogrepl.RelationMessageV2) (map[string]interface{}, error) {
	values := make(map[string]interface{})
	for i, col := range tuple.Columns {
		colName := rel.Columns[i].Name
		switch col.DataType {
		case 'n': // null
			values[colName] = nil
		case 'u': // unchanged toast
			// Skip unchanged TOAST values
			continue
		case 't': // text
			pgtype, ok := r.typeMap.TypeForOID(rel.Columns[i].DataType)
			if !ok {
				return nil, fmt.Errorf("unknown type OID for column %s: %d", colName, rel.Columns[i].DataType)
			}
			val, err := pgtype.Codec.DecodeValue(r.typeMap, rel.Columns[i].DataType, 0, col.Data)
			if err != nil {
				return nil, fmt.Errorf("failed to decode column %s: %w", colName, err)
			}
			values[colName] = val
		}
	}

	return values, nil
}
