// Package wal provides functionality for reading and processing PostgreSQL WAL (Write-Ahead Log) entries
package wal

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/rs/zerolog"

	"repo.nusatek.id/sugeng/walstreamer/decoder/pgoutput"
	"repo.nusatek.id/sugeng/walstreamer/lsn"
	"repo.nusatek.id/sugeng/walstreamer/model"
	"repo.nusatek.id/sugeng/walstreamer/schema"
)

// Reader is responsible for reading and processing WAL entries
type Reader struct {
	conn            *pgconn.PgConn
	logger          zerolog.Logger
	clientXLogPos   pglogrepl.LSN
	standbyTimeout  time.Duration
	publicationName string
	slotName        string
	config          Config
	messageHandler  func(*model.Message) error
	lsnStorage      lsn.Storage
	schemaFetcher   *schema.Fetcher
	decoder         *pgoutput.PgOutputDecoder
}

// Config holds the configuration for the WAL reader
type Config struct {
	ConnString      string
	PublicationName string
	SlotName        string
	StandbyTimeout  time.Duration
	Logger          zerolog.Logger
	LSNStorage      lsn.Storage
}

// NewReader creates a new WAL reader
func NewReader(config Config, handler func(*model.Message) error) (*Reader, error) {
	if handler == nil {
		return nil, fmt.Errorf("message handler is required")
	}

	ctx := context.Background()
	conn, err := pgconn.Connect(ctx, config.ConnString)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}

	reader := &Reader{
		conn:            conn,
		logger:          config.Logger,
		standbyTimeout:  config.StandbyTimeout,
		publicationName: config.PublicationName,
		slotName:        config.SlotName,
		config:          config,
		messageHandler:  handler,
		lsnStorage:      config.LSNStorage,
		decoder:         pgoutput.NewPgOutputDecoder(),
	}

	// Initialize schema fetcher
	reader.schemaFetcher = schema.NewFetcher(config.ConnString, config.Logger)

	return reader, nil
}

// Start begins reading WAL entries
func (r *Reader) Start(ctx context.Context) error {
	r.logger.Info().
		Str("slot", r.slotName).
		Str("publication", r.publicationName).
		Msg("Starting WAL reader")

	sysident, err := pglogrepl.IdentifySystem(ctx, r.conn)
	if err != nil {
		return fmt.Errorf("failed to identify system: %w", err)
	}

	r.clientXLogPos = sysident.XLogPos

	// Create replication slot if it doesn't exist
	_, err = pglogrepl.CreateReplicationSlot(ctx, r.conn, r.slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Temporary:      false,
		SnapshotAction: "NOEXPORT_SNAPSHOT",
		Mode:           pglogrepl.LogicalReplication,
	})
	if err != nil {
		if !strings.Contains(err.Error(), "already exists") {
			return fmt.Errorf("failed to create replication slot: %w", err)
		}
		r.logger.Info().Str("slot", r.slotName).Msg("replication slot already exists")
	}

	// Start replication
	err = pglogrepl.StartReplication(ctx, r.conn, r.slotName, r.clientXLogPos, pglogrepl.StartReplicationOptions{
		PluginArgs: []string{
			"proto_version '2'",
			fmt.Sprintf("publication_names '%s'", r.publicationName),
			"messages 'true'",
			"streaming 'true'",
		},
		Mode: pglogrepl.LogicalReplication,
	})
	if err != nil {
		return fmt.Errorf("failed to start replication: %w", err)
	}

	r.logger.Info().
		Str("slot", r.slotName).
		Str("publication", r.publicationName).
		Str("xlogpos", r.clientXLogPos.String()).
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

		now := time.Now()
		if now.After(nextStandbyMessageDeadline) {
			r.logger.Debug().
				Str("wal_position", r.clientXLogPos.String()).
				Msg("sending standby status update")

			err := pglogrepl.SendStandbyStatusUpdate(ctx, r.conn, pglogrepl.StandbyStatusUpdate{
				WALWritePosition: r.clientXLogPos,
				WALFlushPosition: r.clientXLogPos,
				WALApplyPosition: r.clientXLogPos,
				ReplyRequested:   true,
			})
			if err != nil {
				return fmt.Errorf("failed to send standby status update: %w", err)
			}
			nextStandbyMessageDeadline = now.Add(r.standbyTimeout)
		}

		// Set a deadline for the next message receive operation
		deadline := time.Now().Add(1 * time.Second)
		if err := r.conn.Conn().SetDeadline(deadline); err != nil {
			return fmt.Errorf("failed to set connection deadline: %w", err)
		}

		rawMsg, err := r.conn.ReceiveMessage(ctx)
		if err != nil {
			if pgconn.Timeout(err) {
				// Reset deadline after timeout
				if err := r.conn.Conn().SetDeadline(time.Time{}); err != nil {
					return fmt.Errorf("failed to reset connection deadline: %w", err)
				}
				continue
			}
			return fmt.Errorf("failed to receive message: %w", err)
		}

		// Reset deadline after successful receive
		if err := r.conn.Conn().SetDeadline(time.Time{}); err != nil {
			return fmt.Errorf("failed to reset connection deadline: %w", err)
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			r.logger.Error().
				Str("severity", errMsg.Severity).
				Str("code", errMsg.Code).
				Str("message", errMsg.Message).
				Str("detail", errMsg.Detail).
				Msg("received Postgres error")
			return fmt.Errorf("received Postgres WAL error: %+v", errMsg)
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			r.logger.Debug().
				Str("type", fmt.Sprintf("%T", rawMsg)).
				Interface("msg", rawMsg).
				Msg("received unexpected message type")
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

func (r *Reader) handleXLogData(ctx context.Context, data []byte, inStream *bool) error {
	xld, err := pglogrepl.ParseXLogData(data)
	if err != nil {
		return fmt.Errorf("failed to parse XLogData: %w", err)
	}

	r.clientXLogPos = xld.WALStart + pglogrepl.LSN(len(xld.WALData))

	msg, err := r.decoder.Decode(xld.WALData)
	if err != nil {
		return fmt.Errorf("failed to decode message: %w", err)
	}

	if msg == nil {
		return nil // Skip non-data messages
	}

	// Update LSN in message
	msg.LSN = uint64(r.clientXLogPos)

	// Handle the message
	if err := r.messageHandler(msg); err != nil {
		return fmt.Errorf("failed to handle message: %w", err)
	}

	return nil
}

func (r *Reader) getTypeName(oid uint32) string {
	switch oid {
	case pgtype.TextOID:
		return "text"
	case pgtype.VarcharOID:
		return "varchar"
	case pgtype.Int8OID:
		return "bigint"
	case pgtype.Int4OID:
		return "integer"
	case pgtype.Float8OID:
		return "double precision"
	case pgtype.TimestampOID:
		return "timestamp"
	case pgtype.TimestamptzOID:
		return "timestamptz"
	case pgtype.DateOID:
		return "date"
	case pgtype.BoolOID:
		return "boolean"
	case pgtype.JSONBOID:
		return "jsonb"
	case pgtype.JSONOID:
		return "json"
	default:
		return fmt.Sprintf("unknown_%d", oid)
	}
}
