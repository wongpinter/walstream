// Package wal provides functionality for reading and processing PostgreSQL WAL (Write-Ahead Log) entries
package wal

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/rs/zerolog"

	"repo.nusatek.id/sugeng/walstreamer/decoder/pgoutput"
	"repo.nusatek.id/sugeng/walstreamer/lsn"
	"repo.nusatek.id/sugeng/walstreamer/model"
	"repo.nusatek.id/sugeng/walstreamer/schema"
)

// Reader is responsible for reading and processing WAL entries
type Reader struct {
	conn            *pgconn.PgConn
	logger          *zerolog.Logger
	clientXLogPos   pglogrepl.LSN
	standbyTimeout  time.Duration
	publicationName string
	slotName        string
	config          Config
	messageHandler  func(*model.Message) error
	lsnStorage      lsn.Storage
	schemaFetcher   *schema.Fetcher
	decoder         *pgoutput.PgOutputDecoder
	// Track current transaction
	currentTx *model.Transaction
}

// Config holds the configuration for the WAL reader
type Config struct {
	ConnString      string
	PublicationName string
	SlotName        string
	StandbyTimeout  time.Duration
	Logger          *zerolog.Logger
	LSNStorage      lsn.Storage
	// Maximum number of reconnection attempts, 0 means unlimited
	MaxReconnectAttempts int
	// Initial delay between reconnection attempts
	ReconnectInitialDelay time.Duration
	// Maximum delay between reconnection attempts
	ReconnectMaxDelay time.Duration
}

// connect establishes a connection to PostgreSQL with retries
func (r *Reader) connect(ctx context.Context) error {
	var err error
	delay := r.config.ReconnectInitialDelay
	if delay == 0 {
		delay = 1 * time.Second
	}
	maxDelay := r.config.ReconnectMaxDelay
	if maxDelay == 0 {
		maxDelay = 30 * time.Second
	}
	attempts := 0

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			r.logger.Info().
				Int("attempt", attempts+1).
				Str("delay", delay.String()).
				Msg("connecting to PostgreSQL")

			r.conn, err = pgconn.Connect(ctx, r.config.ConnString)
			if err == nil {
				r.logger.Info().Msg("successfully connected to PostgreSQL")
				return nil
			}

			r.logger.Error().Err(err).Msg("failed to connect to PostgreSQL")

			if r.config.MaxReconnectAttempts > 0 && attempts >= r.config.MaxReconnectAttempts {
				return fmt.Errorf("max reconnection attempts (%d) reached: %w", r.config.MaxReconnectAttempts, err)
			}

			// Exponential backoff with jitter
			jitter := time.Duration(float64(delay) * (0.5 + rand.Float64()))
			time.Sleep(jitter)

			// Double the delay for next attempt, but cap it
			delay *= 2
			if delay > maxDelay {
				delay = maxDelay
			}

			attempts++
		}
	}
}

// reconnect attempts to reconnect to PostgreSQL and resume replication
func (r *Reader) reconnect(ctx context.Context) error {
	r.logger.Info().Msg("attempting to reconnect and resume replication")

	// Close existing connection if any
	if r.conn != nil {
		r.conn.Close(ctx)
	}

	// Try to connect
	if err := r.connect(ctx); err != nil {
		return fmt.Errorf("failed to reconnect: %w", err)
	}

	// Re-identify system
	sysident, err := pglogrepl.IdentifySystem(ctx, r.conn)
	if err != nil {
		return fmt.Errorf("failed to identify system after reconnect: %w", err)
	}

	// Resume from last processed LSN
	if r.clientXLogPos > 0 {
		r.logger.Info().Str("lsn", r.clientXLogPos.String()).Msg("resuming from last processed LSN")
	} else {
		r.clientXLogPos = sysident.XLogPos
		r.logger.Info().Str("lsn", r.clientXLogPos.String()).Msg("starting from current LSN")
	}

	// Restart replication
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
		return fmt.Errorf("failed to restart replication: %w", err)
	}

	r.logger.Info().
		Str("slot", r.slotName).
		Str("publication", r.publicationName).
		Str("xlogpos", r.clientXLogPos.String()).
		Msg("successfully reconnected and resumed WAL streaming")

	return nil
}

// NewReader creates a new WAL reader
func NewReader(config Config, handler func(*model.Message) error) (*Reader, error) {
	if handler == nil {
		return nil, fmt.Errorf("message handler is required")
	}

	reader := &Reader{
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

	// Initial connection
	if err := r.connect(ctx); err != nil {
		return err
	}

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

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if time.Now().After(nextStandbyMessageDeadline) {
				err := pglogrepl.SendStandbyStatusUpdate(ctx, r.conn, pglogrepl.StandbyStatusUpdate{
					WALWritePosition: r.clientXLogPos,
					WALFlushPosition: r.clientXLogPos,
					WALApplyPosition: r.clientXLogPos,
					ReplyRequested:   true,
				})
				if err != nil {
					r.logger.Error().Err(err).Msg("failed to send standby status update")
					if err := r.reconnect(ctx); err != nil {
						return fmt.Errorf("failed to recover after connection error: %w", err)
					}
					nextStandbyMessageDeadline = time.Now().Add(r.standbyTimeout)
					continue
				}
				nextStandbyMessageDeadline = time.Now().Add(r.standbyTimeout)
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
				r.logger.Error().Err(err).Msg("failed to receive message")
				if err := r.reconnect(ctx); err != nil {
					return fmt.Errorf("failed to recover after connection error: %w", err)
				}
				continue
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
				if err := r.handleXLogData(msg.Data[1:]); err != nil {
					return err
				}
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

func (r *Reader) handleXLogData(data []byte) error {
	walData, err := pglogrepl.ParseXLogData(data)
	if err != nil {
		return fmt.Errorf("failed to parse WAL data: %w", err)
	}

	// Update client position
	r.clientXLogPos = walData.WALStart + pglogrepl.LSN(len(walData.WALData))

	// Decode the message
	msg, err := r.decoder.Decode(walData.WALData)
	if err != nil {
		return fmt.Errorf("failed to decode WAL message: %w", err)
	}

	if msg == nil {
		return nil // Skip empty messages
	}

	// Handle transaction boundaries
	switch msg.Operation {
	case "BEGIN":
		txid, err := strconv.ParseUint(msg.TransactionID, 10, 32)
		if err != nil {
			return fmt.Errorf("failed to parse transaction ID: %w", err)
		}

		r.currentTx = &model.Transaction{
			XID:       uint32(txid),
			LSN:       walData.WALStart,
			Timestamp: msg.Timestamp,
			State:     model.TransactionBegin,
			Changes:   make([]*model.Message, 0),
		}
		// Send transaction begin marker
		if err := r.messageHandler(msg); err != nil {
			return fmt.Errorf("failed to handle BEGIN message: %w", err)
		}
		return nil

	case "COMMIT":
		if r.currentTx == nil {
			r.logger.Warn().Msg("received COMMIT without BEGIN")
			return nil
		}
		r.currentTx.State = model.TransactionCommit
		// Send all changes in order
		for _, change := range r.currentTx.Changes {
			if err := r.messageHandler(change); err != nil {
				return fmt.Errorf("failed to handle change message: %w", err)
			}
		}
		// Send transaction commit marker
		if err := r.messageHandler(msg); err != nil {
			return fmt.Errorf("failed to handle COMMIT message: %w", err)
		}
		r.currentTx = nil
		return nil

	case "RELATION":
		// Skip relation messages, they are handled by the decoder
		return nil
	}

	// Handle data changes (INSERT/UPDATE/DELETE)
	if r.currentTx == nil {
		r.logger.Warn().Msg("received change without transaction")
		return nil
	}

	// Add to current transaction
	r.currentTx.Changes = append(r.currentTx.Changes, msg)

	return nil
}
