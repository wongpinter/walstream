package model

import (
	"time"

	"github.com/jackc/pglogrepl"
)

// TransactionState represents the state of a transaction
type TransactionState string

const (
	// TransactionBegin indicates the start of a transaction
	TransactionBegin TransactionState = "BEGIN"
	// TransactionCommit indicates a successful transaction commit
	TransactionCommit TransactionState = "COMMIT"
	// TransactionRollback indicates a transaction rollback
	TransactionRollback TransactionState = "ROLLBACK"
)

// Transaction represents a PostgreSQL transaction
type Transaction struct {
	// XID is the transaction ID
	XID uint32 `json:"xid"`
	// LSN is the Log Sequence Number where this transaction started
	LSN pglogrepl.LSN `json:"lsn"`
	// Timestamp is when this transaction was processed
	Timestamp time.Time `json:"timestamp"`
	// State indicates the transaction state (BEGIN/COMMIT/ROLLBACK)
	State TransactionState `json:"state"`
	// Changes contains all data changes within this transaction
	Changes []*Message `json:"changes,omitempty"`
}

// TransactionMessage wraps a Message with transaction information
type TransactionMessage struct {
	// Transaction contains the transaction metadata
	Transaction *Transaction `json:"transaction"`
	// Message contains the actual change data
	Message *Message `json:"message,omitempty"`
}
