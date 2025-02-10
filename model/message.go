package model

type ColumnDefinition struct {
	Name     string
	Type     string
	TypeOid  uint32
	Order    int
	Optional bool
	IsKey    bool
}

type Message struct {
	TransactionID string
	Operation     string // INSERT, UPDATE, DELETE, CREATE, ALTER, TRUNCATE
	Schema        string
	Table         string
	Before        map[string]interface{} // Old values (for UPDATE/DELETE)
	After         map[string]interface{} // New values (for INSERT/UPDATE)
	Columns       []ColumnDefinition     // Schema definition
	LSN           uint64                 // Log Sequence Number
	Timestamp     int64                  // Unix timestamp of the change
	Raw           []byte                 // Raw WAL message for debugging
}
