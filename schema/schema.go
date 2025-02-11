package schema

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/rs/zerolog"

	"repo.nusatek.id/sugeng/walstreamer/model"
)

// Fetcher is responsible for fetching and caching table schemas
type Fetcher struct {
	conn   *pgx.Conn
	logger *zerolog.Logger
	cache  map[string]*model.TableSchema // key: "schema.table"
}

// NewFetcher creates a new schema fetcher
func NewFetcher(connString string, logger *zerolog.Logger) *Fetcher {
	conn, err := pgx.Connect(context.Background(), connString)
	if err != nil {
		logger.Error().Err(err).Msg("failed to connect to database")
		return nil
	}

	return &Fetcher{
		conn:   conn,
		logger: logger,
		cache:  make(map[string]*model.TableSchema),
	}
}

// GetTableSchema fetches the schema for a table
func (f *Fetcher) GetTableSchema(ctx context.Context, schemaName, tableName string) (*model.TableSchema, error) {
	key := fmt.Sprintf("%s.%s", schemaName, tableName)

	// Check cache first
	if schema, ok := f.cache[key]; ok {
		return schema, nil
	}

	// Fetch columns
	columns, err := f.fetchColumns(ctx, schemaName, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch columns: %w", err)
	}

	// Fetch primary key
	primaryKey, err := f.fetchPrimaryKey(ctx, schemaName, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch primary key: %w", err)
	}

	// Fetch unique keys
	uniqueKeys, err := f.fetchUniqueKeys(ctx, schemaName, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch unique keys: %w", err)
	}

	// Fetch foreign keys
	foreignKeys, err := f.fetchForeignKeys(ctx, schemaName, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch foreign keys: %w", err)
	}

	schema := &model.TableSchema{
		Columns:     columns,
		PrimaryKey:  primaryKey,
		UniqueKeys:  uniqueKeys,
		ForeignKeys: foreignKeys,
	}

	// Cache the schema
	f.cache[key] = schema
	return schema, nil
}

func (f *Fetcher) fetchColumns(ctx context.Context, schemaName, tableName string) ([]model.ColumnDefinition, error) {
	query := `
		SELECT 
			a.attname as column_name,
			t.typname as type_name,
			a.atttypid as type_oid,
			a.attnum as column_position,
			NOT a.attnotnull as is_nullable,
			(SELECT pg_get_expr(d.adbin, d.adrelid)
			 FROM pg_attrdef d
			 WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum) as column_default,
			a.attgenerated as is_generated,
			t.typlen,
			CASE WHEN t.typelem <> 0 THEN true ELSE false END as is_array,
			t.typtype
		FROM pg_attribute a
		JOIN pg_type t ON a.atttypid = t.oid
		WHERE a.attrelid = (
			SELECT c.oid 
			FROM pg_class c 
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname = $1 AND c.relname = $2
		)
		AND a.attnum > 0
		AND NOT a.attisdropped
		ORDER BY a.attnum`

	rows, err := f.conn.Query(ctx, query, schemaName, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []model.ColumnDefinition
	for rows.Next() {
		var (
			col         model.ColumnDefinition
			typeName    string
			typeOid     uint32
			isNullable  bool
			defaultExpr pgtype.Text
			isGenerated string
			typeLen     int
			isArray     bool
			typeType    string
		)

		err := rows.Scan(
			&col.Name,
			&typeName,
			&typeOid,
			&col.Order,
			&isNullable,
			&defaultExpr,
			&isGenerated,
			&typeLen,
			&isArray,
			&typeType,
		)
		if err != nil {
			return nil, err
		}

		col.Optional = isNullable
		col.IsGenerated = isGenerated != ""
		if defaultExpr.Valid {
			col.Default = &defaultExpr.String
		}

		col.Type = model.DataType{
			Name:      typeName,
			TypeOid:   typeOid,
			ArrayType: isArray,
			Length:    typeLen,
		}

		columns = append(columns, col)
	}

	return columns, rows.Err()
}

func (f *Fetcher) fetchPrimaryKey(ctx context.Context, schemaName, tableName string) ([]string, error) {
	query := `
		SELECT a.attname
		FROM pg_index i
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		WHERE i.indrelid = (
			SELECT c.oid
			FROM pg_class c
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname = $1 AND c.relname = $2
		)
		AND i.indisprimary
		ORDER BY array_position(i.indkey, a.attnum)`

	rows, err := f.conn.Query(ctx, query, schemaName, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return nil, err
		}
		columns = append(columns, column)
	}

	return columns, rows.Err()
}

func (f *Fetcher) fetchUniqueKeys(ctx context.Context, schemaName, tableName string) ([][]string, error) {
	query := `
		WITH unique_indexes AS (
			SELECT i.indexrelid, array_agg(a.attname ORDER BY array_position(i.indkey, a.attnum)) as columns
			FROM pg_index i
			JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
			WHERE i.indrelid = (
				SELECT c.oid
				FROM pg_class c
				JOIN pg_namespace n ON n.oid = c.relnamespace
				WHERE n.nspname = $1 AND c.relname = $2
			)
			AND i.indisunique AND NOT i.indisprimary
			GROUP BY i.indexrelid
		)
		SELECT columns FROM unique_indexes`

	rows, err := f.conn.Query(ctx, query, schemaName, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var uniqueKeys [][]string
	for rows.Next() {
		var columns []string
		if err := rows.Scan(&columns); err != nil {
			return nil, err
		}
		uniqueKeys = append(uniqueKeys, columns)
	}

	return uniqueKeys, rows.Err()
}

func (f *Fetcher) fetchForeignKeys(ctx context.Context, schemaName, tableName string) ([]model.ForeignKey, error) {
	query := `
		SELECT
			array_agg(kcu.column_name ORDER BY kcu.ordinal_position) as fk_columns,
			ccu.table_schema as ref_schema,
			ccu.table_name as ref_table,
			array_agg(ccu.column_name ORDER BY kcu.ordinal_position) as ref_columns,
			rc.delete_rule,
			rc.update_rule
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu
			ON tc.constraint_name = kcu.constraint_name
			AND tc.table_schema = kcu.table_schema
		JOIN information_schema.constraint_column_usage ccu
			ON ccu.constraint_name = tc.constraint_name
			AND ccu.table_schema = tc.table_schema
		JOIN information_schema.referential_constraints rc
			ON tc.constraint_name = rc.constraint_name
		WHERE tc.constraint_type = 'FOREIGN KEY'
			AND tc.table_schema = $1
			AND tc.table_name = $2
		GROUP BY tc.constraint_name, ccu.table_schema, ccu.table_name, rc.delete_rule, rc.update_rule`

	rows, err := f.conn.Query(ctx, query, schemaName, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var fks []model.ForeignKey
	for rows.Next() {
		var fk model.ForeignKey
		err := rows.Scan(
			&fk.Columns,
			&fk.ReferencedSchema,
			&fk.ReferencedTable,
			&fk.ReferencedColumns,
			&fk.OnDelete,
			&fk.OnUpdate,
		)
		if err != nil {
			return nil, err
		}
		fks = append(fks, fk)
	}

	return fks, rows.Err()
}

// InvalidateCache removes a table's schema from the cache
func (f *Fetcher) InvalidateCache(schemaName, tableName string) {
	key := fmt.Sprintf("%s.%s", schemaName, tableName)
	delete(f.cache, key)
}

// InvalidateAllCache clears the entire schema cache
func (f *Fetcher) InvalidateAllCache() {
	f.cache = make(map[string]*model.TableSchema)
}
