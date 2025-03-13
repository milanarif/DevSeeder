package main

import (
	"database/sql"
	"fmt"
)

// ForeignKey represents one FK relationship.
// Example: childTable.childColumn -> parentTable.parentColumn
type ForeignKey struct {
	FromTable  string
	FromColumn string
	ToTable    string
	ToColumn   string
	IsNullable bool
}

// ==============================================================================
// 1) Fetch *ALL* foreign keys from your DB (not just the subset).
// ==============================================================================
func FetchAllForeignKeys(db *sql.DB) ([]ForeignKey, error) {
	query := `
	SELECT
		kcu.table_name AS child_table,
		kcu.column_name AS child_column,
		kcu.referenced_table_name AS parent_table,
		kcu.referenced_column_name AS parent_column,
		CASE c.is_nullable WHEN 'YES' THEN TRUE ELSE FALSE END AS is_nullable
	FROM information_schema.key_column_usage kcu
	INNER JOIN information_schema.columns c
		ON c.table_schema = kcu.table_schema
		AND c.table_name = kcu.table_name
		AND c.column_name = kcu.column_name
	WHERE
		kcu.referenced_table_name IS NOT NULL
		AND kcu.table_schema = DATABASE();
	`
	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query all FKs: %w", err)
	}
	defer rows.Close()

	var fks []ForeignKey
	for rows.Next() {
		var fk ForeignKey
		if err := rows.Scan(
			&fk.FromTable,
			&fk.FromColumn,
			&fk.ToTable,
			&fk.ToColumn,
			&fk.IsNullable,
		); err != nil {
			return nil, err
		}
		fks = append(fks, fk)
	}
	return fks, nil
}
