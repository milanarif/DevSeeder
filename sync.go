package main

import (
	"database/sql"
	"fmt"
	"log"
	"slices"
	"strings"
)

// -----------------------------------------------------------------------------
// Example: the BFS-based partial data copy
// -----------------------------------------------------------------------------
func SyncPartialData(
	prodDB, devDB *sql.DB,
	allFks []ForeignKey, // all known FKs
	requestedTables map[string]int, // { tableName : rowLimit }
	resetTables bool, // whether to truncate dev tables first
) error {

	//----------------------------------------------------------------
	// 1) Build adjacency: child -> slice of (ParentTable, ParentColumn, ChildColumn)
	// child:[{parentTable: string, parentColumn: string, childColumn: string}]
	//----------------------------------------------------------------
	// 1) Build adjacency: child -> slice of (ParentTable, ParentColumn, ChildColumn)
	childToParents := make(map[string][]FkEdge)
	for _, fk := range allFks {
		// If you want to skip self-referencing
		if fk.FromTable == fk.ToTable {
			continue
		}

		// IMPORTANT: skip if the child column is nullable
		if fk.IsNullable {
			// This means the child -> parent is optional,
			// so we don't treat it as a "hard" dependency for topological ordering
			continue
		}

		childToParents[fk.FromTable] = append(childToParents[fk.FromTable], FkEdge{
			ParentTable:  fk.ToTable,
			ParentColumn: fk.ToColumn,
			ChildColumn:  fk.FromColumn,
		})
	}

	//----------------------------------------------------------------
	// 2) Maintain sets of row IDs we need to copy for each table
	//----------------------------------------------------------------
	//     table -> set of "id" values
	rowSets := make(map[string]map[int64]bool)

	// Initialize sets (for all tables we see in FKs, plus requested tables)
	for _, fk := range allFks {
		if _, ok := rowSets[fk.FromTable]; !ok {
			rowSets[fk.FromTable] = make(map[int64]bool)
		}
		if _, ok := rowSets[fk.ToTable]; !ok {
			rowSets[fk.ToTable] = make(map[int64]bool)
		}
	}

	// Initialize sets for requested tables in case they're not referenced by FKs
	for tbl := range requestedTables {
		if _, ok := rowSets[tbl]; !ok {
			rowSets[tbl] = make(map[int64]bool)
		}
	}

	//----------------------------------------------------------------
	// 3) Seed the sets with user-requested tables’ limited rowIDs
	// Example:
	// 	If user requested table "products" with limit 2
	// 	rowSets["products"] = map[int64]bool{3: true, 4: true}
	//----------------------------------------------------------------
	for table, limit := range requestedTables {
		ids, err := fetchSomeIDs(prodDB, table, limit)
		if err != nil {
			return fmt.Errorf("fetchSomeIDs error for table %s: %w", table, err)
		}
		for _, id := range ids {
			rowSets[table][id] = true
		}
	}

	//----------------------------------------------------------------
	// 4) BFS queue approach to add all *parent* IDs needed
	//----------------------------------------------------------------
	//    If we discover new child->parent references, add them to the parent's set,
	//    re-queue that parent to find *its* parents, etc.

	queue := make([]string, 0)
	enqueued := make(map[string]bool)

	// Start BFS with each requested table
	for t := range requestedTables {
		queue = append(queue, t)
		enqueued[t] = true
	}

	// Process the queue until there’s nothing left to explore.
	for len(queue) > 0 {
		childTable := queue[0]
		queue = queue[1:]
		enqueued[childTable] = false

		// If we have no row-IDs in this child, skip
		childIDs := rowSets[childTable]
		if len(childIDs) == 0 {
			continue
		}

		// For each parent relationship child -> parent
		// An edge here represents a parent-child relationship
		// Ex. { suppliers id supplier_id}
		edges := childToParents[childTable]
		for _, edge := range edges {
			newParentIDs, err := fetchReferencedParentIDs(prodDB, childTable, edge, childIDs)
			if err != nil {
				return fmt.Errorf("fetchReferencedParentIDs error: %w", err)
			}
			// Insert discovered IDs into parent's rowSets
			parentSet := rowSets[edge.ParentTable]
			changed := false
			for pid := range newParentIDs {
				if !parentSet[pid] {
					parentSet[pid] = true
					changed = true
				}
			}
			// If parent's set grew, re-queue the parent table unless it's already enqueued
			if changed && !enqueued[edge.ParentTable] {
				queue = append(queue, edge.ParentTable)
				enqueued[edge.ParentTable] = true
			}
		}
	}

	//----------------------------------------------------------------
	// 5) Build final list of tables that actually have rowIDs
	//----------------------------------------------------------------
	var tablesNeedingCopy []string
	for tableName, idSet := range rowSets {
		if len(idSet) > 0 {
			tablesNeedingCopy = append(tablesNeedingCopy, tableName)
		}
	}

	//----------------------------------------------------------------
	// 6) Topologically sort them so parents come before children
	//----------------------------------------------------------------
	sorted, err := partialTopoSort(allFks, tablesNeedingCopy)
	if err != nil {
		return fmt.Errorf("topoSort error: %w", err)
	}

	//----------------------------------------------------------------
	// 7) Copy data in topological order
	//----------------------------------------------------------------
	for _, table := range sorted {
		idSet := rowSets[table]
		if len(idSet) == 0 {
			continue
		}
		log.Printf("Copying %d rows from table %s", len(idSet), table)

		// Optionally truncate dev table
		if resetTables {
			if err := truncateTable(devDB, table); err != nil {
				return fmt.Errorf("truncate error on %s: %w", table, err)
			}
		}

		// 7a. Fetch the actual rows from prod
		rowsData, columns, err := fetchRowsByIDs(prodDB, table, idSet)
		if err != nil {
			return fmt.Errorf("fetchRowsByIDs error: %w", err)
		}

		// 7b. Insert them into dev
		if err := insertRows(devDB, table, columns, rowsData); err != nil {
			return fmt.Errorf("insertRows error: %w", err)
		}
	}

	return nil
}

// -----------------------------------------------------------------------------
// HELPER TYPES AND FUNCTIONS
// -----------------------------------------------------------------------------

// FkEdge is a small struct describing child->parent columns
type FkEdge struct {
	ParentTable  string
	ParentColumn string
	ChildColumn  string
}

// truncateTable optionally wipes the dev table
func truncateTable(db *sql.DB, table string) error {
	sqlStr := fmt.Sprintf("TRUNCATE TABLE `%s`", table)
	_, err := db.Exec(sqlStr)
	return err
}

// fetchSomeIDs: fetch up to "limit" IDs from `table` (ordered by `id`)
func fetchSomeIDs(db *sql.DB, table string, limit int) ([]int64, error) {
	sqlStr := fmt.Sprintf(`SELECT id FROM %s ORDER BY id LIMIT %d`, table, limit)
	rows, err := db.Query(sqlStr)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		results = append(results, id)
	}
	return results, nil
}

// fetchReferencedParentIDs: given a child's rowIDs, figure out the parent's IDs they reference.
// For example, if the child FK column is childCol=parent_id, we do:
//
//	SELECT DISTINCT parent_id FROM child WHERE id IN (childIDs) AND parent_id IS NOT NULL
func fetchReferencedParentIDs(
	db *sql.DB,
	childTable string,
	edge FkEdge,
	childIDs map[int64]bool,
) (map[int64]bool, error) {

	if len(childIDs) == 0 {
		return nil, nil
	}

	// Create the IN(...) clause
	var idList []string
	for id := range childIDs {
		idList = append(idList, fmt.Sprintf("%d", id))
	}
	inClause := strings.Join(idList, ",")

	query := fmt.Sprintf(
		`SELECT DISTINCT %s FROM %s WHERE id IN (%s) AND %s IS NOT NULL`,
		edge.ChildColumn, childTable, inClause, edge.ChildColumn,
	)

	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	parentIDs := make(map[int64]bool)
	for rows.Next() {
		var pid int64
		if err := rows.Scan(&pid); err != nil {
			return nil, err
		}
		parentIDs[pid] = true
	}
	return parentIDs, nil
}

// fetchRowsByIDs: SELECT * FROM `table` WHERE id IN (...)
func fetchRowsByIDs(db *sql.DB, table string, idSet map[int64]bool) ([][]interface{}, []string, error) {
	if len(idSet) == 0 {
		return nil, nil, nil
	}

	// Build IN(...) list
	var idList []string
	for id := range idSet {
		idList = append(idList, fmt.Sprintf("%d", id))
	}
	inClause := strings.Join(idList, ",")

	sqlStr := fmt.Sprintf("SELECT * FROM `%s` WHERE id IN (%s)", table, inClause)
	rows, err := db.Query(sqlStr)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	// Column names
	columns, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}

	var allData [][]interface{}
	for rows.Next() {
		rowVals := make([]interface{}, len(columns))
		rowPtrs := make([]interface{}, len(columns))
		for i := range rowVals {
			rowPtrs[i] = &rowVals[i]
		}
		if err := rows.Scan(rowPtrs...); err != nil {
			return nil, nil, err
		}
		allData = append(allData, rowVals)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	return allData, columns, nil
}

// insertRows does a multi-row INSERT to dev table
func insertRows(db *sql.DB, table string, columns []string, rowsData [][]interface{}) error {
	if len(rowsData) == 0 {
		return nil
	}

	colList := backtickJoin(columns)
	placeholders := "(" + strings.Repeat("?,", len(columns)-1) + "?)"

	var valueBlocks []string
	var allArgs []interface{}

	for _, row := range rowsData {
		valueBlocks = append(valueBlocks, placeholders)
		allArgs = append(allArgs, row...)
	}

	sqlStr := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES %s",
		table,
		colList,
		strings.Join(valueBlocks, ","),
	)

	_, err := db.Exec(sqlStr, allArgs...)
	return err
}

// backtickJoin: returns "`col1`,`col2`,`col3`"
func backtickJoin(cols []string) string {
	var b strings.Builder
	for i, c := range cols {
		if i > 0 {
			b.WriteRune(',')
		}
		b.WriteRune('`')
		b.WriteString(c)
		b.WriteRune('`')
	}
	return b.String()
}

// -----------------------------------------------------------------------------
// partialTopoSort is a simpler topological sort that only sorts the subset
// -----------------------------------------------------------------------------
func partialTopoSort(allFks []ForeignKey, neededTables []string) ([]string, error) {
	neededSet := make(map[string]bool)
	for _, t := range neededTables {
		neededSet[t] = true
	}

	depMap := make(map[string][]string)
	inDegree := make(map[string]int)

	// Initialize
	for _, table := range neededTables {
		depMap[table] = nil
		inDegree[table] = 0
	}

	// For each FK in your subset:
	// If child & parent are in neededSet AND it's NOT nullable
	for _, fk := range allFks {
		if fk.FromTable == fk.ToTable {
			continue
		}
		if neededSet[fk.FromTable] && neededSet[fk.ToTable] && !fk.IsNullable {
			depMap[fk.FromTable] = append(depMap[fk.FromTable], fk.ToTable)
		}
	}

	// Calculate in-degree
	for table, parents := range depMap {
		for range parents {
			inDegree[table] = inDegree[table] + 1
		}
	}

	// Start with all tables that have in-degree = 0
	var queue []string
	for t, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, t)
		}
	}

	var sorted []string
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		sorted = append(sorted, cur)

		// Decrease in-degree for each child that depends on `cur` in depMap
		for child, parents := range depMap {
			if slices.Contains(parents, cur) {
				inDegree[child]--
				if inDegree[child] == 0 {
					queue = append(queue, child)
				}
			}
		}
	}

	// If sorted < neededTables, there's a cycle or missing dependency
	if len(sorted) != len(neededTables) {
		return nil, fmt.Errorf("topological sort: cycle detected or missing table references")
	}
	return sorted, nil
}
