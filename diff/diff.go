// Copyright 2026 The spanner-manager Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package diff

import (
	"fmt"
	"slices"
	"strings"

	"github.com/zchee/spanner-manager/sqlutil"
)

// Statement represents a DDL or DML statement produced by the diff engine.
type Statement struct {
	Kind sqlutil.StatementKind
	SQL  string
}

// Diff compares two Database schemas and returns the DDL statements needed
// to migrate from 'from' to 'to'.
func Diff(from, to *Database) ([]Statement, error) {
	var stmts []Statement

	// Phase 1: Drop indexes that no longer exist or have changed.
	for name, fromIdx := range from.Indexes {
		toIdx, exists := to.Indexes[name]
		if !exists || !indexEqual(fromIdx, toIdx) {
			stmts = append(stmts, Statement{
				Kind: sqlutil.KindDDL,
				SQL:  fmt.Sprintf("DROP INDEX %s", name),
			})
		}
	}

	// Phase 2: Drop tables that no longer exist.
	// Must drop child tables before parent tables.
	dropOrder := tableDeletionOrder(from, to)
	for _, name := range dropOrder {
		stmts = append(stmts, Statement{
			Kind: sqlutil.KindDDL,
			SQL:  fmt.Sprintf("DROP TABLE %s", name),
		})
	}

	// Phase 3: Alter existing tables (add/drop/modify columns).
	for name, toTable := range to.Tables {
		fromTable, exists := from.Tables[name]
		if !exists {
			continue // New table, handled in phase 4.
		}

		alterStmts, err := diffTable(fromTable, toTable)
		if err != nil {
			return nil, fmt.Errorf("diffing table %s: %w", name, err)
		}
		stmts = append(stmts, alterStmts...)
	}

	// Phase 4: Create new tables.
	// Must create parent tables before child tables.
	createOrder := tableCreationOrder(from, to)
	for _, name := range createOrder {
		table := to.Tables[name]
		stmts = append(stmts, Statement{
			Kind: sqlutil.KindDDL,
			SQL:  table.DDL,
		})
	}

	// Phase 5: Create new or changed indexes.
	for name, toIdx := range to.Indexes {
		fromIdx, exists := from.Indexes[name]
		if !exists || !indexEqual(fromIdx, toIdx) {
			stmts = append(stmts, Statement{
				Kind: sqlutil.KindDDL,
				SQL:  toIdx.DDL,
			})
		}
	}

	// Phase 6: Handle change streams.
	for name := range from.ChangeStreams {
		if _, exists := to.ChangeStreams[name]; !exists {
			stmts = append(stmts, Statement{
				Kind: sqlutil.KindDDL,
				SQL:  fmt.Sprintf("DROP CHANGE STREAM %s", name),
			})
		}
	}
	for name, toCS := range to.ChangeStreams {
		fromCS, exists := from.ChangeStreams[name]
		if !exists || fromCS.DDL != toCS.DDL {
			if exists {
				stmts = append(stmts, Statement{
					Kind: sqlutil.KindDDL,
					SQL:  fmt.Sprintf("DROP CHANGE STREAM %s", name),
				})
			}
			stmts = append(stmts, Statement{
				Kind: sqlutil.KindDDL,
				SQL:  toCS.DDL,
			})
		}
	}

	return stmts, nil
}

// diffTable compares two versions of the same table and returns ALTER statements.
func diffTable(from, to *Table) ([]Statement, error) {
	var stmts []Statement

	fromCols := columnMap(from)
	toCols := columnMap(to)

	// Drop removed columns.
	for _, col := range from.Columns {
		if _, exists := toCols[col.Name]; !exists {
			stmts = append(stmts, Statement{
				Kind: sqlutil.KindDDL,
				SQL:  fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", from.Name, col.Name),
			})
		}
	}

	// Add new columns.
	for _, col := range to.Columns {
		if _, exists := fromCols[col.Name]; !exists {
			stmts = append(stmts, addColumnStatement(to.Name, col)...)
		}
	}

	// Modify changed columns.
	for _, toCol := range to.Columns {
		fromCol, exists := fromCols[toCol.Name]
		if !exists {
			continue
		}
		alterStmts := alterColumnStatements(to.Name, fromCol, toCol)
		stmts = append(stmts, alterStmts...)
	}

	return stmts, nil
}

// addColumnStatement generates the DDL to add a column to a table.
func addColumnStatement(tableName string, col *Column) []Statement {
	var stmts []Statement

	colDef := fmt.Sprintf("%s %s", col.Name, col.Type)
	if col.NotNull {
		colDef += " NOT NULL"
	}
	if col.DefaultExpr != "" {
		colDef += fmt.Sprintf(" DEFAULT (%s)", col.DefaultExpr)
	}
	if col.GeneratedExpr != "" {
		colDef += fmt.Sprintf(" AS (%s) STORED", col.GeneratedExpr)
	}
	if col.Options != "" {
		colDef += " " + col.Options
	}

	stmts = append(stmts, Statement{
		Kind: sqlutil.KindDDL,
		SQL:  fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s", tableName, colDef),
	})

	return stmts
}

// alterColumnStatements generates DDL/DML to modify a column.
func alterColumnStatements(tableName string, from, to *Column) []Statement {
	var stmts []Statement

	if from.Type == to.Type && from.NotNull == to.NotNull && from.DefaultExpr == to.DefaultExpr {
		return nil
	}

	// Handle nullable → NOT NULL transition: need to fill NULLs first.
	if !from.NotNull && to.NotNull && to.DefaultExpr != "" {
		stmts = append(stmts, Statement{
			Kind: sqlutil.KindDML,
			SQL:  fmt.Sprintf("UPDATE %s SET %s = %s WHERE %s IS NULL", tableName, to.Name, to.DefaultExpr, to.Name),
		})
	}

	colDef := fmt.Sprintf("%s %s", to.Name, to.Type)
	if to.NotNull {
		colDef += " NOT NULL"
	}
	if to.DefaultExpr != "" {
		colDef += fmt.Sprintf(" DEFAULT (%s)", to.DefaultExpr)
	}

	stmts = append(stmts, Statement{
		Kind: sqlutil.KindDDL,
		SQL:  fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s %s", tableName, to.Name, to.Type+notNullSuffix(to.NotNull)),
	})

	return stmts
}

func notNullSuffix(notNull bool) string {
	if notNull {
		return " NOT NULL"
	}
	return ""
}

// columnMap builds a lookup map from column name to Column.
func columnMap(t *Table) map[string]*Column {
	m := make(map[string]*Column, len(t.Columns))
	for _, c := range t.Columns {
		m[c.Name] = c
	}
	return m
}

// indexEqual compares two indexes for structural equality.
func indexEqual(a, b *Index) bool {
	if a.TableName != b.TableName || a.Unique != b.Unique || a.NullFiltered != b.NullFiltered || a.Interleaved != b.Interleaved {
		return false
	}
	if len(a.Columns) != len(b.Columns) {
		return false
	}
	for i := range a.Columns {
		if a.Columns[i].Name != b.Columns[i].Name || a.Columns[i].Desc != b.Columns[i].Desc {
			return false
		}
	}
	return slices.Equal(a.Storing, b.Storing)
}

// tableDeletionOrder returns the names of tables in 'from' that are not in 'to',
// ordered so child tables come before parent tables.
func tableDeletionOrder(from, to *Database) []string {
	var dropped []string
	for name := range from.Tables {
		if _, exists := to.Tables[name]; !exists {
			dropped = append(dropped, name)
		}
	}

	// Sort: child tables first (tables whose parent is also being dropped should come first).
	slices.SortFunc(dropped, func(a, b string) int {
		aTable := from.Tables[a]
		bTable := from.Tables[b]
		// If a's parent is b, a should come first.
		if aTable.ParentTable == b {
			return -1
		}
		if bTable.ParentTable == a {
			return 1
		}
		return strings.Compare(a, b)
	})

	return dropped
}

// tableCreationOrder returns the names of tables in 'to' that are not in 'from',
// ordered so parent tables come before child tables.
func tableCreationOrder(from, to *Database) []string {
	var created []string
	for name := range to.Tables {
		if _, exists := from.Tables[name]; !exists {
			created = append(created, name)
		}
	}

	// Sort: parent tables first.
	slices.SortFunc(created, func(a, b string) int {
		aTable := to.Tables[a]
		bTable := to.Tables[b]
		// If b's parent is a, a should come first.
		if bTable.ParentTable == a {
			return -1
		}
		if aTable.ParentTable == b {
			return 1
		}
		return strings.Compare(a, b)
	})

	return created
}
