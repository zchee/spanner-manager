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
	"reflect"
	"strings"

	"github.com/cloudspannerecosystem/memefish/ast"

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
	generator := &generator{
		from:                     from,
		to:                       to,
		willCreateOrAlterStreams: map[string]*ChangeStream{},
		alteredChangeStreamByKey: map[string]*ChangeStream{},
		droppedConstraintBySQL:   map[string]struct{}{},
		droppedIndexByKey:        map[string]struct{}{},
		droppedTableByKey:        map[string]struct{}{},
		droppedChangeStreamByKey: map[string]struct{}{},
	}

	return generator.generate(), nil
}

type generator struct {
	from *Database
	to   *Database

	willCreateOrAlterStreams map[string]*ChangeStream
	alteredChangeStreamByKey map[string]*ChangeStream

	droppedConstraintBySQL   map[string]struct{}
	droppedIndexByKey        map[string]struct{}
	droppedTableByKey        map[string]struct{}
	droppedChangeStreamByKey map[string]struct{}
	droppedGrants            []*Grant
}

type alterColumnDDL struct {
	Table      *ast.Path
	Def        *ast.ColumnDef
	SetOptions bool
}

func (a alterColumnDDL) SQL() string {
	sql := "ALTER TABLE " + a.Table.SQL() + " ALTER COLUMN " + a.Def.Name.SQL()

	if a.SetOptions {
		allowCommitTimestamp := false
		if v := optionValueByName(a.Def.Options, "allow_commit_timestamp"); v != nil {
			if lit, ok := v.(*ast.BoolLiteral); ok {
				allowCommitTimestamp = lit.Value
			}
		}
		if allowCommitTimestamp {
			return sql + " SET OPTIONS (allow_commit_timestamp = true)"
		}
		return sql + " SET OPTIONS (allow_commit_timestamp = null)"
	}

	sql += " " + a.Def.Type.SQL()
	if a.Def.NotNull {
		sql += " NOT NULL"
	}
	if a.Def.DefaultSemantics != nil {
		sql += " " + a.Def.DefaultSemantics.SQL()
	}
	if isColumnHidden(a.Def) {
		sql += " HIDDEN"
	}

	return sql
}

type updateDML struct {
	Table *ast.Path
	Def   *ast.ColumnDef
}

func (u updateDML) defaultValue() ast.Expr {
	if defaultSemantics := u.Def.DefaultSemantics; defaultSemantics != nil {
		if expr, ok := defaultSemantics.(*ast.ColumnDefaultExpr); ok {
			return expr.Expr
		}
	}

	switch t := u.Def.Type.(type) {
	case *ast.ArraySchemaType:
		return &ast.ArrayLiteral{}
	case *ast.ScalarSchemaType:
		return defaultByScalarTypeName(t.Name)
	case *ast.SizedSchemaType:
		return defaultByScalarTypeName(t.Name)
	default:
		return &ast.StringLiteral{Value: ""}
	}
}

func (u updateDML) SQL() string {
	defaultValue := u.defaultValue()
	return fmt.Sprintf(
		"UPDATE %s SET %s = %s WHERE %s IS NULL",
		u.Table.SQL(),
		u.Def.Name.SQL(),
		defaultValue.SQL(),
		u.Def.Name.SQL(),
	)
}

func (g *generator) generate() []Statement {
	var stmts []Statement

	for _, schema := range g.to.schemas {
		if _, exists := g.findSchemaByKey(g.from.schemas, schema.Key); !exists {
			stmts = append(stmts, ddlStatement(schema.Raw))
		}
	}
	for _, sequence := range g.to.sequences {
		fromSequence, exists := g.findSequenceByKey(g.from.sequences, sequence.Key)
		if !exists {
			stmts = append(stmts, ddlStatement(sequence.Raw))
			continue
		}
		if normalizeSQL(fromSequence.Raw.SQL()) != normalizeSQL(sequence.Raw.SQL()) {
			stmts = append(stmts, ddlStatement(&ast.DropSequence{Name: fromSequence.Raw.Name}))
			stmts = append(stmts, ddlStatement(sequence.Raw))
		}
	}

	stmts = append(stmts, g.generateAlterDatabaseOptions()...)

	for _, toTable := range g.to.tables {
		fromTable, exists := g.findTableByKey(g.from.tables, toTable.Key)
		if !exists {
			stmts = append(stmts, g.generateCreateTableAndIndexes(toTable)...)
			continue
		}
		if g.isDroppedTable(toTable.Key) {
			stmts = append(stmts, g.generateCreateTableAndIndexes(toTable)...)
			continue
		}
		if !g.interleaveEqual(fromTable, toTable) || !g.primaryKeyEqual(fromTable, toTable) {
			stmts = append(stmts, g.generateDropConstraintsIndexesAndTable(fromTable)...)
			stmts = append(stmts, g.generateCreateTableAndIndexes(toTable)...)
			continue
		}

		stmts = append(stmts, g.generateDropIndexes(fromTable, toTable)...)
		stmts = append(stmts, g.generateColumnDiffs(fromTable, toTable)...)
		stmts = append(stmts, g.generateCreateIndexes(fromTable, toTable)...)
		stmts = append(stmts, g.generateAlterIndexes(fromTable, toTable)...)
		stmts = append(stmts, g.generateConstraintDiffs(fromTable, toTable)...)
		stmts = append(stmts, g.generateRowDeletionPolicyDiffs(fromTable, toTable)...)
		stmts = append(stmts, g.generateCreateChangeStreamsForTable(toTable)...)
	}

	for _, toChangeStream := range g.to.globalChangeStreams {
		fromChangeStream, exists := g.findChangeStreamByKey(g.from, toChangeStream.Key)
		if !exists {
			stmts = append(stmts, ddlStatement(toChangeStream.Raw))
			continue
		}
		stmts = append(stmts, g.generateAlterChangeStream(fromChangeStream, toChangeStream)...)
	}

	for _, changeStream := range g.willCreateOrAlterStreams {
		fromChangeStream, exists := g.findChangeStreamByKey(g.from, changeStream.Key)
		if !exists || g.isDroppedChangeStream(changeStream.Key) {
			stmts = append(stmts, ddlStatement(changeStream.Raw))
			continue
		}
		if altered, ok := g.alteredChangeStreamByKey[changeStream.Key]; ok {
			fromChangeStream = altered
		}
		stmts = append(stmts, g.generateAlterChangeStream(fromChangeStream, changeStream)...)
	}

	for _, fromTable := range g.from.tables {
		if _, exists := g.findTableByKey(g.to.tables, fromTable.Key); !exists {
			stmts = append(stmts, g.generateDropConstraintsIndexesAndTable(fromTable)...)
		}
	}

	for _, fromChangeStream := range g.from.globalChangeStreams {
		if g.isDroppedChangeStream(fromChangeStream.Key) {
			continue
		}
		if _, exists := g.findChangeStreamByKey(g.to, fromChangeStream.Key); !exists {
			stmts = append(stmts, g.generateDropChangeStream(fromChangeStream)...)
		}
	}

	for _, fromSequence := range g.from.sequences {
		if _, exists := g.findSequenceByKey(g.to.sequences, fromSequence.Key); !exists {
			stmts = append(stmts, ddlStatement(&ast.DropSequence{Name: fromSequence.Raw.Name}))
		}
	}
	for _, fromSchema := range g.from.schemas {
		if _, exists := g.findSchemaByKey(g.to.schemas, fromSchema.Key); !exists {
			stmts = append(stmts, ddlStatement(&ast.DropSchema{Name: fromSchema.Raw.Name}))
		}
	}

	for _, fromTable := range g.from.tables {
		if _, exists := g.findTableByKey(g.to.tables, fromTable.Key); !exists {
			continue
		}
		for _, changeStream := range fromTable.ChangeStreams {
			if g.isDroppedChangeStream(changeStream.Key) {
				continue
			}
			if _, exists := g.findChangeStreamByKey(g.to, changeStream.Key); !exists {
				stmts = append(stmts, g.generateDropChangeStream(changeStream)...)
			}
		}
	}

	for _, toView := range g.to.views {
		if _, exists := g.findViewByKey(g.from.views, toView.Key); !exists {
			stmts = append(stmts, ddlStatement(toView.Raw))
			continue
		}
		stmts = append(stmts, g.generateReplaceView(toView)...)
	}
	for _, fromView := range g.from.views {
		if _, exists := g.findViewByKey(g.to.views, fromView.Key); !exists {
			stmts = append(stmts, g.generateDropView(fromView)...)
		}
	}

	for _, toRole := range g.to.roles {
		if _, exists := g.findRoleByKey(g.from.roles, toRole.Key); !exists {
			stmts = append(stmts, ddlStatement(toRole.Raw))
		}
	}
	for _, fromRole := range g.from.roles {
		if _, exists := g.findRoleByKey(g.to.roles, fromRole.Key); !exists {
			stmts = append(stmts, g.generateDropRole(fromRole)...)
		}
	}

	for _, fromGrant := range g.from.grants {
		if _, exists := g.findGrant(g.to.grants, fromGrant); !exists {
			if g.isDroppedGrant(fromGrant) {
				continue
			}
			stmts = append(stmts, g.generateRevokeAll(fromGrant)...)
		}
	}
	for _, toGrant := range g.to.grants {
		if fromGrant, exists := g.findGrant(g.from.grants, toGrant); exists {
			if g.isDroppedGrant(fromGrant) {
				stmts = append(stmts, ddlStatement(toGrant.Raw))
			}
			continue
		}
		stmts = append(stmts, ddlStatement(toGrant.Raw))
	}

	return stmts
}

func ddlStatement(sqler interface{ SQL() string }) Statement {
	return Statement{Kind: sqlutil.KindDDL, SQL: sqler.SQL()}
}

func dmlStatement(sqler interface{ SQL() string }) Statement {
	return Statement{Kind: sqlutil.KindDML, SQL: sqler.SQL()}
}

func (g *generator) generateAlterDatabaseOptions() []Statement {
	optionsFrom := make(map[string]string)
	optionsTo := make(map[string]string)
	if g.from.options != nil {
		for _, record := range g.from.options.Records {
			optionsFrom[record.Name.Name] = record.Value.SQL()
		}
	}
	if g.to.options != nil {
		for _, record := range g.to.options.Records {
			optionsTo[record.Name.Name] = record.Value.SQL()
		}
	}
	if reflect.DeepEqual(optionsFrom, optionsTo) {
		return nil
	}

	if g.to.alterDatabaseOptions == nil {
		if g.from.alterDatabaseOptions == nil {
			return nil
		}
		stmt := &ast.AlterDatabase{
			Name: g.from.alterDatabaseOptions.Name,
			Options: &ast.Options{Records: []*ast.OptionsDef{
				{Name: &ast.Ident{Name: "optimizer_version"}, Value: &ast.NullLiteral{}},
				{Name: &ast.Ident{Name: "version_retention_period"}, Value: &ast.NullLiteral{}},
				{Name: &ast.Ident{Name: "enable_key_visualizer"}, Value: &ast.NullLiteral{}},
			}},
		}
		return []Statement{ddlStatement(stmt)}
	}

	options := cloneOptions(g.to.alterDatabaseOptions.Options)
	if g.from.options != nil {
		for _, record := range g.from.options.Records {
			if optionValueByName(options, record.Name.Name) != nil {
				continue
			}
			options.Records = append(options.Records, &ast.OptionsDef{
				Name:  &ast.Ident{Name: record.Name.Name},
				Value: &ast.NullLiteral{},
			})
		}
	}

	stmt := &ast.AlterDatabase{
		Name:    g.to.alterDatabaseOptions.Name,
		Options: options,
	}
	return []Statement{ddlStatement(stmt)}
}

func (g *generator) generateCreateTableAndIndexes(table *Table) []Statement {
	var stmts []Statement
	stmts = append(stmts, ddlStatement(table.Raw))
	for _, index := range table.Indexes {
		stmts = append(stmts, ddlStatement(index.Raw))
	}
	for _, index := range table.SearchIndexes {
		stmts = append(stmts, ddlStatement(index.Raw))
	}
	for _, index := range table.VectorIndexes {
		stmts = append(stmts, ddlStatement(index.Raw))
	}
	for _, changeStream := range table.ChangeStreams {
		g.willCreateOrAlterStreams[changeStream.Key] = changeStream
	}
	return stmts
}

func (g *generator) generateDropConstraintsIndexesAndTable(table *Table) []Statement {
	if g.isDroppedTable(table.Key) {
		return nil
	}

	var stmts []Statement
	for _, child := range table.children {
		stmts = append(stmts, g.generateDropConstraintsIndexesAndTable(child)...)
	}
	for _, index := range table.Indexes {
		stmts = append(stmts, ddlStatement(&ast.DropIndex{Name: index.Raw.Name}))
		g.droppedIndexByKey[index.Key] = struct{}{}
	}
	for _, index := range table.SearchIndexes {
		stmts = append(stmts, ddlStatement(&ast.DropSearchIndex{Name: index.Raw.Name}))
		g.droppedIndexByKey[index.Key] = struct{}{}
	}
	for _, index := range table.VectorIndexes {
		stmts = append(stmts, ddlStatement(&ast.DropVectorIndex{Name: index.Raw.Name}))
		g.droppedIndexByKey[index.Key] = struct{}{}
	}
	for _, changeStream := range table.ChangeStreams {
		if g.isDroppedChangeStream(changeStream.Key) {
			continue
		}
		if tables, ok := changeStream.Raw.For.(*ast.ChangeStreamForTables); ok && len(tables.Tables) > 1 {
			var remaining []*ast.ChangeStreamForTable
			for _, watched := range tables.Tables {
				if comparableIdent(watched.TableName) == table.Key {
					continue
				}
				remaining = append(remaining, watched)
			}
			hasRemainingInTarget := false
			for _, watched := range remaining {
				if _, exists := g.findTableByKey(g.to.tables, comparableIdent(watched.TableName)); exists {
					hasRemainingInTarget = true
					break
				}
			}
			if len(remaining) > 0 && hasRemainingInTarget {
				if toChangeStream, exists := g.findChangeStreamByKey(g.to, changeStream.Key); exists {
					altered := &ChangeStream{
						Name: changeStream.Name,
						Key:  changeStream.Key,
						DDL:  changeStream.DDL,
						Raw: &ast.CreateChangeStream{
							Name:    changeStream.Raw.Name,
							For:     &ast.ChangeStreamForTables{Tables: remaining},
							Options: changeStream.Raw.Options,
						},
					}
					g.alteredChangeStreamByKey[changeStream.Key] = altered
					if _, willAlter := g.willCreateOrAlterStreams[changeStream.Key]; !willAlter && changeStreamType(toChangeStream.Raw) == changeStreamTypeTables {
						stmts = append(stmts, ddlStatement(&ast.AlterChangeStream{
							Name: changeStream.Raw.Name,
							ChangeStreamAlteration: &ast.ChangeStreamSetFor{
								For: &ast.ChangeStreamForTables{Tables: remaining},
							},
						}))
					}
					continue
				}
			}
		}
		if _, exists := g.findChangeStreamByKey(g.to, changeStream.Key); exists {
			if _, tableExists := g.findTableByKey(g.to.tables, table.Key); !tableExists {
				continue
			}
		}
		stmts = append(stmts, g.generateDropChangeStream(changeStream)...)
	}

	stmts = append(stmts, g.generateDropNamedConstraintsMatching(func(_ *Table, constraint *ast.TableConstraint) bool {
		fk, ok := constraint.Constraint.(*ast.ForeignKey)
		if !ok {
			return false
		}
		return comparablePath(fk.ReferenceTable) == table.Key
	})...)

	for _, grant := range g.from.grantsOnTable(table) {
		if g.isDroppedGrant(grant) {
			continue
		}
		g.droppedGrants = append(g.droppedGrants, grant)
	}

	stmts = append(stmts, ddlStatement(&ast.DropTable{Name: table.Raw.Name}))
	g.droppedTableByKey[table.Key] = struct{}{}

	return stmts
}

func (g *generator) generateConstraintDiffs(from, to *Table) []Statement {
	var stmts []Statement

	for _, toConstraint := range to.Constraints {
		if toConstraint.Name == nil {
			if _, exists := g.findUnnamedConstraint(from.Constraints, toConstraint); !exists {
				stmts = append(stmts, ddlStatement(&ast.AlterTable{
					Name:            to.Raw.Name,
					TableAlteration: &ast.AddTableConstraint{TableConstraint: toConstraint},
				}))
			}
			continue
		}

		fromConstraint, exists := g.findNamedConstraint(from.Constraints, comparableIdent(toConstraint.Name))
		if !exists || g.isDroppedConstraint(toConstraint) {
			stmts = append(stmts, ddlStatement(&ast.AlterTable{
				Name:            to.Raw.Name,
				TableAlteration: &ast.AddTableConstraint{TableConstraint: toConstraint},
			}))
			continue
		}
		if g.constraintEqual(fromConstraint, toConstraint) {
			continue
		}
		stmts = append(stmts, g.generateDropNamedConstraint(from.Raw.Name, fromConstraint)...)
		stmts = append(stmts, ddlStatement(&ast.AlterTable{
			Name:            to.Raw.Name,
			TableAlteration: &ast.AddTableConstraint{TableConstraint: toConstraint},
		}))
	}

	for _, fromConstraint := range from.Constraints {
		if fromConstraint.Name == nil {
			continue
		}
		if _, exists := g.findNamedConstraint(to.Constraints, comparableIdent(fromConstraint.Name)); !exists {
			stmts = append(stmts, g.generateDropNamedConstraint(from.Raw.Name, fromConstraint)...)
		}
	}

	return stmts
}

func (g *generator) generateRowDeletionPolicyDiffs(from, to *Table) []Statement {
	switch {
	case from.RowDeletionPolicy != nil && to.RowDeletionPolicy != nil:
		if g.rowDeletionPolicyEqual(from.RowDeletionPolicy, to.RowDeletionPolicy) {
			return nil
		}
		return []Statement{ddlStatement(&ast.AlterTable{
			Name: to.Raw.Name,
			TableAlteration: &ast.ReplaceRowDeletionPolicy{
				RowDeletionPolicy: to.RowDeletionPolicy.RowDeletionPolicy,
			},
		})}
	case from.RowDeletionPolicy != nil && to.RowDeletionPolicy == nil:
		return []Statement{ddlStatement(&ast.AlterTable{
			Name:            to.Raw.Name,
			TableAlteration: &ast.DropRowDeletionPolicy{},
		})}
	case from.RowDeletionPolicy == nil && to.RowDeletionPolicy != nil:
		return []Statement{ddlStatement(&ast.AlterTable{
			Name: to.Raw.Name,
			TableAlteration: &ast.AddRowDeletionPolicy{
				RowDeletionPolicy: to.RowDeletionPolicy.RowDeletionPolicy,
			},
		})}
	default:
		return nil
	}
}

func (g *generator) generateColumnDiffs(from, to *Table) []Statement {
	var stmts []Statement

	for _, toCol := range to.Raw.Columns {
		fromCol, exists := g.findColumnDefByKey(from.Raw.Columns, comparableIdent(toCol.Name))
		if !exists {
			if toCol.NotNull && toCol.DefaultSemantics == nil {
				stmts = append(stmts, ddlStatement(&ast.AlterTable{
					Name:            to.Raw.Name,
					TableAlteration: &ast.AddColumn{Column: g.setDefaultSemantics(toCol)},
				}))
				stmts = append(stmts, ddlStatement(&ast.AlterTable{
					Name: to.Raw.Name,
					TableAlteration: &ast.AlterColumn{
						Name:       toCol.Name,
						Alteration: &ast.AlterColumnDropDefault{},
					},
				}))
			} else {
				stmts = append(stmts, ddlStatement(&ast.AlterTable{
					Name:            to.Raw.Name,
					TableAlteration: &ast.AddColumn{Column: toCol},
				}))
			}
			continue
		}

		if isColumnHidden(fromCol) != isColumnHidden(toCol) {
			stmts = append(stmts, ddlStatement(alterColumnDDL{Table: to.Raw.Name, Def: toCol}))
			continue
		}
		if g.columnDefEqual(fromCol, toCol) {
			continue
		}

		requiresDropAndCreateDefault := func(defaultSemantics ast.ColumnDefaultSemantics) bool {
			if defaultSemantics == nil {
				return false
			}
			_, ok := defaultSemantics.(*ast.ColumnDefaultExpr)
			return !ok
		}

		if g.columnTypeEqual(fromCol, toCol) &&
			!requiresDropAndCreateDefault(fromCol.DefaultSemantics) &&
			!requiresDropAndCreateDefault(toCol.DefaultSemantics) {
			if scalar, ok := fromCol.Type.(*ast.ScalarSchemaType); ok && scalar.Name == ast.TimestampTypeName {
				if fromCol.NotNull != toCol.NotNull || !g.columnDefaultExprEqual(fromCol.DefaultSemantics, toCol.DefaultSemantics) {
					if !fromCol.NotNull && toCol.NotNull {
						stmts = append(stmts, dmlStatement(updateDML{Table: to.Raw.Name, Def: toCol}))
					}
					stmts = append(stmts, ddlStatement(alterColumnDDL{Table: to.Raw.Name, Def: toCol}))
				}
				if !g.optionValueEqual(fromCol.Options, toCol.Options, "allow_commit_timestamp") {
					stmts = append(stmts, ddlStatement(alterColumnDDL{Table: to.Raw.Name, Def: toCol, SetOptions: true}))
				}
			} else {
				if !fromCol.NotNull && toCol.NotNull {
					stmts = append(stmts, dmlStatement(updateDML{Table: to.Raw.Name, Def: toCol}))
				}
				stmts = append(stmts, ddlStatement(alterColumnDDL{Table: to.Raw.Name, Def: toCol}))
			}
		} else {
			stmts = append(stmts, g.generateDropAndCreateColumn(from, to, fromCol, toCol)...)
		}
	}

	for _, fromCol := range from.Raw.Columns {
		if _, exists := g.findColumnDefByKey(to.Raw.Columns, comparableIdent(fromCol.Name)); !exists {
			stmts = append(stmts, g.generateDropColumn(from.Raw.Name, fromCol.Name)...)
		}
	}

	return stmts
}

func (g *generator) generateDropColumn(table *ast.Path, column *ast.Ident) []Statement {
	var stmts []Statement
	stmts = append(stmts, g.generateDropNamedConstraintsMatching(func(owner *Table, constraint *ast.TableConstraint) bool {
		fk, ok := constraint.Constraint.(*ast.ForeignKey)
		if !ok {
			return false
		}
		if comparablePath(owner.Raw.Name) == comparablePath(table) {
			for _, col := range fk.Columns {
				if comparableIdent(col) == comparableIdent(column) {
					return true
				}
			}
		}
		if comparablePath(fk.ReferenceTable) == comparablePath(table) {
			for _, refColumn := range fk.ReferenceColumns {
				if comparableIdent(refColumn) == comparableIdent(column) {
					return true
				}
			}
		}
		return false
	})...)

	for _, grant := range g.from.grantsFromTablePath(table) {
		privilege, ok := grant.Raw.Privilege.(*ast.PrivilegeOnTable)
		if !ok {
			continue
		}
		if !hasPrivilegeOnColumn(privilege, column) {
			continue
		}
		if g.isDroppedGrant(grant) {
			continue
		}
		g.droppedGrants = append(g.droppedGrants, grant)
	}

	stmts = append(stmts, ddlStatement(&ast.AlterTable{
		Name:            table,
		TableAlteration: &ast.DropColumn{Name: column},
	}))
	return stmts
}

func (g *generator) generateDropAndCreateColumn(from, to *Table, fromCol, toCol *ast.ColumnDef) []Statement {
	var stmts []Statement

	for _, index := range g.findIndexesByColumn(from.Indexes, comparableIdent(fromCol.Name)) {
		if g.isDroppedIndex(index.Key) {
			continue
		}
		stmts = append(stmts, ddlStatement(&ast.DropIndex{Name: index.Raw.Name}))
		g.droppedIndexByKey[index.Key] = struct{}{}
	}
	for _, index := range g.findSearchIndexesByColumn(from.SearchIndexes, comparableIdent(fromCol.Name)) {
		if g.isDroppedIndex(index.Key) {
			continue
		}
		stmts = append(stmts, ddlStatement(&ast.DropSearchIndex{Name: index.Raw.Name}))
		g.droppedIndexByKey[index.Key] = struct{}{}
	}

	stmts = append(stmts, g.generateDropColumn(from.Raw.Name, fromCol.Name)...)

	if toCol.NotNull && toCol.DefaultSemantics == nil {
		stmts = append(stmts, ddlStatement(&ast.AlterTable{
			Name:            to.Raw.Name,
			TableAlteration: &ast.AddColumn{Column: g.setDefaultSemantics(toCol)},
		}))
		stmts = append(stmts, ddlStatement(&ast.AlterTable{
			Name: to.Raw.Name,
			TableAlteration: &ast.AlterColumn{
				Name:       toCol.Name,
				Alteration: &ast.AlterColumnDropDefault{},
			},
		}))
	} else {
		stmts = append(stmts, ddlStatement(&ast.AlterTable{
			Name:            to.Raw.Name,
			TableAlteration: &ast.AddColumn{Column: toCol},
		}))
	}

	for _, index := range g.findIndexesByColumn(from.Indexes, comparableIdent(fromCol.Name)) {
		stmts = append(stmts, ddlStatement(index.Raw))
	}
	for _, index := range g.findSearchIndexesByColumn(from.SearchIndexes, comparableIdent(fromCol.Name)) {
		stmts = append(stmts, ddlStatement(index.Raw))
	}

	return stmts
}

func (g *generator) generateDropIndexes(from, to *Table) []Statement {
	var stmts []Statement

	for _, toIndex := range to.Indexes {
		fromIndex, exists := g.findIndexByKey(from.Indexes, toIndex.Key)
		if exists && !g.indexEqualIgnoringStoring(fromIndex.Raw, toIndex.Raw) {
			stmts = append(stmts, ddlStatement(&ast.DropIndex{Name: fromIndex.Raw.Name}))
			g.droppedIndexByKey[fromIndex.Key] = struct{}{}
		}
	}
	for _, fromIndex := range from.Indexes {
		if _, exists := g.findIndexByKey(to.Indexes, fromIndex.Key); !exists {
			stmts = append(stmts, ddlStatement(&ast.DropIndex{Name: fromIndex.Raw.Name}))
			g.droppedIndexByKey[fromIndex.Key] = struct{}{}
		}
	}

	for _, toIndex := range to.SearchIndexes {
		fromIndex, exists := g.findSearchIndexByKey(from.SearchIndexes, toIndex.Key)
		if exists && !g.searchIndexEqual(fromIndex.Raw, toIndex.Raw) {
			stmts = append(stmts, ddlStatement(&ast.DropSearchIndex{Name: fromIndex.Raw.Name}))
			g.droppedIndexByKey[fromIndex.Key] = struct{}{}
		}
	}
	for _, fromIndex := range from.SearchIndexes {
		if _, exists := g.findSearchIndexByKey(to.SearchIndexes, fromIndex.Key); !exists {
			stmts = append(stmts, ddlStatement(&ast.DropSearchIndex{Name: fromIndex.Raw.Name}))
			g.droppedIndexByKey[fromIndex.Key] = struct{}{}
		}
	}
	for _, toIndex := range to.VectorIndexes {
		fromIndex, exists := g.findVectorIndexByKey(from.VectorIndexes, toIndex.Key)
		if exists && normalizeSQL(fromIndex.Raw.SQL()) != normalizeSQL(toIndex.Raw.SQL()) {
			stmts = append(stmts, ddlStatement(&ast.DropVectorIndex{Name: fromIndex.Raw.Name}))
			g.droppedIndexByKey[fromIndex.Key] = struct{}{}
		}
	}
	for _, fromIndex := range from.VectorIndexes {
		if _, exists := g.findVectorIndexByKey(to.VectorIndexes, fromIndex.Key); !exists {
			stmts = append(stmts, ddlStatement(&ast.DropVectorIndex{Name: fromIndex.Raw.Name}))
			g.droppedIndexByKey[fromIndex.Key] = struct{}{}
		}
	}

	return stmts
}

func (g *generator) generateCreateIndexes(from, to *Table) []Statement {
	var stmts []Statement
	for _, toIndex := range to.Indexes {
		fromIndex, exists := g.findIndexByKey(from.Indexes, toIndex.Key)
		if !exists || !g.indexEqualIgnoringStoring(fromIndex.Raw, toIndex.Raw) {
			stmts = append(stmts, ddlStatement(toIndex.Raw))
		}
	}
	for _, toIndex := range to.SearchIndexes {
		fromIndex, exists := g.findSearchIndexByKey(from.SearchIndexes, toIndex.Key)
		if !exists || !g.searchIndexEqual(fromIndex.Raw, toIndex.Raw) {
			stmts = append(stmts, ddlStatement(toIndex.Raw))
		}
	}
	for _, toIndex := range to.VectorIndexes {
		fromIndex, exists := g.findVectorIndexByKey(from.VectorIndexes, toIndex.Key)
		if !exists || normalizeSQL(fromIndex.Raw.SQL()) != normalizeSQL(toIndex.Raw.SQL()) {
			stmts = append(stmts, ddlStatement(toIndex.Raw))
		}
	}
	return stmts
}

func (g *generator) generateAlterIndexes(from, to *Table) []Statement {
	var stmts []Statement

	for _, toIndex := range to.Indexes {
		if toIndex.Raw.Storing == nil {
			continue
		}
		fromIndex, exists := g.findIndexByKey(from.Indexes, toIndex.Key)
		if !exists || !g.indexEqualIgnoringStoring(fromIndex.Raw, toIndex.Raw) {
			continue
		}
		for _, storing := range toIndex.Raw.Storing.Columns {
			if fromIndex.Raw.Storing != nil {
				if _, exists := g.findIdentByKey(fromIndex.Raw.Storing.Columns, comparableIdent(storing)); exists {
					continue
				}
			}
			stmts = append(stmts, ddlStatement(&ast.AlterIndex{
				Name: toIndex.Raw.Name,
				IndexAlteration: &ast.AddStoredColumn{
					Name: storing,
				},
			}))
		}
	}

	for _, fromIndex := range from.Indexes {
		if fromIndex.Raw.Storing == nil {
			continue
		}
		toIndex, exists := g.findIndexByKey(to.Indexes, fromIndex.Key)
		if !exists || !g.indexEqualIgnoringStoring(fromIndex.Raw, toIndex.Raw) {
			continue
		}
		for _, storing := range fromIndex.Raw.Storing.Columns {
			if toIndex.Raw.Storing != nil {
				if _, exists := g.findIdentByKey(toIndex.Raw.Storing.Columns, comparableIdent(storing)); exists {
					continue
				}
			}
			stmts = append(stmts, ddlStatement(&ast.AlterIndex{
				Name: toIndex.Raw.Name,
				IndexAlteration: &ast.DropStoredColumn{
					Name: storing,
				},
			}))
		}
	}

	return stmts
}

func (g *generator) generateCreateChangeStreamsForTable(table *Table) []Statement {
	for _, changeStream := range table.ChangeStreams {
		g.willCreateOrAlterStreams[changeStream.Key] = changeStream
	}
	return nil
}

func (g *generator) generateDropNamedConstraint(table *ast.Path, constraint *ast.TableConstraint) []Statement {
	if constraint.Name == nil {
		return nil
	}
	key := constraint.SQL()
	if _, exists := g.droppedConstraintBySQL[key]; exists {
		return nil
	}
	g.droppedConstraintBySQL[key] = struct{}{}
	return []Statement{ddlStatement(&ast.AlterTable{
		Name:            table,
		TableAlteration: &ast.DropConstraint{Name: constraint.Name},
	})}
}

func (g *generator) generateDropNamedConstraintsMatching(predicate func(*Table, *ast.TableConstraint) bool) []Statement {
	var stmts []Statement
	for _, table := range g.from.tables {
		for _, constraint := range table.Constraints {
			if predicate(table, constraint) {
				stmts = append(stmts, g.generateDropNamedConstraint(table.Raw.Name, constraint)...)
			}
		}
	}
	return stmts
}

func (g *generator) generateAlterChangeStream(from, to *ChangeStream) []Statement {
	var stmts []Statement

	switch {
	case changeStreamType(from.Raw) == changeStreamTypeAll && changeStreamType(to.Raw) == changeStreamTypeTables:
		stmts = append(stmts, ddlStatement(&ast.AlterChangeStream{
			Name:                   to.Raw.Name,
			ChangeStreamAlteration: &ast.ChangeStreamSetFor{For: to.Raw.For},
		}))
	case changeStreamType(from.Raw) == changeStreamTypeAll && changeStreamType(to.Raw) == changeStreamTypeNone:
		stmts = append(stmts, ddlStatement(&ast.AlterChangeStream{
			Name:                   to.Raw.Name,
			ChangeStreamAlteration: &ast.ChangeStreamDropForAll{},
		}))
	case changeStreamType(from.Raw) == changeStreamTypeTables && changeStreamType(to.Raw) == changeStreamTypeAll:
		stmts = append(stmts, ddlStatement(&ast.AlterChangeStream{
			Name:                   to.Raw.Name,
			ChangeStreamAlteration: &ast.ChangeStreamSetFor{For: to.Raw.For},
		}))
	case changeStreamType(from.Raw) == changeStreamTypeTables && changeStreamType(to.Raw) == changeStreamTypeNone:
		stmts = append(stmts, ddlStatement(&ast.AlterChangeStream{
			Name:                   to.Raw.Name,
			ChangeStreamAlteration: &ast.ChangeStreamDropForAll{},
		}))
	case changeStreamType(from.Raw) == changeStreamTypeTables && changeStreamType(to.Raw) == changeStreamTypeTables:
		if !g.changeStreamForEqual(from.Raw.For, to.Raw.For) {
			stmts = append(stmts, ddlStatement(&ast.AlterChangeStream{
				Name:                   to.Raw.Name,
				ChangeStreamAlteration: &ast.ChangeStreamSetFor{For: to.Raw.For},
			}))
		}
	case changeStreamType(from.Raw) == changeStreamTypeNone && changeStreamType(to.Raw) == changeStreamTypeAll:
		stmts = append(stmts, ddlStatement(&ast.AlterChangeStream{
			Name:                   to.Raw.Name,
			ChangeStreamAlteration: &ast.ChangeStreamSetFor{For: to.Raw.For},
		}))
	case changeStreamType(from.Raw) == changeStreamTypeNone && changeStreamType(to.Raw) == changeStreamTypeTables:
		stmts = append(stmts, ddlStatement(&ast.AlterChangeStream{
			Name:                   to.Raw.Name,
			ChangeStreamAlteration: &ast.ChangeStreamSetFor{For: to.Raw.For},
		}))
	}

	if !g.optionsEqual(from.Raw.Options, to.Raw.Options) {
		options := cloneOptions(to.Raw.Options)
		if options == nil {
			options = &ast.Options{}
		}
		if from.Raw.Options != nil {
			for _, record := range from.Raw.Options.Records {
				if optionValueByName(to.Raw.Options, record.Name.Name) != nil {
					continue
				}
				options.Records = append(options.Records, &ast.OptionsDef{
					Name:  &ast.Ident{Name: record.Name.Name},
					Value: &ast.NullLiteral{},
				})
			}
		}
		stmts = append(stmts, ddlStatement(&ast.AlterChangeStream{
			Name:                   to.Raw.Name,
			ChangeStreamAlteration: &ast.ChangeStreamSetOptions{Options: options},
		}))
	}

	return stmts
}

func (g *generator) generateDropChangeStream(changeStream *ChangeStream) []Statement {
	if g.isDroppedChangeStream(changeStream.Key) {
		return nil
	}
	for _, grant := range g.from.grantsOnChangeStream(changeStream) {
		if g.isDroppedGrant(grant) {
			continue
		}
		g.droppedGrants = append(g.droppedGrants, grant)
	}
	g.droppedChangeStreamByKey[changeStream.Key] = struct{}{}
	return []Statement{ddlStatement(&ast.DropChangeStream{Name: changeStream.Raw.Name})}
}

func (g *generator) isDroppedTable(key string) bool {
	_, exists := g.droppedTableByKey[key]
	return exists
}

func (g *generator) isDroppedIndex(key string) bool {
	_, exists := g.droppedIndexByKey[key]
	return exists
}

func (g *generator) isDroppedConstraint(constraint *ast.TableConstraint) bool {
	_, exists := g.droppedConstraintBySQL[constraint.SQL()]
	return exists
}

func (g *generator) isDroppedChangeStream(key string) bool {
	_, exists := g.droppedChangeStreamByKey[key]
	return exists
}

func (g *generator) isDroppedGrant(grant *Grant) bool {
	for _, dropped := range g.droppedGrants {
		if equalGrant(dropped, grant) {
			return true
		}
	}
	return false
}

func (g *generator) interleaveEqual(x, y *Table) bool {
	return x.ParentKey == y.ParentKey && normalizeOnDelete(x.OnDelete) == normalizeOnDelete(y.OnDelete)
}

func (g *generator) primaryKeyEqual(x, y *Table) bool {
	if len(x.PrimaryKey) != len(y.PrimaryKey) {
		return false
	}
	for i := range x.PrimaryKey {
		if !strings.EqualFold(x.PrimaryKey[i].Key, y.PrimaryKey[i].Key) {
			return false
		}
		if x.PrimaryKey[i].Desc != y.PrimaryKey[i].Desc {
			return false
		}
	}

	for _, pk := range y.Raw.PrimaryKeys {
		xCol, exists := g.findColumnByKey(x.Columns, comparableIdent(pk.Name))
		if !exists {
			return false
		}
		yCol, exists := g.findColumnByKey(y.Columns, comparableIdent(pk.Name))
		if !exists {
			return false
		}
		if !g.columnEqual(xCol, yCol) {
			return false
		}
	}

	return true
}

func (g *generator) columnDefEqual(x, y *ast.ColumnDef) bool {
	return strings.EqualFold(comparableIdent(x.Name), comparableIdent(y.Name)) &&
		x.Type.SQL() == y.Type.SQL() &&
		x.NotNull == y.NotNull &&
		columnDefaultSemanticsSQL(x.DefaultSemantics) == columnDefaultSemanticsSQL(y.DefaultSemantics) &&
		normalizeSQL(optionsSQL(x.Options)) == normalizeSQL(optionsSQL(y.Options)) &&
		isColumnHidden(x) == isColumnHidden(y)
}

func (g *generator) columnTypeEqual(x, y *ast.ColumnDef) bool {
	return x.Type.SQL() == y.Type.SQL()
}

func (g *generator) constraintEqual(x, y *ast.TableConstraint) bool {
	xSQL := normalizeConstraintSQL(x.SQL())
	ySQL := normalizeConstraintSQL(y.SQL())
	return strings.EqualFold(xSQL, ySQL)
}

func (g *generator) indexEqualIgnoringStoring(x, y *ast.CreateIndex) bool {
	if !strings.EqualFold(comparableIdents(x.Name.Idents...), comparableIdents(y.Name.Idents...)) {
		return false
	}
	if !strings.EqualFold(comparablePath(x.TableName), comparablePath(y.TableName)) {
		return false
	}
	if x.Unique != y.Unique || x.NullFiltered != y.NullFiltered {
		return false
	}
	if comparableInterleaveIn(x.InterleaveIn) != comparableInterleaveIn(y.InterleaveIn) {
		return false
	}
	if len(x.Keys) != len(y.Keys) {
		return false
	}
	for i := range x.Keys {
		if !strings.EqualFold(comparableIdent(x.Keys[i].Name), comparableIdent(y.Keys[i].Name)) {
			return false
		}
		if normalizeDirection(x.Keys[i].Dir) != normalizeDirection(y.Keys[i].Dir) {
			return false
		}
	}
	return true
}

func (g *generator) searchIndexEqual(x, y *ast.CreateSearchIndex) bool {
	return normalizeSQL(x.SQL()) == normalizeSQL(y.SQL())
}

func (g *generator) changeStreamForEqual(x, y ast.ChangeStreamFor) bool {
	return normalizeSQL(x.SQL()) == normalizeSQL(y.SQL())
}

func (g *generator) columnDefaultExprEqual(x, y ast.ColumnDefaultSemantics) bool {
	return columnDefaultSemanticsSQL(x) == columnDefaultSemanticsSQL(y)
}

func (g *generator) rowDeletionPolicyEqual(x, y *ast.CreateRowDeletionPolicy) bool {
	return normalizeSQL(x.SQL()) == normalizeSQL(y.SQL())
}

func (g *generator) optionsEqual(x, y *ast.Options) bool {
	return reflect.DeepEqual(optionValueMap(x), optionValueMap(y))
}

func (g *generator) optionValueEqual(x, y *ast.Options, name string) bool {
	return exprSQL(optionValueByName(x, name)) == exprSQL(optionValueByName(y, name))
}

func optionValueByName(options *ast.Options, name string) ast.Expr {
	if options == nil {
		return nil
	}
	for _, record := range options.Records {
		if record.Name.Name == name {
			return record.Value
		}
	}
	return nil
}

func defaultByScalarTypeName(name ast.ScalarTypeName) ast.Expr {
	switch name {
	case ast.BoolTypeName:
		return &ast.BoolLiteral{Value: false}
	case ast.Int64TypeName:
		return &ast.IntLiteral{Value: "0"}
	case ast.Float32TypeName, ast.Float64TypeName:
		return &ast.FloatLiteral{Value: "0"}
	case ast.StringTypeName:
		return &ast.StringLiteral{Value: ""}
	case ast.BytesTypeName:
		return &ast.BytesLiteral{Value: nil}
	case ast.DateTypeName:
		return &ast.DateLiteral{Value: &ast.StringLiteral{Value: "0001-01-01"}}
	case ast.TimestampTypeName:
		return &ast.TimestampLiteral{Value: &ast.StringLiteral{Value: "0001-01-01T00:00:00Z"}}
	case ast.NumericTypeName:
		return &ast.NumericLiteral{Value: &ast.StringLiteral{Value: "0"}}
	case ast.JSONTypeName:
		return &ast.JSONLiteral{Value: &ast.StringLiteral{Value: "{}"}}
	case ast.TokenListTypeName:
		return &ast.BytesLiteral{Value: nil}
	default:
		panic("unsupported scalar default")
	}
}

func (g *generator) columnEqual(x, y *Column) bool {
	return strings.EqualFold(x.Key, y.Key) &&
		x.Type == y.Type &&
		x.NotNull == y.NotNull &&
		x.DefaultExpr == y.DefaultExpr &&
		x.GeneratedExpr == y.GeneratedExpr &&
		normalizeSQL(x.Options) == normalizeSQL(y.Options) &&
		x.Hidden == y.Hidden
}

func (g *generator) setDefaultSemantics(col *ast.ColumnDef) *ast.ColumnDef {
	columnCopy := *col
	switch t := col.Type.(type) {
	case *ast.ArraySchemaType:
		columnCopy.DefaultSemantics = &ast.ColumnDefaultExpr{Expr: &ast.ArrayLiteral{}}
	case *ast.ScalarSchemaType:
		columnCopy.DefaultSemantics = &ast.ColumnDefaultExpr{Expr: defaultByScalarTypeName(t.Name)}
	case *ast.SizedSchemaType:
		columnCopy.DefaultSemantics = &ast.ColumnDefaultExpr{Expr: defaultByScalarTypeName(t.Name)}
	}
	return &columnCopy
}

func (g *generator) findTableByKey(tables []*Table, key string) (*Table, bool) {
	for _, table := range tables {
		if strings.EqualFold(table.Key, key) {
			return table, true
		}
	}
	return nil, false
}

func (g *generator) findColumnByKey(columns []*Column, key string) (*Column, bool) {
	for _, column := range columns {
		if strings.EqualFold(column.Key, key) {
			return column, true
		}
	}
	return nil, false
}

func (g *generator) findColumnDefByKey(columns []*ast.ColumnDef, key string) (*ast.ColumnDef, bool) {
	for _, column := range columns {
		if strings.EqualFold(comparableIdent(column.Name), key) {
			return column, true
		}
	}
	return nil, false
}

func (g *generator) findIdentByKey(idents []*ast.Ident, key string) (*ast.Ident, bool) {
	for _, ident := range idents {
		if strings.EqualFold(comparableIdent(ident), key) {
			return ident, true
		}
	}
	return nil, false
}

func (g *generator) findNamedConstraint(constraints []*ast.TableConstraint, key string) (*ast.TableConstraint, bool) {
	if key == "" {
		return nil, false
	}
	for _, constraint := range constraints {
		if comparableIdent(constraint.Name) == key {
			return constraint, true
		}
	}
	return nil, false
}

func (g *generator) findUnnamedConstraint(constraints []*ast.TableConstraint, item *ast.TableConstraint) (*ast.TableConstraint, bool) {
	for _, constraint := range constraints {
		if g.constraintEqual(constraint, item) {
			return constraint, true
		}
	}
	return nil, false
}

func (g *generator) findIndexByKey(indexes []*Index, key string) (*Index, bool) {
	for _, index := range indexes {
		if index.Key == key {
			return index, true
		}
	}
	return nil, false
}

func (g *generator) findIndexesByColumn(indexes []*Index, columnKey string) []*Index {
	var result []*Index
	for _, index := range indexes {
		for _, key := range index.Raw.Keys {
			if comparableIdent(key.Name) == columnKey {
				result = append(result, index)
				break
			}
		}
	}
	return result
}

func (g *generator) findSearchIndexByKey(indexes []*SearchIndex, key string) (*SearchIndex, bool) {
	for _, index := range indexes {
		if index.Key == key {
			return index, true
		}
	}
	return nil, false
}

func (g *generator) findSearchIndexesByColumn(indexes []*SearchIndex, columnKey string) []*SearchIndex {
	var result []*SearchIndex
	for _, index := range indexes {
		for _, part := range index.Raw.TokenListPart {
			if comparableIdentName(part.Name) == columnKey {
				result = append(result, index)
				break
			}
		}
	}
	return result
}

func (g *generator) findChangeStreamByKey(database *Database, key string) (*ChangeStream, bool) {
	for _, changeStream := range database.globalChangeStreams {
		if changeStream.Key == key {
			return changeStream, true
		}
	}
	for _, table := range database.tables {
		for _, changeStream := range table.ChangeStreams {
			if changeStream.Key == key {
				return changeStream, true
			}
		}
	}
	return nil, false
}

func (g *generator) findSchemaByKey(schemas []*Schema, key string) (*Schema, bool) {
	for _, schema := range schemas {
		if strings.EqualFold(schema.Key, key) {
			return schema, true
		}
	}
	return nil, false
}

func (g *generator) findSequenceByKey(sequences []*Sequence, key string) (*Sequence, bool) {
	for _, sequence := range sequences {
		if strings.EqualFold(sequence.Key, key) {
			return sequence, true
		}
	}
	return nil, false
}

func (g *generator) findVectorIndexByKey(indexes []*VectorIndex, key string) (*VectorIndex, bool) {
	for _, index := range indexes {
		if strings.EqualFold(index.Key, key) {
			return index, true
		}
	}
	return nil, false
}

func (g *generator) findViewByKey(views []*View, key string) (*View, bool) {
	for _, view := range views {
		if strings.EqualFold(view.Key, key) {
			return view, true
		}
	}
	return nil, false
}

func (g *generator) generateReplaceView(view *View) []Statement {
	replacement := *view.Raw
	replacement.OrReplace = true
	return []Statement{ddlStatement(&replacement)}
}

func (g *generator) generateDropView(view *View) []Statement {
	for _, grant := range g.from.grantsOnView(view) {
		if g.isDroppedGrant(grant) {
			continue
		}
		g.droppedGrants = append(g.droppedGrants, grant)
	}
	return []Statement{ddlStatement(&ast.DropView{Name: view.Raw.Name})}
}

func (g *generator) findRoleByKey(roles []*Role, key string) (*Role, bool) {
	for _, role := range roles {
		if strings.EqualFold(role.Key, key) {
			return role, true
		}
	}
	return nil, false
}

func (g *generator) generateDropRole(role *Role) []Statement {
	var stmts []Statement
	for _, grant := range g.from.grantsOnRole(role) {
		if g.isDroppedGrant(grant) {
			continue
		}
		g.droppedGrants = append(g.droppedGrants, grant)
		if !g.existsGrantResourceIn(grant, g.to) {
			continue
		}
		stmts = append(stmts, g.generateRevokeAll(grant)...)
	}
	stmts = append(stmts, ddlStatement(&ast.DropRole{Name: role.Raw.Name}))
	return stmts
}

func (g *generator) findGrant(grants []*Grant, grant *Grant) (*Grant, bool) {
	for _, existing := range grants {
		if equalGrant(existing, grant) {
			return existing, true
		}
	}
	return nil, false
}

func (g *generator) generateRevokeAll(grant *Grant) []Statement {
	if grant == nil || len(grant.Raw.Roles) == 0 {
		return nil
	}
	return []Statement{ddlStatement(&ast.Revoke{
		Privilege: grant.Raw.Privilege,
		Roles:     grant.Raw.Roles,
	})}
}

func (g *generator) existsGrantResourceIn(grant *Grant, database *Database) bool {
	if grant == nil || grant.Raw == nil || grant.Raw.Privilege == nil {
		return false
	}
	switch privilege := grant.Raw.Privilege.(type) {
	case *ast.PrivilegeOnTable:
		for _, name := range privilege.Names {
			for _, table := range database.tables {
				if comparablePath(name) == table.Key {
					return true
				}
			}
		}
		return false
	case *ast.SelectPrivilegeOnView:
		for _, name := range privilege.Names {
			if _, exists := g.findViewByKey(database.views, comparablePath(name)); exists {
				return true
			}
		}
		return false
	case *ast.SelectPrivilegeOnChangeStream:
		for _, name := range privilege.Names {
			if _, exists := g.findChangeStreamByKey(database, comparablePath(name)); exists {
				return true
			}
		}
		return false
	case *ast.ExecutePrivilegeOnTableFunction:
		return true
	default:
		return false
	}
}

func changeStreamType(changeStream *ast.CreateChangeStream) string {
	if changeStream.For == nil {
		return changeStreamTypeNone
	}
	switch changeStream.For.(type) {
	case *ast.ChangeStreamForTables:
		return changeStreamTypeTables
	default:
		return changeStreamTypeAll
	}
}

const (
	changeStreamTypeAll    = "ALL"
	changeStreamTypeNone   = "NONE"
	changeStreamTypeTables = "TABLES"
)

func cloneOptions(options *ast.Options) *ast.Options {
	if options == nil {
		return nil
	}
	cloned := &ast.Options{Records: make([]*ast.OptionsDef, 0, len(options.Records))}
	for _, record := range options.Records {
		cloned.Records = append(cloned.Records, &ast.OptionsDef{
			Name:  &ast.Ident{Name: record.Name.Name},
			Value: record.Value,
		})
	}
	return cloned
}

func optionsSQL(options *ast.Options) string {
	if options == nil {
		return ""
	}
	return options.SQL()
}

func columnDefaultSemanticsSQL(semantics ast.ColumnDefaultSemantics) string {
	if semantics == nil {
		return ""
	}
	return semantics.SQL()
}

func exprSQL(expr ast.Expr) string {
	if expr == nil {
		return ""
	}
	return expr.SQL()
}

func optionValueMap(options *ast.Options) map[string]string {
	result := make(map[string]string)
	if options == nil {
		return result
	}
	for _, record := range options.Records {
		result[record.Name.Name] = record.Value.SQL()
	}
	return result
}

func normalizeSQL(sql string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(sql)), " ")
}

func normalizeConstraintSQL(sql string) string {
	sql = strings.ReplaceAll(sql, "ON DELETE NO ACTION", "")
	return normalizeSQL(sql)
}

func normalizeOnDelete(onDelete string) string {
	if onDelete == "" {
		return "NO ACTION"
	}
	return onDelete
}

func normalizeDirection(dir ast.Direction) ast.Direction {
	if dir == "" {
		return ast.DirectionAsc
	}
	return dir
}

func comparableInterleaveIn(interleave *ast.InterleaveIn) string {
	if interleave == nil {
		return ""
	}
	return comparableIdent(interleave.TableName)
}

func equalGrant(a, b *Grant) bool {
	if a == nil || b == nil {
		return a == b
	}
	return equalASTGrant(a.Raw, b.Raw)
}

func equalASTGrant(a, b *ast.Grant) bool {
	if a == nil || b == nil {
		return a == b
	}
	if !equalPrivilege(a.Privilege, b.Privilege) {
		return false
	}
	return equalIdentLists(a.Roles, b.Roles)
}

func equalPrivilege(a, b ast.Privilege) bool {
	if a == nil || b == nil {
		return a == b
	}
	if reflect.TypeOf(a) != reflect.TypeOf(b) {
		return false
	}

	switch left := a.(type) {
	case *ast.PrivilegeOnTable:
		right := b.(*ast.PrivilegeOnTable)
		return equalPathLists(left.Names, right.Names) && equalTablePrivileges(left.Privileges, right.Privileges)
	case *ast.SelectPrivilegeOnChangeStream:
		right := b.(*ast.SelectPrivilegeOnChangeStream)
		return equalPathLists(left.Names, right.Names)
	case *ast.SelectPrivilegeOnView:
		right := b.(*ast.SelectPrivilegeOnView)
		return equalPathLists(left.Names, right.Names)
	case *ast.ExecutePrivilegeOnTableFunction:
		right := b.(*ast.ExecutePrivilegeOnTableFunction)
		return equalPathLists(left.Names, right.Names)
	case *ast.RolePrivilege:
		right := b.(*ast.RolePrivilege)
		return equalIdentLists(left.Names, right.Names)
	default:
		return normalizeSQL(fmt.Sprintf("%#v", a)) == normalizeSQL(fmt.Sprintf("%#v", b))
	}
}

func equalTablePrivileges(a, b []ast.TablePrivilege) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !matchTablePrivilege(a[i], b[i]) {
			return false
		}
	}
	return true
}

func matchTablePrivilege(a, b ast.TablePrivilege) bool {
	if reflect.TypeOf(a) != reflect.TypeOf(b) {
		return false
	}
	switch left := a.(type) {
	case *ast.SelectPrivilege:
		right := b.(*ast.SelectPrivilege)
		return equalIdentLists(left.Columns, right.Columns)
	case *ast.InsertPrivilege:
		right := b.(*ast.InsertPrivilege)
		return equalIdentLists(left.Columns, right.Columns)
	case *ast.UpdatePrivilege:
		right := b.(*ast.UpdatePrivilege)
		return equalIdentLists(left.Columns, right.Columns)
	case *ast.DeletePrivilege:
		return true
	default:
		return normalizeSQL(fmt.Sprintf("%#v", a)) == normalizeSQL(fmt.Sprintf("%#v", b))
	}
}

func equalIdentLists(a, b []*ast.Ident) bool {
	return comparableIdents(a...) == comparableIdents(b...)
}

func equalPathLists(a, b []*ast.Path) bool {
	var left []string
	for _, path := range a {
		left = append(left, comparablePath(path))
	}
	var right []string
	for _, path := range b {
		right = append(right, comparablePath(path))
	}
	return strings.Join(left, ",") == strings.Join(right, ",")
}

func hasPrivilegeOnColumn(privilege *ast.PrivilegeOnTable, column *ast.Ident) bool {
	if privilege == nil || column == nil {
		return false
	}
	target := comparableIdent(column)
	for _, tablePrivilege := range privilege.Privileges {
		switch p := tablePrivilege.(type) {
		case *ast.SelectPrivilege:
			for _, col := range p.Columns {
				if comparableIdent(col) == target {
					return true
				}
			}
		case *ast.InsertPrivilege:
			for _, col := range p.Columns {
				if comparableIdent(col) == target {
					return true
				}
			}
		case *ast.UpdatePrivilege:
			for _, col := range p.Columns {
				if comparableIdent(col) == target {
					return true
				}
			}
		}
	}
	return false
}
