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

// Package diff implements AST-based schema comparison for Cloud Spanner DDL.
package diff

import (
	"fmt"
	"strings"

	"github.com/cloudspannerecosystem/memefish/ast"

	"github.com/zchee/spanner-manager/sqlutil"
)

// Database represents a parsed Spanner database schema.
type Database struct {
	Tables        map[string]*Table
	Indexes       map[string]*Index
	SearchIndexes map[string]*SearchIndex
	VectorIndexes map[string]*VectorIndex
	ChangeStreams map[string]*ChangeStream
	Schemas       map[string]*Schema
	Sequences     map[string]*Sequence

	tables               []*Table
	tableByKey           map[string]*Table
	schemas              []*Schema
	sequences            []*Sequence
	globalChangeStreams  []*ChangeStream
	views                []*View
	roles                []*Role
	grants               []*Grant
	alterDatabaseOptions *ast.AlterDatabase
	options              *ast.Options
}

// Table represents a Spanner table.
type Table struct {
	Name        string
	Key         string
	Path        *ast.Path
	Columns     []*Column
	PrimaryKey  []*KeyColumn
	ParentTable string
	ParentKey   string
	OnDelete    string
	DDL         string

	Indexes           []*Index
	SearchIndexes     []*SearchIndex
	VectorIndexes     []*VectorIndex
	Constraints       []*ast.TableConstraint
	RowDeletionPolicy *ast.CreateRowDeletionPolicy
	ChangeStreams     []*ChangeStream

	Raw      *ast.CreateTable
	children []*Table
}

// Column represents a table column.
type Column struct {
	Name          string
	Key           string
	Type          string // SQL type text (e.g., "STRING(MAX)").
	NotNull       bool
	DefaultExpr   string
	GeneratedExpr string
	Options       string
	Hidden        bool

	Raw *ast.ColumnDef
}

// KeyColumn represents a column in a primary key or index.
type KeyColumn struct {
	Name string
	Key  string
	Desc bool
}

// Index represents a secondary index.
type Index struct {
	Name         string
	Key          string
	TableName    string
	TableKey     string
	Columns      []*KeyColumn
	Unique       bool
	NullFiltered bool
	Storing      []string
	Interleaved  string
	DDL          string

	Raw *ast.CreateIndex
}

// SearchIndex represents a search index.
type SearchIndex struct {
	Name         string
	Key          string
	TableName    string
	TableKey     string
	TokenColumns []string
	DDL          string

	Raw *ast.CreateSearchIndex
}

// ChangeStream represents a change stream.
type ChangeStream struct {
	Name string
	Key  string
	DDL  string

	Raw *ast.CreateChangeStream
}

// Schema represents a named schema.
type Schema struct {
	Name string
	Key  string
	DDL  string

	Raw *ast.CreateSchema
}

// Sequence represents a sequence object.
type Sequence struct {
	Name string
	Key  string
	DDL  string

	Raw *ast.CreateSequence
}

// VectorIndex represents a vector index definition.
type VectorIndex struct {
	Name       string
	Key        string
	TableName  string
	TableKey   string
	ColumnName string
	DDL        string

	Raw *ast.CreateVectorIndex
}

// View represents a view definition.
type View struct {
	Name string
	Key  string
	DDL  string

	Raw *ast.CreateView
}

// Role represents a role definition.
type Role struct {
	Name string
	Key  string
	DDL  string

	Raw *ast.CreateRole
}

// Grant represents a grant definition.
type Grant struct {
	Key string
	DDL string

	Raw *ast.Grant
}

// ParseDatabase builds a Database from a list of raw DDL statement strings.
func ParseDatabase(ddlStrings []string) (*Database, error) {
	db := &Database{
		Tables:        make(map[string]*Table),
		Indexes:       make(map[string]*Index),
		SearchIndexes: make(map[string]*SearchIndex),
		VectorIndexes: make(map[string]*VectorIndex),
		ChangeStreams: make(map[string]*ChangeStream),
		Schemas:       make(map[string]*Schema),
		Sequences:     make(map[string]*Sequence),
		tableByKey:    make(map[string]*Table),
	}

	fullDDL := strings.Join(ddlStrings, ";\n")
	ddls, err := sqlutil.ParseDDLs(fullDDL)
	if err != nil {
		return nil, fmt.Errorf("parsing DDL: %w", err)
	}

	for _, ddl := range ddls {
		switch stmt := ddl.(type) {
		case *ast.CreateSchema:
			schema := parseCreateSchema(stmt)
			db.Schemas[schema.Name] = schema
			db.schemas = append(db.schemas, schema)
		case *ast.CreateTable:
			table := parseCreateTable(stmt)
			db.Tables[table.Name] = table
			db.tables = append(db.tables, table)
			db.tableByKey[table.Key] = table
		case *ast.CreateIndex:
			idx := parseCreateIndex(stmt)
			db.Indexes[idx.Name] = idx
			table, ok := db.tableByKey[idx.TableKey]
			if !ok {
				return nil, fmt.Errorf("cannot find table for index %s", stmt.Name.SQL())
			}
			table.Indexes = append(table.Indexes, idx)
		case *ast.CreateSearchIndex:
			idx := parseCreateSearchIndex(stmt)
			db.SearchIndexes[idx.Name] = idx
			table, ok := db.tableByKey[idx.TableKey]
			if !ok {
				return nil, fmt.Errorf("cannot find table for search index %s", stmt.Name.SQL())
			}
			table.SearchIndexes = append(table.SearchIndexes, idx)
		case *ast.CreateVectorIndex:
			idx := parseCreateVectorIndex(stmt)
			db.VectorIndexes[idx.Name] = idx
			table, ok := db.tableByKey[idx.TableKey]
			if !ok {
				return nil, fmt.Errorf("cannot find table for vector index %s", stmt.Name.SQL())
			}
			table.VectorIndexes = append(table.VectorIndexes, idx)
		case *ast.AlterTable:
			table, ok := db.tableByKey[comparablePath(stmt.Name)]
			if !ok {
				return nil, fmt.Errorf("cannot find table for alteration %s", stmt.Name.SQL())
			}
			switch alteration := stmt.TableAlteration.(type) {
			case *ast.AddTableConstraint:
				table.Constraints = append(table.Constraints, alteration.TableConstraint)
			default:
				return nil, fmt.Errorf("unsupported table alteration in ParseDatabase: %T", alteration)
			}
		case *ast.AlterDatabase:
			db.alterDatabaseOptions = stmt
			db.options = stmt.Options
		case *ast.CreateSequence:
			sequence := parseCreateSequence(stmt)
			db.Sequences[sequence.Name] = sequence
			db.sequences = append(db.sequences, sequence)
		case *ast.CreateChangeStream:
			cs := parseCreateChangeStream(stmt)
			db.ChangeStreams[cs.Name] = cs

			if tables, ok := stmt.For.(*ast.ChangeStreamForTables); ok {
				for _, watched := range tables.Tables {
					table, exists := db.tableByKey[comparableIdent(watched.TableName)]
					if !exists {
						return nil, fmt.Errorf("cannot find table for change stream %s", stmt.Name.SQL())
					}
					table.ChangeStreams = append(table.ChangeStreams, cs)
				}
				continue
			}

			db.globalChangeStreams = append(db.globalChangeStreams, cs)
		case *ast.CreateView:
			db.views = append(db.views, parseCreateView(stmt))
		case *ast.CreateRole:
			db.roles = append(db.roles, parseCreateRole(stmt))
		case *ast.Grant:
			db.grants = append(db.grants, parseGrant(stmt))
		}
	}

	for _, table := range db.tables {
		if table.ParentKey == "" {
			continue
		}
		parent, ok := db.tableByKey[table.ParentKey]
		if !ok {
			return nil, fmt.Errorf("parent table %s not found", table.ParentTable)
		}
		parent.children = append(parent.children, table)
	}

	return db, nil
}

func parseCreateTable(stmt *ast.CreateTable) *Table {
	table := &Table{
		Name:              pathName(stmt.Name),
		Key:               comparablePath(stmt.Name),
		Path:              stmt.Name,
		DDL:               stmt.SQL(),
		Constraints:       append([]*ast.TableConstraint(nil), stmt.TableConstraints...),
		RowDeletionPolicy: stmt.RowDeletionPolicy,
		Raw:               stmt,
	}

	for _, col := range stmt.Columns {
		column := &Column{
			Name:    col.Name.Name,
			Key:     comparableIdent(col.Name),
			Type:    col.Type.SQL(),
			NotNull: col.NotNull,
			Hidden:  isColumnHidden(col),
			Raw:     col,
		}

		switch ds := col.DefaultSemantics.(type) {
		case *ast.ColumnDefaultExpr:
			column.DefaultExpr = ds.Expr.SQL()
		case *ast.GeneratedColumnExpr:
			column.GeneratedExpr = ds.Expr.SQL()
		}
		if col.Options != nil {
			column.Options = col.Options.SQL()
		}

		table.Columns = append(table.Columns, column)
	}

	for _, key := range stmt.PrimaryKeys {
		table.PrimaryKey = append(table.PrimaryKey, &KeyColumn{
			Name: key.Name.Name,
			Key:  comparableIdent(key.Name),
			Desc: key.Dir == ast.DirectionDesc,
		})
	}

	if stmt.Cluster != nil {
		table.ParentTable = pathName(stmt.Cluster.TableName)
		table.ParentKey = comparablePath(stmt.Cluster.TableName)
		if stmt.Cluster.OnDelete == ast.OnDeleteCascade {
			table.OnDelete = "CASCADE"
		} else {
			table.OnDelete = "NO ACTION"
		}
	}

	return table
}

func parseCreateIndex(stmt *ast.CreateIndex) *Index {
	index := &Index{
		Name:         pathName(stmt.Name),
		Key:          comparableIdents(stmt.Name.Idents...),
		TableName:    pathName(stmt.TableName),
		TableKey:     comparablePath(stmt.TableName),
		Unique:       stmt.Unique,
		NullFiltered: stmt.NullFiltered,
		DDL:          stmt.SQL(),
		Raw:          stmt,
	}

	for _, key := range stmt.Keys {
		index.Columns = append(index.Columns, &KeyColumn{
			Name: key.Name.Name,
			Key:  comparableIdent(key.Name),
			Desc: key.Dir == ast.DirectionDesc,
		})
	}

	if stmt.Storing != nil {
		for _, col := range stmt.Storing.Columns {
			index.Storing = append(index.Storing, col.Name)
		}
	}

	if stmt.InterleaveIn != nil {
		index.Interleaved = stmt.InterleaveIn.TableName.Name
	}

	return index
}

func parseCreateSearchIndex(stmt *ast.CreateSearchIndex) *SearchIndex {
	index := &SearchIndex{
		Name:      pathName(stmt.Name),
		Key:       comparablePath(stmt.Name),
		TableName: pathName(stmt.TableName),
		TableKey:  comparablePath(stmt.TableName),
		DDL:       stmt.SQL(),
		Raw:       stmt,
	}

	for _, part := range stmt.TokenListPart {
		index.TokenColumns = append(index.TokenColumns, comparableIdentName(part.Name))
	}

	return index
}

func parseCreateChangeStream(stmt *ast.CreateChangeStream) *ChangeStream {
	return &ChangeStream{
		Name: stmt.Name.Name,
		Key:  comparableIdent(stmt.Name),
		DDL:  stmt.SQL(),
		Raw:  stmt,
	}
}

func parseCreateSchema(stmt *ast.CreateSchema) *Schema {
	return &Schema{
		Name: stmt.Name.Name,
		Key:  comparableIdent(stmt.Name),
		DDL:  stmt.SQL(),
		Raw:  stmt,
	}
}

func parseCreateSequence(stmt *ast.CreateSequence) *Sequence {
	return &Sequence{
		Name: pathName(stmt.Name),
		Key:  comparablePath(stmt.Name),
		DDL:  stmt.SQL(),
		Raw:  stmt,
	}
}

func parseCreateVectorIndex(stmt *ast.CreateVectorIndex) *VectorIndex {
	return &VectorIndex{
		Name:       stmt.Name.Name,
		Key:        comparableIdent(stmt.Name),
		TableName:  stmt.TableName.Name,
		TableKey:   comparableIdent(stmt.TableName),
		ColumnName: stmt.ColumnName.Name,
		DDL:        stmt.SQL(),
		Raw:        stmt,
	}
}

func parseCreateView(stmt *ast.CreateView) *View {
	return &View{
		Name: pathName(stmt.Name),
		Key:  comparablePath(stmt.Name),
		DDL:  stmt.SQL(),
		Raw:  stmt,
	}
}

func parseCreateRole(stmt *ast.CreateRole) *Role {
	return &Role{
		Name: stmt.Name.Name,
		Key:  comparableIdent(stmt.Name),
		DDL:  stmt.SQL(),
		Raw:  stmt,
	}
}

func parseGrant(stmt *ast.Grant) *Grant {
	return &Grant{
		Key: grantKey(stmt),
		DDL: stmt.SQL(),
		Raw: stmt,
	}
}

// pathName extracts the simple name from a Path (last identifier).
func pathName(p *ast.Path) string {
	if p == nil || len(p.Idents) == 0 {
		return ""
	}
	return p.Idents[len(p.Idents)-1].Name
}

func comparablePath(p *ast.Path) string {
	if p == nil {
		return ""
	}
	return comparableIdents(p.Idents...)
}

func comparableIdents(idents ...*ast.Ident) string {
	var parts []string
	for _, ident := range idents {
		if ident == nil {
			continue
		}
		parts = append(parts, strings.ToLower(ident.Name))
	}
	return strings.Join(parts, ".")
}

func comparableIdent(ident *ast.Ident) string {
	if ident == nil {
		return ""
	}
	return strings.ToLower(ident.Name)
}

func comparableIdentName(name string) string {
	return strings.ToLower(name)
}

func grantKey(stmt *ast.Grant) string {
	if stmt == nil {
		return ""
	}
	return strings.ToLower(stmt.SQL())
}

func (d *Database) grantsOnTable(table *Table) []*Grant {
	var result []*Grant
	if table == nil {
		return result
	}

	for _, grant := range d.grants {
		privilege, ok := grant.Raw.Privilege.(*ast.PrivilegeOnTable)
		if !ok {
			continue
		}
		for _, name := range privilege.Names {
			if comparablePath(name) == table.Key {
				result = append(result, grant)
				break
			}
		}
	}

	return result
}

func (d *Database) grantsFromTablePath(path *ast.Path) []*Grant {
	var result []*Grant
	if path == nil {
		return result
	}

	target := comparablePath(path)
	for _, grant := range d.grants {
		privilege, ok := grant.Raw.Privilege.(*ast.PrivilegeOnTable)
		if !ok {
			continue
		}
		for _, name := range privilege.Names {
			if comparablePath(name) == target || pathName(name) == target {
				result = append(result, grant)
				break
			}
		}
	}

	return result
}

func (d *Database) grantsOnView(view *View) []*Grant {
	var result []*Grant
	if view == nil {
		return result
	}

	for _, grant := range d.grants {
		privilege, ok := grant.Raw.Privilege.(*ast.SelectPrivilegeOnView)
		if !ok {
			continue
		}
		for _, name := range privilege.Names {
			if comparablePath(name) == view.Key {
				result = append(result, grant)
				break
			}
		}
	}

	return result
}

func (d *Database) grantsOnChangeStream(changeStream *ChangeStream) []*Grant {
	var result []*Grant
	if changeStream == nil {
		return result
	}

	for _, grant := range d.grants {
		privilege, ok := grant.Raw.Privilege.(*ast.SelectPrivilegeOnChangeStream)
		if !ok {
			continue
		}
		for _, name := range privilege.Names {
			if comparablePath(name) == changeStream.Key {
				result = append(result, grant)
				break
			}
		}
	}

	return result
}

func (d *Database) grantsOnRole(role *Role) []*Grant {
	var result []*Grant
	if role == nil {
		return result
	}

	for _, grant := range d.grants {
		for _, roleIdent := range grant.Raw.Roles {
			if comparableIdent(roleIdent) == role.Key {
				result = append(result, grant)
				break
			}
		}
	}

	return result
}

func isColumnHidden(col *ast.ColumnDef) bool {
	return !col.Hidden.Invalid() && col.Hidden != 0
}
