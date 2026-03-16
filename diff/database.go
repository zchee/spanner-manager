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
	ChangeStreams map[string]*ChangeStream
}

// Table represents a Spanner table.
type Table struct {
	Name        string
	Columns     []*Column
	PrimaryKey  []*KeyColumn
	ParentTable string
	OnDelete    string
	DDL         string // Original DDL text.
}

// Column represents a table column.
type Column struct {
	Name          string
	Type          string // SQL type text (e.g., "STRING(MAX)").
	NotNull       bool
	DefaultExpr   string
	GeneratedExpr string
	Options       string
}

// KeyColumn represents a column in a primary key or index.
type KeyColumn struct {
	Name string
	Desc bool
}

// Index represents a secondary index.
type Index struct {
	Name         string
	TableName    string
	Columns      []*KeyColumn
	Unique       bool
	NullFiltered bool
	Storing      []string
	Interleaved  string // Interleaved table name, if any.
	DDL          string
}

// ChangeStream represents a change stream.
type ChangeStream struct {
	Name string
	DDL  string
}

// ParseDatabase builds a Database from a list of raw DDL statement strings.
func ParseDatabase(ddlStrings []string) (*Database, error) {
	db := &Database{
		Tables:        make(map[string]*Table),
		Indexes:       make(map[string]*Index),
		ChangeStreams: make(map[string]*ChangeStream),
	}

	// Join all DDL strings and parse with memefish.
	fullDDL := strings.Join(ddlStrings, ";\n")
	ddls, err := sqlutil.ParseDDLs(fullDDL)
	if err != nil {
		return nil, fmt.Errorf("parsing DDL: %w", err)
	}

	for _, ddl := range ddls {
		switch stmt := ddl.(type) {
		case *ast.CreateTable:
			table := parseCreateTable(stmt)
			db.Tables[table.Name] = table
		case *ast.CreateIndex:
			idx := parseCreateIndex(stmt)
			db.Indexes[idx.Name] = idx
		case *ast.CreateChangeStream:
			cs := &ChangeStream{
				Name: stmt.Name.Name,
				DDL:  stmt.SQL(),
			}
			db.ChangeStreams[cs.Name] = cs
		}
	}

	return db, nil
}

// pathName extracts the simple name from a Path (last identifier).
func pathName(p *ast.Path) string {
	if p == nil || len(p.Idents) == 0 {
		return ""
	}
	return p.Idents[len(p.Idents)-1].Name
}

func parseCreateTable(stmt *ast.CreateTable) *Table {
	t := &Table{
		Name: pathName(stmt.Name),
		DDL:  stmt.SQL(),
	}

	// Parse columns.
	for _, col := range stmt.Columns {
		c := &Column{
			Name:    col.Name.Name,
			Type:    col.Type.SQL(),
			NotNull: col.NotNull,
		}
		// Extract default/generated expressions from DefaultSemantics.
		switch ds := col.DefaultSemantics.(type) {
		case *ast.ColumnDefaultExpr:
			c.DefaultExpr = ds.Expr.SQL()
		case *ast.GeneratedColumnExpr:
			c.GeneratedExpr = ds.Expr.SQL()
		}
		if col.Options != nil {
			c.Options = col.Options.SQL()
		}
		t.Columns = append(t.Columns, c)
	}

	// Parse primary key.
	for _, key := range stmt.PrimaryKeys {
		t.PrimaryKey = append(t.PrimaryKey, &KeyColumn{
			Name: key.Name.Name,
			Desc: key.Dir == ast.DirectionDesc,
		})
	}

	// Parse cluster (interleave).
	if stmt.Cluster != nil {
		t.ParentTable = pathName(stmt.Cluster.TableName)
		if stmt.Cluster.OnDelete == ast.OnDeleteCascade {
			t.OnDelete = "CASCADE"
		} else {
			t.OnDelete = "NO ACTION"
		}
	}

	return t
}

func parseCreateIndex(stmt *ast.CreateIndex) *Index {
	idx := &Index{
		Name:         pathName(stmt.Name),
		TableName:    pathName(stmt.TableName),
		Unique:       stmt.Unique,
		NullFiltered: stmt.NullFiltered,
		DDL:          stmt.SQL(),
	}

	for _, key := range stmt.Keys {
		idx.Columns = append(idx.Columns, &KeyColumn{
			Name: key.Name.Name,
			Desc: key.Dir == ast.DirectionDesc,
		})
	}

	if stmt.Storing != nil {
		for _, col := range stmt.Storing.Columns {
			idx.Storing = append(idx.Storing, col.Name)
		}
	}

	if stmt.InterleaveIn != nil {
		idx.Interleaved = stmt.InterleaveIn.TableName.Name
	}

	return idx
}
