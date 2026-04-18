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

package codegen

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"

	"cloud.google.com/go/spanner"
	"github.com/cloudspannerecosystem/memefish/ast"

	"github.com/zchee/spanner-manager/spannerutil"
	"github.com/zchee/spanner-manager/sqlutil"
)

// SchemaSource loads a Schema from some source.
type SchemaSource interface {
	Load(ctx context.Context) (*Schema, error)
}

// InformationSchemaSource loads schema from a live Spanner database.
type InformationSchemaSource struct {
	client *spannerutil.Client
}

// NewInformationSchemaSource creates a new InformationSchemaSource.
func NewInformationSchemaSource(client *spannerutil.Client) *InformationSchemaSource {
	return &InformationSchemaSource{client: client}
}

// Load queries INFORMATION_SCHEMA to build a Schema.
func (s *InformationSchemaSource) Load(ctx context.Context) (*Schema, error) {
	schema := &Schema{}

	// Query tables.
	tables, err := s.loadTables(ctx)
	if err != nil {
		return nil, err
	}
	indexDDLsByTable, err := s.loadIndexDDLsByTable(ctx)
	if err != nil {
		indexDDLsByTable = nil
	}

	for _, tableName := range tables {
		t, err := s.loadType(ctx, tableName)
		if err != nil {
			return nil, fmt.Errorf("loading table %s: %w", tableName, err)
		}
		if indexDDLs := indexDDLsByTable[tableName]; len(indexDDLs) > 0 {
			t.Indexes = buildIndexInfos(indexDDLs, t.Fields)
		}
		schema.Types = append(schema.Types, *t)
	}

	return schema, nil
}

func (s *InformationSchemaSource) loadTables(ctx context.Context) ([]string, error) {
	iter := s.client.Single().Query(ctx, spanner.Statement{
		SQL: `SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
			  WHERE TABLE_SCHEMA = '' ORDER BY TABLE_NAME`,
	})

	var tables []string
	if err := iter.Do(func(row *spanner.Row) error {
		var name string
		if err := row.Columns(&name); err != nil {
			return err
		}
		tables = append(tables, name)
		return nil
	}); err != nil {
		return nil, fmt.Errorf("querying tables: %w", err)
	}

	return tables, nil
}

func (s *InformationSchemaSource) loadType(ctx context.Context, tableName string) (*Type, error) {
	t := &Type{
		Name:  snakeToCamel(tableName),
		Table: tableName,
	}

	commitTimestampColumns, err := s.loadCommitTimestampColumns(ctx, tableName)
	if err != nil {
		return nil, fmt.Errorf("querying commit timestamp options: %w", err)
	}

	// Query columns.
	iter := s.client.Single().Query(ctx, spanner.Statement{
		SQL: `SELECT COLUMN_NAME, SPANNER_TYPE, IS_NULLABLE, IS_GENERATED,
			         ORDINAL_POSITION
			  FROM INFORMATION_SCHEMA.COLUMNS
			  WHERE TABLE_SCHEMA = '' AND TABLE_NAME = @table
			  ORDER BY ORDINAL_POSITION`,
		Params: map[string]any{"table": tableName},
	})

	if err := iter.Do(func(row *spanner.Row) error {
		var colName, spannerType, isNullable, isGenerated string
		var ordinal int64
		if err := row.Columns(&colName, &spannerType, &isNullable, &isGenerated, &ordinal); err != nil {
			return err
		}

		typeInfo, err := goTypeForSpannerTypeString(spannerType, isNullable == "YES")
		if err != nil {
			return err
		}

		f := Field{
			Name:                 snakeToCamel(colName),
			ColumnName:           colName,
			SpannerType:          spannerType,
			BaseSpannerType:      typeInfo.BaseSpannerType,
			IsArray:              typeInfo.IsArray,
			GoType:               typeInfo.Expr,
			NotNull:              isNullable == "NO",
			IsGenerated:          isGenerated == "ALWAYS",
			AllowCommitTimestamp: commitTimestampColumns[colName],
			Imports:              typeInfo.Imports,
		}
		t.Fields = append(t.Fields, f)
		return nil
	}); err != nil {
		return nil, fmt.Errorf("querying columns: %w", err)
	}

	// Query primary key columns.
	pkIter := s.client.Single().Query(ctx, spanner.Statement{
		SQL: `SELECT kcu.COLUMN_NAME
			  FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
			  JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
			    ON kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
			  WHERE tc.TABLE_NAME = @table AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
			  ORDER BY kcu.ORDINAL_POSITION`,
		Params: map[string]any{"table": tableName},
	})

	pkCols := make(map[string]bool)
	if err := pkIter.Do(func(row *spanner.Row) error {
		var name string
		if err := row.Columns(&name); err != nil {
			return err
		}
		pkCols[name] = true
		return nil
	}); err != nil {
		return nil, fmt.Errorf("querying primary key: %w", err)
	}

	for i := range t.Fields {
		if pkCols[t.Fields[i].ColumnName] {
			t.Fields[i].IsPrimaryKey = true
			t.PrimaryKeyFields = append(t.PrimaryKeyFields, t.Fields[i])
		}
	}

	indexKeyColumns, indexStoringColumns, err := s.loadIndexMetadata(ctx, tableName)
	if err != nil {
		return nil, fmt.Errorf("querying index columns: %w", err)
	}

	// Query indexes.
	idxIter := s.client.Single().Query(ctx, spanner.Statement{
		SQL: `SELECT INDEX_NAME, IS_UNIQUE
			  FROM INFORMATION_SCHEMA.INDEXES
			  WHERE TABLE_NAME = @table AND INDEX_TYPE = 'INDEX'
			  ORDER BY INDEX_NAME`,
		Params: map[string]any{"table": tableName},
	})

	if err := idxIter.Do(func(row *spanner.Row) error {
		var name string
		var isUnique bool
		if err := row.Columns(&name, &isUnique); err != nil {
			return err
		}
		idx, err := buildIndexInfo(name, isUnique, indexKeyColumns[name], indexStoringColumns[name], t.Fields)
		if err != nil {
			return err
		}
		t.Indexes = append(t.Indexes, idx)
		return nil
	}); err != nil {
		return nil, fmt.Errorf("querying indexes: %w", err)
	}

	refreshTypeMetadata(t)

	return t, nil
}

func (s *InformationSchemaSource) loadCommitTimestampColumns(ctx context.Context, tableName string) (map[string]bool, error) {
	iter := s.client.Single().Query(ctx, spanner.Statement{
		SQL: `SELECT COLUMN_NAME, OPTION_VALUE
			  FROM INFORMATION_SCHEMA.COLUMN_OPTIONS
			  WHERE TABLE_SCHEMA = '' AND TABLE_NAME = @table
			    AND OPTION_NAME = 'allow_commit_timestamp'`,
		Params: map[string]any{"table": tableName},
	})

	columns := make(map[string]bool)
	if err := iter.Do(func(row *spanner.Row) error {
		var columnName, optionValue string
		if err := row.Columns(&columnName, &optionValue); err != nil {
			return err
		}
		columns[columnName] = strings.EqualFold(strings.TrimSpace(optionValue), "TRUE")
		return nil
	}); err != nil {
		return nil, err
	}

	return columns, nil
}

func (s *InformationSchemaSource) loadIndexDDLsByTable(ctx context.Context) (map[string][]*ast.CreateIndex, error) {
	ddlStatements, err := s.client.GetDatabaseDDL(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting database DDL for index metadata: %w", err)
	}
	ddls, err := sqlutil.ParseDDLs(strings.Join(ddlStatements, ";\n"))
	if err != nil {
		return nil, fmt.Errorf("parsing database DDL for index metadata: %w", err)
	}
	return createIndexDDLsByTable(ddls), nil
}

// DDLFileSource loads schema from a DDL file using memefish.
type DDLFileSource struct {
	path string
}

// NewDDLFileSource creates a new DDLFileSource.
func NewDDLFileSource(path string) *DDLFileSource {
	return &DDLFileSource{path: path}
}

// Load parses the DDL file and builds a Schema.
func (s *DDLFileSource) Load(_ context.Context) (*Schema, error) {
	data, err := os.ReadFile(s.path)
	if err != nil {
		return nil, fmt.Errorf("reading DDL file: %w", err)
	}

	ddls, err := sqlutil.ParseDDLs(string(data))
	if err != nil {
		return nil, fmt.Errorf("parsing DDL: %w", err)
	}

	schema := &Schema{}
	typeIndexByTable := make(map[string]int)
	for _, ddl := range ddls {
		ct, ok := ddl.(*ast.CreateTable)
		if !ok {
			continue
		}

		tableName := pathName(ct.Name)
		t := &Type{
			Name:  snakeToCamel(tableName),
			Table: tableName,
		}

		for _, col := range ct.Columns {
			spannerType := col.Type.SQL()
			notNull := col.NotNull
			typeInfo := goTypeForSchemaType(col.Type, !notNull)

			var isGenerated bool
			if col.DefaultSemantics != nil {
				_, isGenerated = col.DefaultSemantics.(*ast.GeneratedColumnExpr)
			}
			f := Field{
				Name:                 snakeToCamel(col.Name.Name),
				ColumnName:           col.Name.Name,
				SpannerType:          spannerType,
				BaseSpannerType:      typeInfo.BaseSpannerType,
				IsArray:              typeInfo.IsArray,
				GoType:               typeInfo.Expr,
				NotNull:              notNull,
				IsGenerated:          isGenerated,
				AllowCommitTimestamp: columnHasTrueOption(col.Options, "allow_commit_timestamp"),
				Imports:              typeInfo.Imports,
			}
			t.Fields = append(t.Fields, f)
		}

		for _, key := range ct.PrimaryKeys {
			for i := range t.Fields {
				if t.Fields[i].ColumnName == key.Name.Name {
					t.Fields[i].IsPrimaryKey = true
					t.PrimaryKeyFields = append(t.PrimaryKeyFields, t.Fields[i])
					break
				}
			}
		}

		refreshTypeMetadata(t)
		typeIndexByTable[t.Table] = len(schema.Types)
		schema.Types = append(schema.Types, *t)
		tableIndexes[t.Table] = len(schema.Types) - 1
	}

	for _, ddl := range ddls {
		ci, ok := ddl.(*ast.CreateIndex)
		if !ok {
			continue
		}

		tableName := pathName(ci.TableName)
		indexPos, ok := tableIndexes[tableName]
		if !ok {
			return nil, fmt.Errorf("index %s references unknown table %s", pathName(ci.Name), tableName)
		}

		idx, err := buildIndexInfoFromDDL(&schema.Types[indexPos], ci)
		if err != nil {
			return nil, err
		}
		schema.Types[indexPos].Indexes = append(schema.Types[indexPos].Indexes, idx)
	}

	for tableName, indexDDLs := range createIndexDDLsByTable(ddls) {
		typeIndex, ok := typeIndexByTable[tableName]
		if !ok {
			continue
		}
		schema.Types[typeIndex].Indexes = buildIndexInfos(indexDDLs, schema.Types[typeIndex].Fields)
	}

	for tableName, indexDDLs := range createIndexDDLsByTable(ddls) {
		typeIndex, ok := typeIndexByTable[tableName]
		if !ok {
			continue
		}
		schema.Types[typeIndex].Indexes = buildIndexInfos(indexDDLs, schema.Types[typeIndex].Fields)
	}

	return schema, nil
}

func buildIndexInfoFromDDL(t *Type, stmt *ast.CreateIndex) (IndexInfo, error) {
	keyColumns := make([]IndexKeyColumn, 0, len(stmt.Keys))
	for i, key := range stmt.Keys {
		keyColumns = append(keyColumns, IndexKeyColumn{
			ColumnName:      key.Name.Name,
			OrdinalPosition: int64(i + 1),
			Desc:            key.Dir == ast.DirectionDesc,
		})
	}

	storingColumns := make([]string, 0)
	if stmt.Storing != nil {
		storingColumns = make([]string, 0, len(stmt.Storing.Columns))
		for _, col := range stmt.Storing.Columns {
			storingColumns = append(storingColumns, col.Name)
		}
	}

	return buildIndexInfo(pathName(stmt.Name), stmt.Unique, keyColumns, storingColumns, t.Fields)
}

func buildIndexInfo(name string, isUnique bool, keyColumns []IndexKeyColumn, storingColumns []string, tableFields []Field) (IndexInfo, error) {
	fields, err := fieldsForIndexColumns(tableFields, keyColumns)
	if err != nil {
		return IndexInfo{}, fmt.Errorf("building index %s metadata: %w", name, err)
	}
	if len(keyColumns) == 0 {
		return IndexInfo{}, fmt.Errorf("building index %s metadata: missing key columns", name)
	}

	return IndexInfo{
		Name:           name,
		FuncName:       snakeToCamel(name),
		Fields:         fields,
		KeyColumns:     append([]IndexKeyColumn(nil), keyColumns...),
		StoringColumns: append([]string(nil), storingColumns...),
		IsUnique:       isUnique,
	}, nil
}

func fieldsForIndexColumns(tableFields []Field, keyColumns []IndexKeyColumn) ([]Field, error) {
	fieldByColumn := make(map[string]Field, len(tableFields))
	for _, field := range tableFields {
		fieldByColumn[field.ColumnName] = field
	}

	fields := make([]Field, 0, len(keyColumns))
	for _, keyColumn := range keyColumns {
		field, ok := fieldByColumn[keyColumn.ColumnName]
		if !ok {
			return nil, fmt.Errorf("column %q not found", keyColumn.ColumnName)
		}
		fields = append(fields, field)
	}

	return fields, nil
}

// pathName extracts the simple name from a memefish Path (last identifier).
func pathName(p *ast.Path) string {
	if p == nil || len(p.Idents) == 0 {
		return ""
	}
	return p.Idents[len(p.Idents)-1].Name
}

func columnHasTrueOption(options *ast.Options, name string) bool {
	if options == nil {
		return false
	}
	for _, record := range options.Records {
		if record.Name.Name != name {
			continue
		}
		lit, ok := record.Value.(*ast.BoolLiteral)
		return ok && lit.Value
	}
	return false
}

func createIndexDDLsByTable(ddls []ast.DDL) map[string][]*ast.CreateIndex {
	indexDDLsByTable := make(map[string][]*ast.CreateIndex)
	for _, ddl := range ddls {
		createIndex, ok := ddl.(*ast.CreateIndex)
		if !ok {
			continue
		}
		tableName := pathName(createIndex.TableName)
		indexDDLsByTable[tableName] = append(indexDDLsByTable[tableName], createIndex)
	}
	return indexDDLsByTable
}

func buildIndexInfos(indexDDLs []*ast.CreateIndex, tableFields []Field) []IndexInfo {
	indexInfos := make([]IndexInfo, 0, len(indexDDLs))
	for _, indexDDL := range indexDDLs {
		indexInfo := IndexInfo{
			Name:     pathName(indexDDL.Name),
			FuncName: snakeToCamel(pathName(indexDDL.Name)),
			IsUnique: indexDDL.Unique,
		}
		for _, key := range indexDDL.Keys {
			indexInfo.Fields = append(indexInfo.Fields, fieldForIndexColumn(tableFields, key.Name.Name))
		}
		indexInfos = append(indexInfos, indexInfo)
	}
	sort.Slice(indexInfos, func(i, j int) bool {
		return indexInfos[i].Name < indexInfos[j].Name
	})
	return indexInfos
}

func fieldForIndexColumn(tableFields []Field, columnName string) Field {
	for _, field := range tableFields {
		if field.ColumnName == columnName {
			return field
		}
	}
	return Field{
		Name:       snakeToCamel(columnName),
		ColumnName: columnName,
	}
}

// snakeToCamel converts a snake_case string to CamelCase.
func snakeToCamel(s string) string {
	return upperCamelIdentifier(s)
}
