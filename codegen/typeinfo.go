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
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/cloudspannerecosystem/memefish/ast"

	"github.com/zchee/spanner-manager/sqlutil"
)

type goTypeInfo struct {
	Expr            string
	BaseSpannerType string
	IsArray         bool
	Imports         []ImportSpec
}

var typeQualifierPattern = regexp.MustCompile(`\b([A-Za-z_][A-Za-z0-9_]*)\.`)

var knownImportAliases = map[string]string{
	"big":     "math/big",
	"civil":   "cloud.google.com/go/civil",
	"json":    "encoding/json",
	"spanner": "cloud.google.com/go/spanner",
	"time":    "time",
}

func goTypeForSpannerTypeString(spannerType string, nullable bool) (goTypeInfo, error) {
	schemaType, err := parseSchemaTypeString(spannerType)
	if err != nil {
		return goTypeInfo{}, err
	}
	return goTypeForSchemaType(schemaType, nullable), nil
}

func parseSchemaTypeString(spannerType string) (ast.SchemaType, error) {
	ddl := fmt.Sprintf("CREATE TABLE T (C %s) PRIMARY KEY (C)", spannerType)

	ddls, err := sqlutil.ParseDDLs(ddl)
	if err != nil {
		return nil, fmt.Errorf("parsing schema type %q: %w", spannerType, err)
	}
	if len(ddls) != 1 {
		return nil, fmt.Errorf("parsing schema type %q: unexpected DDL count %d", spannerType, len(ddls))
	}

	createTable, ok := ddls[0].(*ast.CreateTable)
	if !ok || len(createTable.Columns) != 1 {
		return nil, fmt.Errorf("parsing schema type %q: unexpected AST shape", spannerType)
	}

	return createTable.Columns[0].Type, nil
}

func goTypeForSchemaType(schemaType ast.SchemaType, nullable bool) goTypeInfo {
	switch t := schemaType.(type) {
	case *ast.ArraySchemaType:
		item := goTypeForSchemaType(t.Item, false)
		return goTypeInfo{
			Expr:            "[]" + item.Expr,
			BaseSpannerType: item.BaseSpannerType,
			IsArray:         true,
			Imports:         item.Imports,
		}
	case *ast.ScalarSchemaType:
		return scalarGoType(t.Name, nullable)
	case *ast.SizedSchemaType:
		return scalarGoType(t.Name, nullable)
	case *ast.NamedType:
		return goTypeInfo{
			Expr:            "spanner.GenericColumnValue",
			BaseSpannerType: strings.ToUpper(pathNameFromIdentPath(t.Path)),
		}
	default:
		return goTypeInfo{
			Expr:            "spanner.GenericColumnValue",
			BaseSpannerType: strings.ToUpper(schemaType.SQL()),
		}
	}
}

func scalarGoType(name ast.ScalarTypeName, nullable bool) goTypeInfo {
	base := strings.ToUpper(string(name))

	if nullable {
		switch name {
		case ast.BoolTypeName:
			return goTypeInfo{Expr: "spanner.NullBool", BaseSpannerType: base}
		case ast.Int64TypeName:
			return goTypeInfo{Expr: "spanner.NullInt64", BaseSpannerType: base}
		case ast.Float32TypeName:
			return goTypeInfo{Expr: "spanner.NullFloat32", BaseSpannerType: base}
		case ast.Float64TypeName:
			return goTypeInfo{Expr: "spanner.NullFloat64", BaseSpannerType: base}
		case ast.StringTypeName:
			return goTypeInfo{Expr: "spanner.NullString", BaseSpannerType: base}
		case ast.BytesTypeName:
			return goTypeInfo{Expr: "[]byte", BaseSpannerType: base}
		case ast.DateTypeName:
			return goTypeInfo{Expr: "spanner.NullDate", BaseSpannerType: base}
		case ast.TimestampTypeName:
			return goTypeInfo{Expr: "spanner.NullTime", BaseSpannerType: base}
		case ast.NumericTypeName:
			return goTypeInfo{Expr: "spanner.NullNumeric", BaseSpannerType: base}
		case ast.JSONTypeName:
			return goTypeInfo{Expr: "spanner.NullJSON", BaseSpannerType: base}
		default:
			return goTypeInfo{Expr: "spanner.GenericColumnValue", BaseSpannerType: base}
		}
	}

	switch name {
	case ast.BoolTypeName:
		return goTypeInfo{Expr: "bool", BaseSpannerType: base}
	case ast.Int64TypeName:
		return goTypeInfo{Expr: "int64", BaseSpannerType: base}
	case ast.Float32TypeName:
		return goTypeInfo{Expr: "float32", BaseSpannerType: base}
	case ast.Float64TypeName:
		return goTypeInfo{Expr: "float64", BaseSpannerType: base}
	case ast.StringTypeName:
		return goTypeInfo{Expr: "string", BaseSpannerType: base}
	case ast.BytesTypeName:
		return goTypeInfo{Expr: "[]byte", BaseSpannerType: base}
	case ast.DateTypeName:
		return goTypeInfo{
			Expr:            "civil.Date",
			BaseSpannerType: base,
			Imports:         []ImportSpec{{Path: "cloud.google.com/go/civil"}},
		}
	case ast.TimestampTypeName:
		return goTypeInfo{
			Expr:            "time.Time",
			BaseSpannerType: base,
			Imports:         []ImportSpec{{Path: "time"}},
		}
	case ast.NumericTypeName:
		return goTypeInfo{
			Expr:            "big.Rat",
			BaseSpannerType: base,
			Imports:         []ImportSpec{{Path: "math/big"}},
		}
	case ast.JSONTypeName:
		return goTypeInfo{Expr: "spanner.NullJSON", BaseSpannerType: base}
	default:
		return goTypeInfo{Expr: "spanner.GenericColumnValue", BaseSpannerType: base}
	}
}

func refreshTypeMetadata(t *Type) {
	t.PrimaryKeyFields = refreshOrderedFields(t.Fields, t.PrimaryKeyFields, func(field Field) bool {
		return field.IsPrimaryKey
	})
	t.CommitTSFields = t.CommitTSFields[:0]

	imports := []ImportSpec{
		{Path: "context"},
		{Path: "fmt"},
		{Path: "cloud.google.com/go/spanner"},
	}
	for i := range t.Fields {
		field := t.Fields[i]
		imports = append(imports, field.Imports...)
		if _, ok := commitTimestampExpr(field); ok {
			t.CommitTSFields = append(t.CommitTSFields, field)
		}
	}

	t.Imports = dedupeImportSpecs(imports)
	if t.FileNameBase == "" {
		t.FileNameBase = strings.ToLower(t.Name)
	}
}

func refreshOrderedFields(fields, ordered []Field, include func(Field) bool) []Field {
	fieldByColumn := make(map[string]Field, len(fields))
	included := make([]Field, 0, len(fields))
	for i := range fields {
		field := fields[i]
		if !include(field) {
			continue
		}
		fieldByColumn[field.ColumnName] = field
		included = append(included, field)
	}
	if len(ordered) == 0 {
		return included
	}

	out := make([]Field, 0, len(included))
	seen := make(map[string]struct{}, len(included))
	for _, field := range ordered {
		refreshed, ok := fieldByColumn[field.ColumnName]
		if !ok {
			continue
		}
		out = append(out, refreshed)
		seen[field.ColumnName] = struct{}{}
	}
	for _, field := range included {
		if _, ok := seen[field.ColumnName]; ok {
			continue
		}
		out = append(out, field)
	}
	return out
}

func dedupeImportSpecs(imports []ImportSpec) []ImportSpec {
	seen := make(map[string]ImportSpec)
	for _, spec := range imports {
		if spec.Path == "" {
			continue
		}
		existing, ok := seen[spec.Path]
		// Prefer an explicit alias over an inferred bare import for the same path.
		if !ok || (existing.Alias == "" && spec.Alias != "") {
			seen[spec.Path] = spec
		}
	}

	out := make([]ImportSpec, 0, len(seen))
	for _, spec := range seen {
		out = append(out, spec)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].Path < out[j].Path
	})
	return out
}

func pathNameFromIdentPath(path []*ast.Ident) string {
	if len(path) == 0 {
		return ""
	}
	return path[len(path)-1].Name
}

func inferImportSpecsFromTypeExpr(expr string) []ImportSpec {
	matches := typeQualifierPattern.FindAllStringSubmatch(expr, -1)
	specs := make([]ImportSpec, 0, len(matches))
	for _, match := range matches {
		if len(match) < 2 {
			continue
		}
		if path, ok := knownImportAliases[match[1]]; ok {
			specs = append(specs, ImportSpec{Path: path})
		}
	}
	return dedupeImportSpecs(specs)
}

func importSpecsFromConfig(paths []string) ([]ImportSpec, error) {
	specs := make([]ImportSpec, 0, len(paths))
	for _, raw := range paths {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}

		spec := ImportSpec{Path: raw}
		if alias, path, ok := strings.Cut(raw, "="); ok {
			spec.Alias = strings.TrimSpace(alias)
			spec.Path = strings.TrimSpace(path)
		}
		if spec.Path == "" {
			return nil, fmt.Errorf("invalid import spec %q", raw)
		}
		specs = append(specs, spec)
	}
	return dedupeImportSpecs(specs), nil
}

func commitTimestampExpr(field Field) (string, bool) {
	if !field.AllowCommitTimestamp {
		return "", false
	}

	switch field.GoType {
	case "time.Time":
		return "spanner.CommitTimestamp", true
	case "spanner.NullTime":
		return "spanner.NullTime{Time: spanner.CommitTimestamp, Valid: true}", true
	default:
		return "", false
	}
}
