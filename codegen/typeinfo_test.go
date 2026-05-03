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
	"strings"
	"testing"

	"cloud.google.com/go/spanner"
	spanast "github.com/cloudspannerecosystem/memefish/ast"
	gocmp "github.com/google/go-cmp/cmp"
)

func TestDedupeImportSpecs(t *testing.T) {
	tests := map[string]struct {
		imports []ImportSpec
		want    []ImportSpec
	}{
		"success: prefer explicit alias over inferred path": {
			imports: []ImportSpec{
				{Path: "encoding/json"},
				{Alias: "json", Path: "encoding/json"},
			},
			want: []ImportSpec{
				{Alias: "json", Path: "encoding/json"},
			},
		},
		"success: keep explicit alias when duplicate path appears later": {
			imports: []ImportSpec{
				{Alias: "json", Path: "encoding/json"},
				{Path: "encoding/json"},
				{Path: "fmt"},
			},
			want: []ImportSpec{
				{Alias: "json", Path: "encoding/json"},
				{Path: "fmt"},
			},
		},
		"success: ignore empty import paths": {
			imports: []ImportSpec{
				{},
				{Path: "fmt"},
			},
			want: []ImportSpec{
				{Path: "fmt"},
			},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if diff := gocmp.Diff(tt.want, dedupeImportSpecs(tt.imports)); diff != "" {
				t.Fatalf("dedupeImportSpecs() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestRefreshTypeMetadata_PreservesPrimaryKeyOrdinal(t *testing.T) {
	typ := Type{
		Fields: []Field{
			{
				Name:            "A",
				ColumnName:      "a",
				GoType:          "int64",
				BaseSpannerType: "INT64",
				IsPrimaryKey:    true,
			},
			{
				Name:            "B",
				ColumnName:      "b",
				GoType:          "string",
				BaseSpannerType: "STRING",
				IsPrimaryKey:    true,
			},
			{
				Name:                 "UpdatedAt",
				ColumnName:           "updated_at",
				GoType:               "spanner.NullTime",
				BaseSpannerType:      "TIMESTAMP",
				AllowCommitTimestamp: true,
			},
		},
		PrimaryKeyFields: []Field{
			{ColumnName: "b"},
			{ColumnName: "a"},
		},
	}

	refreshTypeMetadata(&typ)

	if diff := gocmp.Diff([]Field{typ.Fields[1], typ.Fields[0]}, typ.PrimaryKeyFields); diff != "" {
		t.Fatalf("primary key fields mismatch (-want +got):\n%s", diff)
	}
}

func TestRefreshTypeMetadata_DropsStaleAndAppendsNewPrimaryKeys(t *testing.T) {
	typ := Type{
		Fields: []Field{
			{
				Name:         "A",
				ColumnName:   "a",
				IsPrimaryKey: true,
			},
			{
				Name:         "B",
				ColumnName:   "b",
				IsPrimaryKey: true,
			},
			{
				Name:         "C",
				ColumnName:   "c",
				IsPrimaryKey: true,
			},
		},
		PrimaryKeyFields: []Field{
			{ColumnName: "missing"},
			{ColumnName: "b"},
		},
	}

	refreshTypeMetadata(&typ)

	if diff := gocmp.Diff([]Field{typ.Fields[1], typ.Fields[0], typ.Fields[2]}, typ.PrimaryKeyFields); diff != "" {
		t.Fatalf("primary key fields mismatch (-want +got):\n%s", diff)
	}
}

func TestFieldIsWritable(t *testing.T) {
	tests := map[string]struct {
		field Field
		want  bool
	}{
		"plain column": {
			field: Field{Name: "Name"},
			want:  true,
		},
		"default column": {
			field: Field{Name: "CreatedAt", HasDefault: true},
			want:  true,
		},
		"generated column": {
			field: Field{Name: "ShardID", IsGenerated: true},
			want:  false,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if diff := gocmp.Diff(tt.want, tt.field.IsWritable()); diff != "" {
				t.Fatalf("IsWritable() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestRefreshTypeMetadata_PopulatesWritableFields(t *testing.T) {
	typ := Type{
		Fields: []Field{
			{
				Name:         "UserID",
				ColumnName:   "user_id",
				IsPrimaryKey: true,
			},
			{
				Name:       "DisplayName",
				ColumnName: "display_name",
			},
			{
				Name:       "Nickname",
				ColumnName: "nickname",
				HasDefault: true,
			},
			{
				Name:        "DisplayNameLower",
				ColumnName:  "display_name_lower",
				IsGenerated: true,
			},
		},
	}

	refreshTypeMetadata(&typ)

	if diff := gocmp.Diff([]Field{typ.Fields[0], typ.Fields[1], typ.Fields[2]}, typ.WritableFields); diff != "" {
		t.Fatalf("writable fields mismatch (-want +got):\n%s", diff)
	}
}

func TestFieldWriteSemanticsFromDefaultSemantics(t *testing.T) {
	tests := map[string]struct {
		semantics   spanast.ColumnDefaultSemantics
		wantDefault bool
		wantGen     bool
	}{
		"no semantics": {},
		"default expr": {
			semantics:   &spanast.ColumnDefaultExpr{},
			wantDefault: true,
		},
		"generated expr": {
			semantics: &spanast.GeneratedColumnExpr{},
			wantGen:   true,
		},
		"identity treated as default-managed": {
			semantics:   &spanast.IdentityColumn{},
			wantDefault: true,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			gotDefault, gotGen := fieldWriteSemanticsFromDefaultSemantics(tt.semantics)
			if diff := gocmp.Diff(tt.wantDefault, gotDefault); diff != "" {
				t.Fatalf("hasDefault mismatch (-want +got):\n%s", diff)
			}
			if diff := gocmp.Diff(tt.wantGen, gotGen); diff != "" {
				t.Fatalf("isGenerated mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestFieldWriteSemanticsFromInformationSchema(t *testing.T) {
	tests := map[string]struct {
		columnDefault       string
		columnDefaultValid  bool
		generationExpr      string
		generationExprValid bool
		wantDefault         bool
		wantGen             bool
	}{
		"plain column": {},
		"default column": {
			columnDefault:      "CURRENT_TIMESTAMP()",
			columnDefaultValid: true,
			wantDefault:        true,
		},
		"generated column wins": {
			columnDefault:       "CURRENT_TIMESTAMP()",
			columnDefaultValid:  true,
			generationExpr:      "LOWER(DisplayName)",
			generationExprValid: true,
			wantGen:             true,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			gotDefault, gotGen := fieldWriteSemanticsFromInformationSchema(
				spanner.NullString{StringVal: tt.columnDefault, Valid: tt.columnDefaultValid},
				spanner.NullString{StringVal: tt.generationExpr, Valid: tt.generationExprValid},
			)
			if diff := gocmp.Diff(tt.wantDefault, gotDefault); diff != "" {
				t.Fatalf("hasDefault mismatch (-want +got):\n%s", diff)
			}
			if diff := gocmp.Diff(tt.wantGen, gotGen); diff != "" {
				t.Fatalf("isGenerated mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestGoTypeForSpannerTypeString(t *testing.T) {
	tests := map[string]struct {
		spannerType string
		nullable    bool
		want        goTypeInfo
		wantErr     string
	}{
		"success: BOOL not null": {
			spannerType: "BOOL",
			want: goTypeInfo{
				Expr:            "bool",
				BaseSpannerType: "BOOL",
			},
		},
		"success: BOOL nullable": {
			spannerType: "BOOL",
			nullable:    true,
			want: goTypeInfo{
				Expr:            "spanner.NullBool",
				BaseSpannerType: "BOOL",
			},
		},
		"success: INT64 not null": {
			spannerType: "INT64",
			want: goTypeInfo{
				Expr:            "int64",
				BaseSpannerType: "INT64",
			},
		},
		"success: INT64 nullable": {
			spannerType: "INT64",
			nullable:    true,
			want: goTypeInfo{
				Expr:            "spanner.NullInt64",
				BaseSpannerType: "INT64",
			},
		},
		"success: FLOAT32 not null": {
			spannerType: "FLOAT32",
			want: goTypeInfo{
				Expr:            "float32",
				BaseSpannerType: "FLOAT32",
			},
		},
		"success: FLOAT32 nullable": {
			spannerType: "FLOAT32",
			nullable:    true,
			want: goTypeInfo{
				Expr:            "spanner.NullFloat32",
				BaseSpannerType: "FLOAT32",
			},
		},
		"success: FLOAT64 not null": {
			spannerType: "FLOAT64",
			want: goTypeInfo{
				Expr:            "float64",
				BaseSpannerType: "FLOAT64",
			},
		},
		"success: FLOAT64 nullable": {
			spannerType: "FLOAT64",
			nullable:    true,
			want: goTypeInfo{
				Expr:            "spanner.NullFloat64",
				BaseSpannerType: "FLOAT64",
			},
		},
		"success: STRING(MAX) not null": {
			spannerType: "STRING(MAX)",
			want: goTypeInfo{
				Expr:            "string",
				BaseSpannerType: "STRING",
			},
		},
		"success: STRING(MAX) nullable": {
			spannerType: "STRING(MAX)",
			nullable:    true,
			want: goTypeInfo{
				Expr:            "spanner.NullString",
				BaseSpannerType: "STRING",
			},
		},
		"success: BYTES(MAX) not null": {
			spannerType: "BYTES(MAX)",
			want: goTypeInfo{
				Expr:            "[]byte",
				BaseSpannerType: "BYTES",
			},
		},
		"success: BYTES(MAX) nullable": {
			spannerType: "BYTES(MAX)",
			nullable:    true,
			want: goTypeInfo{
				Expr:            "[]byte",
				BaseSpannerType: "BYTES",
			},
		},
		"success: DATE not null": {
			spannerType: "DATE",
			want: goTypeInfo{
				Expr:            "civil.Date",
				BaseSpannerType: "DATE",
				Imports:         []ImportSpec{{Path: "cloud.google.com/go/civil"}},
			},
		},
		"success: DATE nullable": {
			spannerType: "DATE",
			nullable:    true,
			want: goTypeInfo{
				Expr:            "spanner.NullDate",
				BaseSpannerType: "DATE",
			},
		},
		"success: TIMESTAMP not null": {
			spannerType: "TIMESTAMP",
			want: goTypeInfo{
				Expr:            "time.Time",
				BaseSpannerType: "TIMESTAMP",
				Imports:         []ImportSpec{{Path: "time"}},
			},
		},
		"success: TIMESTAMP nullable": {
			spannerType: "TIMESTAMP",
			nullable:    true,
			want: goTypeInfo{
				Expr:            "spanner.NullTime",
				BaseSpannerType: "TIMESTAMP",
			},
		},
		"success: TIMESTAMP array": {
			spannerType: "ARRAY<TIMESTAMP>",
			want: goTypeInfo{
				Expr:            "[]time.Time",
				BaseSpannerType: "TIMESTAMP",
				IsArray:         true,
				Imports:         []ImportSpec{{Path: "time"}},
			},
		},
		"success: NUMERIC not null": {
			spannerType: "NUMERIC",
			want: goTypeInfo{
				Expr:            "big.Rat",
				BaseSpannerType: "NUMERIC",
				Imports:         []ImportSpec{{Path: "math/big"}},
			},
		},
		"success: NUMERIC nullable": {
			spannerType: "NUMERIC",
			nullable:    true,
			want: goTypeInfo{
				Expr:            "spanner.NullNumeric",
				BaseSpannerType: "NUMERIC",
			},
		},
		"success: JSON not null": {
			spannerType: "JSON",
			want: goTypeInfo{
				Expr:            "spanner.NullJSON",
				BaseSpannerType: "JSON",
			},
		},
		"success: JSON nullable": {
			spannerType: "JSON",
			nullable:    true,
			want: goTypeInfo{
				Expr:            "spanner.NullJSON",
				BaseSpannerType: "JSON",
			},
		},
		"success: UUID not null": {
			spannerType: "UUID",
			want: goTypeInfo{
				Expr:            "uuid.UUID",
				BaseSpannerType: "UUID",
				Imports:         []ImportSpec{{Path: "github.com/google/uuid"}},
			},
		},
		"success: UUID nullable": {
			spannerType: "UUID",
			nullable:    true,
			want: goTypeInfo{
				Expr:            "spanner.NullUUID",
				BaseSpannerType: "UUID",
			},
		},
		"success: UUID array": {
			spannerType: "ARRAY<UUID>",
			want: goTypeInfo{
				Expr:            "[]uuid.UUID",
				BaseSpannerType: "UUID",
				IsArray:         true,
				Imports:         []ImportSpec{{Path: "github.com/google/uuid"}},
			},
		},
		"success: opaque type falls back to generic column value": {
			spannerType: "STRUCT<>",
			want: goTypeInfo{
				Expr:            "spanner.GenericColumnValue",
				BaseSpannerType: "STRUCT<>",
			},
		},
		"error: invalid type syntax": {
			spannerType: "ARRAY<",
			wantErr:     `parsing schema type "ARRAY<"`,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := goTypeForSpannerTypeString(tt.spannerType, tt.nullable)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("goTypeForSpannerTypeString(%q, %v) error = nil, want substring %q", tt.spannerType, tt.nullable, tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("goTypeForSpannerTypeString(%q, %v) error = %v, want substring %q", tt.spannerType, tt.nullable, err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("goTypeForSpannerTypeString(%q, %v) error = %v", tt.spannerType, tt.nullable, err)
			}
			if diff := gocmp.Diff(tt.want, got); diff != "" {
				t.Fatalf("goTypeForSpannerTypeString(%q, %v) mismatch (-want +got):\n%s", tt.spannerType, tt.nullable, diff)
			}
		})
	}
}

func TestGoTypeForSchemaType_UUIDCompat(t *testing.T) {
	tests := map[string]struct {
		schemaType spanast.SchemaType
		nullable   bool
		want       goTypeInfo
	}{
		"named type compatibility": {
			schemaType: &spanast.NamedType{Path: []*spanast.Ident{{Name: "UUID"}}},
			want: goTypeInfo{
				Expr:            "uuid.UUID",
				BaseSpannerType: "UUID",
				Imports:         []ImportSpec{{Path: "github.com/google/uuid"}},
			},
		},
		"scalar type compatibility": {
			schemaType: &spanast.ScalarSchemaType{Name: spanast.ScalarTypeName("UUID")},
			nullable:   true,
			want: goTypeInfo{
				Expr:            "spanner.NullUUID",
				BaseSpannerType: "UUID",
			},
		},
		"non uuid named type falls back to generic column value": {
			schemaType: &spanast.NamedType{Path: []*spanast.Ident{
				{Name: "example"},
				{Name: "Message"},
			}},
			want: goTypeInfo{
				Expr:            "spanner.GenericColumnValue",
				BaseSpannerType: "MESSAGE",
			},
		},
		"unknown nullable scalar falls back to generic column value": {
			schemaType: &spanast.ScalarSchemaType{Name: spanast.ScalarTypeName("PROTO")},
			nullable:   true,
			want: goTypeInfo{
				Expr:            "spanner.GenericColumnValue",
				BaseSpannerType: "PROTO",
			},
		},
		"unknown non nullable scalar falls back to generic column value": {
			schemaType: &spanast.ScalarSchemaType{Name: spanast.ScalarTypeName("PROTO")},
			want: goTypeInfo{
				Expr:            "spanner.GenericColumnValue",
				BaseSpannerType: "PROTO",
			},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if diff := gocmp.Diff(tt.want, goTypeForSchemaType(tt.schemaType, tt.nullable)); diff != "" {
				t.Fatalf("goTypeForSchemaType() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestImportSpecsFromConfig(t *testing.T) {
	tests := map[string]struct {
		paths   []string
		want    []ImportSpec
		wantErr string
	}{
		"success: trims blank paths and parses aliases": {
			paths: []string{
				" ",
				"json = encoding/json",
				"fmt",
			},
			want: []ImportSpec{
				{Path: "encoding/json", Alias: "json"},
				{Path: "fmt"},
			},
		},
		"error: alias with empty path": {
			paths:   []string{"json = "},
			wantErr: `invalid import spec "json ="`,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := importSpecsFromConfig(tt.paths)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("importSpecsFromConfig(%v) error = nil, want substring %q", tt.paths, tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("importSpecsFromConfig(%v) error = %q, want substring %q", tt.paths, err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("importSpecsFromConfig(%v) error = %v", tt.paths, err)
			}
			if diff := gocmp.Diff(tt.want, got); diff != "" {
				t.Fatalf("import specs mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestPathNameFromIdentPath(t *testing.T) {
	tests := map[string]struct {
		path []*spanast.Ident
		want string
	}{
		"empty path": {},
		"multi part path": {
			path: []*spanast.Ident{
				{Name: "example"},
				{Name: "Message"},
			},
			want: "Message",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if got := pathNameFromIdentPath(tt.path); got != tt.want {
				t.Fatalf("pathNameFromIdentPath() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestInferImportSpecsFromTypeExpr(t *testing.T) {
	tests := map[string]struct {
		expr string
		want []ImportSpec
	}{
		"known qualifiers are deduped and sorted": {
			expr: "json.RawMessage, time.Time, json.Decoder",
			want: []ImportSpec{
				{Path: "encoding/json"},
				{Path: "time"},
			},
		},
		"unknown qualifier is ignored": {
			expr: "example.Type",
			want: []ImportSpec{},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if diff := gocmp.Diff(tt.want, inferImportSpecsFromTypeExpr(tt.expr)); diff != "" {
				t.Fatalf("inferImportSpecsFromTypeExpr(%q) mismatch (-want +got):\n%s", tt.expr, diff)
			}
		})
	}
}

func TestCommitTimestampExpr(t *testing.T) {
	tests := map[string]struct {
		field Field
		want  string
		ok    bool
	}{
		"not commit timestamp": {
			field: Field{GoType: "time.Time"},
		},
		"commit timestamp time": {
			field: Field{
				GoType:               "time.Time",
				AllowCommitTimestamp: true,
			},
			want: "spanner.CommitTimestamp",
			ok:   true,
		},
		"commit timestamp nullable time": {
			field: Field{
				GoType:               "spanner.NullTime",
				AllowCommitTimestamp: true,
			},
			want: "spanner.NullTime{Time: spanner.CommitTimestamp, Valid: true}",
			ok:   true,
		},
		"commit timestamp unsupported go type": {
			field: Field{
				GoType:               "string",
				AllowCommitTimestamp: true,
			},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got, ok := commitTimestampExpr(tt.field)
			if got != tt.want || ok != tt.ok {
				t.Fatalf("commitTimestampExpr(%+v) = %q, %v; want %q, %v", tt.field, got, ok, tt.want, tt.ok)
			}
		})
	}
}
