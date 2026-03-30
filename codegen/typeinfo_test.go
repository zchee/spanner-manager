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

	"github.com/google/go-cmp/cmp"
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
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if diff := cmp.Diff(tt.want, dedupeImportSpecs(tt.imports)); diff != "" {
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

	if diff := cmp.Diff([]Field{typ.Fields[1], typ.Fields[0]}, typ.PrimaryKeyFields); diff != "" {
		t.Fatalf("primary key fields mismatch (-want +got):\n%s", diff)
	}
}

func TestGoTypeForSpannerTypeString(t *testing.T) {
	tests := map[string]struct {
		spannerType string
		nullable    bool
		want        goTypeInfo
		wantErr     string
	}{
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
		"success: TIMESTAMP not null": {
			spannerType: "TIMESTAMP",
			want: goTypeInfo{
				Expr:            "time.Time",
				BaseSpannerType: "TIMESTAMP",
				Imports:         []ImportSpec{{Path: "time"}},
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
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Fatalf("goTypeForSpannerTypeString(%q, %v) mismatch (-want +got):\n%s", tt.spannerType, tt.nullable, diff)
			}
		})
	}
}
