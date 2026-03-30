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
