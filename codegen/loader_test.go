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
	"os"
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestDDLFileSource_Load(t *testing.T) {
	ddl := `CREATE TABLE Users (
		UserId INT64 NOT NULL,
		Name STRING(MAX),
		Email STRING(256) NOT NULL,
		Age INT64,
		CreatedAt TIMESTAMP NOT NULL,
	) PRIMARY KEY (UserId)`

	dir := t.TempDir()
	path := filepath.Join(dir, "schema.sql")
	if err := os.WriteFile(path, []byte(ddl), 0o644); err != nil {
		t.Fatal(err)
	}

	source := NewDDLFileSource(path)
	schema, err := source.Load(t.Context())
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if len(schema.Types) != 1 {
		t.Fatalf("expected 1 type, got %d", len(schema.Types))
	}

	typ := schema.Types[0]
	if typ.Name != "Users" {
		t.Errorf("type name = %q, want %q", typ.Name, "Users")
	}
	if typ.Table != "Users" {
		t.Errorf("table name = %q, want %q", typ.Table, "Users")
	}

	// Check field count.
	if len(typ.Fields) != 5 {
		t.Fatalf("expected 5 fields, got %d", len(typ.Fields))
	}

	// Check some field mappings.
	tests := map[string]struct {
		fieldIdx int
		name     string
		goType   string
		baseType string
		notNull  bool
	}{
		"UserId":    {fieldIdx: 0, name: "UserId", goType: "int64", baseType: "INT64", notNull: true},
		"Name":      {fieldIdx: 1, name: "Name", goType: "spanner.NullString", baseType: "STRING", notNull: false},
		"Email":     {fieldIdx: 2, name: "Email", goType: "string", baseType: "STRING", notNull: true},
		"Age":       {fieldIdx: 3, name: "Age", goType: "spanner.NullInt64", baseType: "INT64", notNull: false},
		"CreatedAt": {fieldIdx: 4, name: "CreatedAt", goType: "time.Time", baseType: "TIMESTAMP", notNull: true},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			f := typ.Fields[tt.fieldIdx]
			if f.Name != tt.name {
				t.Errorf("field name = %q, want %q", f.Name, tt.name)
			}
			if f.GoType != tt.goType {
				t.Errorf("field %s GoType = %q, want %q", tt.name, f.GoType, tt.goType)
			}
			if f.BaseSpannerType != tt.baseType {
				t.Errorf("field %s BaseSpannerType = %q, want %q", tt.name, f.BaseSpannerType, tt.baseType)
			}
			if f.NotNull != tt.notNull {
				t.Errorf("field %s NotNull = %v, want %v", tt.name, f.NotNull, tt.notNull)
			}
		})
	}

	// Check primary key.
	if len(typ.PrimaryKeyFields) != 1 {
		t.Fatalf("expected 1 primary key field, got %d", len(typ.PrimaryKeyFields))
	}
	if typ.PrimaryKeyFields[0].Name != "UserId" {
		t.Errorf("primary key field = %q, want %q", typ.PrimaryKeyFields[0].Name, "UserId")
	}
}

func TestDDLFileSource_Load_ArrayAndCommitTimestamp(t *testing.T) {
	ddl := `CREATE TABLE Runs (
		RunId INT64 NOT NULL,
		RepositoryIds ARRAY<STRING(MAX)>,
		UpdatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)
	) PRIMARY KEY (RunId)`

	dir := t.TempDir()
	path := filepath.Join(dir, "schema.sql")
	if err := os.WriteFile(path, []byte(ddl), 0o644); err != nil {
		t.Fatal(err)
	}

	source := NewDDLFileSource(path)
	schema, err := source.Load(t.Context())
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if len(schema.Types) != 1 {
		t.Fatalf("expected 1 type, got %d", len(schema.Types))
	}

	got := schema.Types[0]
	if diff := cmp.Diff([]Field{
		{
			Name:            "RunId",
			ColumnName:      "RunId",
			GoType:          "int64",
			SpannerType:     "INT64",
			BaseSpannerType: "INT64",
			NotNull:         true,
			IsPrimaryKey:    true,
		},
		{
			Name:            "RepositoryIds",
			ColumnName:      "RepositoryIds",
			GoType:          "[]string",
			SpannerType:     "ARRAY<STRING(MAX)>",
			BaseSpannerType: "STRING",
			IsArray:         true,
		},
		{
			Name:                 "UpdatedAt",
			ColumnName:           "UpdatedAt",
			GoType:               "time.Time",
			SpannerType:          "TIMESTAMP",
			BaseSpannerType:      "TIMESTAMP",
			NotNull:              true,
			AllowCommitTimestamp: true,
			Imports:              []ImportSpec{{Path: "time"}},
		},
	}, got.Fields); diff != "" {
		t.Fatalf("fields mismatch (-want +got):\n%s", diff)
	}

	if diff := cmp.Diff([]Field{got.Fields[0]}, got.PrimaryKeyFields); diff != "" {
		t.Fatalf("primary key fields mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]Field{got.Fields[2]}, got.CommitTSFields); diff != "" {
		t.Fatalf("commit timestamp fields mismatch (-want +got):\n%s", diff)
	}
}

func TestSnakeToCamel(t *testing.T) {
	tests := map[string]struct {
		input    string
		expected string
	}{
		"simple":        {input: "user_id", expected: "UserId"},
		"single word":   {input: "name", expected: "Name"},
		"already camel": {input: "UserID", expected: "UserID"},
		"empty":         {input: "", expected: ""},
		"underscores":   {input: "created_at_time", expected: "CreatedAtTime"},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got := snakeToCamel(tt.input)
			if diff := cmp.Diff(tt.expected, got); diff != "" {
				t.Errorf("snakeToCamel(%q) mismatch (-want +got):\n%s", tt.input, diff)
			}
		})
	}
}
