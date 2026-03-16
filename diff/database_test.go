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
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestParseDatabase(t *testing.T) {
	tests := map[string]struct {
		ddl         []string
		wantTables  []string
		wantIndexes []string
		wantErr     bool
	}{
		"success: single table": {
			ddl: []string{
				"CREATE TABLE Users (UserId INT64 NOT NULL, Name STRING(MAX)) PRIMARY KEY (UserId)",
			},
			wantTables: []string{"Users"},
		},
		"success: table and index": {
			ddl: []string{
				"CREATE TABLE Users (UserId INT64 NOT NULL, Name STRING(MAX)) PRIMARY KEY (UserId)",
				"CREATE INDEX UsersByName ON Users(Name)",
			},
			wantTables:  []string{"Users"},
			wantIndexes: []string{"UsersByName"},
		},
		"success: interleaved table": {
			ddl: []string{
				"CREATE TABLE Users (UserId INT64 NOT NULL) PRIMARY KEY (UserId)",
				"CREATE TABLE Accounts (UserId INT64 NOT NULL, AccountId INT64 NOT NULL) PRIMARY KEY (UserId, AccountId), INTERLEAVE IN PARENT Users ON DELETE CASCADE",
			},
			wantTables: []string{"Users", "Accounts"},
		},
		"success: empty DDL list": {
			ddl:        []string{},
			wantTables: nil,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			db, err := ParseDatabase(tt.ddl)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseDatabase() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}

			var gotTables []string
			for n := range db.Tables {
				gotTables = append(gotTables, n)
			}
			// Sort for stable comparison is not needed since we compare sets.
			for _, want := range tt.wantTables {
				if _, ok := db.Tables[want]; !ok {
					t.Errorf("expected table %q not found, got tables: %v", want, gotTables)
				}
			}
			if len(db.Tables) != len(tt.wantTables) {
				t.Errorf("got %d tables, want %d", len(db.Tables), len(tt.wantTables))
			}

			var gotIndexes []string
			for n := range db.Indexes {
				gotIndexes = append(gotIndexes, n)
			}
			for _, want := range tt.wantIndexes {
				if _, ok := db.Indexes[want]; !ok {
					t.Errorf("expected index %q not found, got indexes: %v", want, gotIndexes)
				}
			}
		})
	}
}

func TestParseDatabase_TableDetails(t *testing.T) {
	ddl := []string{
		"CREATE TABLE Users (UserId INT64 NOT NULL, Name STRING(MAX), Email STRING(256) NOT NULL) PRIMARY KEY (UserId)",
	}

	db, err := ParseDatabase(ddl)
	if err != nil {
		t.Fatalf("ParseDatabase() error = %v", err)
	}

	table := db.Tables["Users"]
	if table == nil {
		t.Fatal("Users table not found")
	}

	if diff := cmp.Diff("Users", table.Name); diff != "" {
		t.Errorf("table name mismatch (-want +got):\n%s", diff)
	}

	if len(table.Columns) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(table.Columns))
	}

	// Check UserId column.
	col0 := table.Columns[0]
	if col0.Name != "UserId" || !col0.NotNull {
		t.Errorf("UserId column: name=%q notNull=%v, want name=UserId notNull=true", col0.Name, col0.NotNull)
	}

	// Check Name column (nullable).
	col1 := table.Columns[1]
	if col1.Name != "Name" || col1.NotNull {
		t.Errorf("Name column: name=%q notNull=%v, want name=Name notNull=false", col1.Name, col1.NotNull)
	}

	// Check primary key.
	if len(table.PrimaryKey) != 1 || table.PrimaryKey[0].Name != "UserId" {
		t.Errorf("primary key = %v, want [UserId]", table.PrimaryKey)
	}
}

func TestParseDatabase_InterleavedTable(t *testing.T) {
	ddl := []string{
		"CREATE TABLE Users (UserId INT64 NOT NULL) PRIMARY KEY (UserId)",
		"CREATE TABLE Accounts (UserId INT64 NOT NULL, AccountId INT64 NOT NULL) PRIMARY KEY (UserId, AccountId), INTERLEAVE IN PARENT Users ON DELETE CASCADE",
	}

	db, err := ParseDatabase(ddl)
	if err != nil {
		t.Fatalf("ParseDatabase() error = %v", err)
	}

	accounts := db.Tables["Accounts"]
	if accounts == nil {
		t.Fatal("Accounts table not found")
	}

	if accounts.ParentTable != "Users" {
		t.Errorf("ParentTable = %q, want %q", accounts.ParentTable, "Users")
	}
	if accounts.OnDelete != "CASCADE" {
		t.Errorf("OnDelete = %q, want %q", accounts.OnDelete, "CASCADE")
	}
}

func TestParseDatabase_IndexDetails(t *testing.T) {
	ddl := []string{
		"CREATE TABLE Users (UserId INT64 NOT NULL, Name STRING(MAX), Email STRING(256)) PRIMARY KEY (UserId)",
		"CREATE UNIQUE NULL_FILTERED INDEX UsersByEmail ON Users(Email) STORING (Name)",
	}

	db, err := ParseDatabase(ddl)
	if err != nil {
		t.Fatalf("ParseDatabase() error = %v", err)
	}

	idx := db.Indexes["UsersByEmail"]
	if idx == nil {
		t.Fatal("UsersByEmail index not found")
	}

	if !idx.Unique {
		t.Error("expected Unique = true")
	}
	if !idx.NullFiltered {
		t.Error("expected NullFiltered = true")
	}
	if idx.TableName != "Users" {
		t.Errorf("TableName = %q, want %q", idx.TableName, "Users")
	}
	if len(idx.Columns) != 1 || idx.Columns[0].Name != "Email" {
		t.Errorf("Columns = %v, want [Email]", idx.Columns)
	}
	if diff := cmp.Diff([]string{"Name"}, idx.Storing); diff != "" {
		t.Errorf("Storing mismatch (-want +got):\n%s", diff)
	}
}
