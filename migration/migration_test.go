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

package migration

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/zchee/spanner-manager/sqlutil"
)

func TestReadMigrations(t *testing.T) {
	tests := map[string]struct {
		files    map[string]string // filename → content
		expected []Migration
		wantErr  bool
	}{
		"success: single DDL migration": {
			files: map[string]string{
				"000001_create_users.sql": "CREATE TABLE Users (UserId INT64 NOT NULL) PRIMARY KEY (UserId)",
			},
			expected: []Migration{
				{
					Version:    1,
					Name:       "create_users",
					Statements: []string{"CREATE TABLE Users (UserId INT64 NOT NULL) PRIMARY KEY (UserId)"},
					Kind:       sqlutil.KindDDL,
				},
			},
		},
		"success: multiple migrations sorted by version": {
			files: map[string]string{
				"000002_add_index.sql":    "CREATE INDEX UsersByName ON Users(Name)",
				"000001_create_users.sql": "CREATE TABLE Users (UserId INT64 NOT NULL, Name STRING(MAX)) PRIMARY KEY (UserId)",
			},
			expected: []Migration{
				{
					Version:    1,
					Name:       "create_users",
					Statements: []string{"CREATE TABLE Users (UserId INT64 NOT NULL, Name STRING(MAX)) PRIMARY KEY (UserId)"},
					Kind:       sqlutil.KindDDL,
				},
				{
					Version:    2,
					Name:       "add_index",
					Statements: []string{"CREATE INDEX UsersByName ON Users(Name)"},
					Kind:       sqlutil.KindDDL,
				},
			},
		},
		"success: DML migration": {
			files: map[string]string{
				"000001_seed_data.sql": "INSERT INTO Users (UserId, Name) VALUES (1, 'Alice')",
			},
			expected: []Migration{
				{
					Version:    1,
					Name:       "seed_data",
					Statements: []string{"INSERT INTO Users (UserId, Name) VALUES (1, 'Alice')"},
					Kind:       sqlutil.KindDML,
				},
			},
		},
		"success: .up.sql extension": {
			files: map[string]string{
				"000001_create_users.up.sql": "CREATE TABLE Users (UserId INT64 NOT NULL) PRIMARY KEY (UserId)",
			},
			expected: []Migration{
				{
					Version:    1,
					Name:       "create_users",
					Statements: []string{"CREATE TABLE Users (UserId INT64 NOT NULL) PRIMARY KEY (UserId)"},
					Kind:       sqlutil.KindDDL,
				},
			},
		},
		"success: empty file is skipped": {
			files: map[string]string{
				"000001_empty.sql": "",
			},
			expected: nil,
		},
		"success: non-matching files are ignored": {
			files: map[string]string{
				"readme.txt":              "ignore me",
				"000001_create_users.sql": "CREATE TABLE Users (UserId INT64 NOT NULL) PRIMARY KEY (UserId)",
			},
			expected: []Migration{
				{
					Version:    1,
					Name:       "create_users",
					Statements: []string{"CREATE TABLE Users (UserId INT64 NOT NULL) PRIMARY KEY (UserId)"},
					Kind:       sqlutil.KindDDL,
				},
			},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			migrationsDir := filepath.Join(dir, "migrations")
			if err := os.MkdirAll(migrationsDir, 0o755); err != nil {
				t.Fatal(err)
			}

			for filename, content := range tt.files {
				if err := os.WriteFile(filepath.Join(migrationsDir, filename), []byte(content), 0o644); err != nil {
					t.Fatal(err)
				}
			}

			got, err := ReadMigrations(dir)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ReadMigrations() error = %v, wantErr %v", err, tt.wantErr)
			}

			if diff := cmp.Diff(tt.expected, got, cmp.AllowUnexported(Migration{})); diff != "" {
				t.Errorf("ReadMigrations() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestReadMigrations_MissingDirectory(t *testing.T) {
	_, err := ReadMigrations(t.TempDir())
	if err == nil {
		t.Error("expected error for missing migrations directory")
	}
}
