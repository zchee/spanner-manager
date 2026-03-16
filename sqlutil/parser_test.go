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

package sqlutil

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestParseDDLs(t *testing.T) {
	tests := map[string]struct {
		sql       string
		wantCount int
		wantErr   bool
	}{
		"success: single CREATE TABLE": {
			sql:       "CREATE TABLE Users (UserId INT64 NOT NULL) PRIMARY KEY (UserId)",
			wantCount: 1,
		},
		"success: multiple DDL statements": {
			sql: `CREATE TABLE Users (UserId INT64 NOT NULL) PRIMARY KEY (UserId);
				  CREATE INDEX UsersByName ON Users(Name)`,
			wantCount: 2,
		},
		"error: invalid DDL": {
			sql:     "NOT VALID SQL",
			wantErr: true,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			ddls, err := ParseDDLs(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseDDLs() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && len(ddls) != tt.wantCount {
				t.Errorf("ParseDDLs() returned %d DDLs, want %d", len(ddls), tt.wantCount)
			}
		})
	}
}

func TestSplitStatements(t *testing.T) {
	tests := map[string]struct {
		sql      string
		expected []string
		wantErr  bool
	}{
		"success: single statement without semicolon": {
			sql:      "CREATE TABLE Users (UserId INT64 NOT NULL) PRIMARY KEY (UserId)",
			expected: []string{"CREATE TABLE Users (UserId INT64 NOT NULL) PRIMARY KEY (UserId)"},
		},
		"success: multiple statements with semicolons": {
			sql: "CREATE TABLE A (X INT64) PRIMARY KEY (X); CREATE TABLE B (Y INT64) PRIMARY KEY (Y)",
			expected: []string{
				"CREATE TABLE A (X INT64) PRIMARY KEY (X)",
				"CREATE TABLE B (Y INT64) PRIMARY KEY (Y)",
			},
		},
		"success: empty input": {
			sql:      "",
			expected: []string{},
		},
		"success: whitespace only": {
			sql:      "   \n\t  ",
			expected: []string{},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := SplitStatements(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Fatalf("SplitStatements() error = %v, wantErr %v", err, tt.wantErr)
			}
			if diff := cmp.Diff(tt.expected, got); diff != "" {
				t.Errorf("SplitStatements() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
