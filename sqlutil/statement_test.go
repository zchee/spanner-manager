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
)

func TestClassifyStatement(t *testing.T) {
	tests := map[string]struct {
		sql      string
		expected StatementKind
		wantErr  bool
	}{
		"success: CREATE TABLE is DDL": {
			sql:      "CREATE TABLE Users (UserId INT64 NOT NULL) PRIMARY KEY (UserId)",
			expected: KindDDL,
		},
		"success: ALTER TABLE is DDL": {
			sql:      "ALTER TABLE Users ADD COLUMN Name STRING(MAX)",
			expected: KindDDL,
		},
		"success: DROP TABLE is DDL": {
			sql:      "DROP TABLE Users",
			expected: KindDDL,
		},
		"success: CREATE INDEX is DDL": {
			sql:      "CREATE INDEX UsersByName ON Users(Name)",
			expected: KindDDL,
		},
		"success: INSERT is DML": {
			sql:      "INSERT INTO Users (UserId, Name) VALUES (1, 'Alice')",
			expected: KindDML,
		},
		"success: UPDATE is DML": {
			sql:      "UPDATE Users SET Name = 'Bob' WHERE UserId = 1",
			expected: KindDML,
		},
		"success: DELETE is DML": {
			sql:      "DELETE FROM Users WHERE UserId = 1",
			expected: KindDML,
		},
		"success: partitioned DML comment prefix": {
			sql:      "-- PARTITIONED_DML\nUPDATE Users SET Name = 'default' WHERE Name IS NULL",
			expected: KindPartitionedDML,
		},
		"error: invalid SQL": {
			sql:     "NOT VALID SQL AT ALL",
			wantErr: true,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := ClassifyStatement(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ClassifyStatement() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && got != tt.expected {
				t.Errorf("ClassifyStatement() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestIsDDL(t *testing.T) {
	tests := map[string]struct {
		sql      string
		expected bool
	}{
		"success: CREATE TABLE": {
			sql:      "CREATE TABLE T (X INT64) PRIMARY KEY (X)",
			expected: true,
		},
		"success: INSERT is not DDL": {
			sql:      "INSERT INTO T (X) VALUES (1)",
			expected: false,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if got := IsDDL(tt.sql); got != tt.expected {
				t.Errorf("IsDDL(%q) = %v, want %v", tt.sql, got, tt.expected)
			}
		})
	}
}

func TestIsDML(t *testing.T) {
	tests := map[string]struct {
		sql      string
		expected bool
	}{
		"success: INSERT is DML": {
			sql:      "INSERT INTO T (X) VALUES (1)",
			expected: true,
		},
		"success: CREATE TABLE is not DML": {
			sql:      "CREATE TABLE T (X INT64) PRIMARY KEY (X)",
			expected: false,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if got := IsDML(tt.sql); got != tt.expected {
				t.Errorf("IsDML(%q) = %v, want %v", tt.sql, got, tt.expected)
			}
		})
	}
}

func TestStatementKind_String(t *testing.T) {
	tests := map[string]struct {
		kind     StatementKind
		expected string
	}{
		"DDL":            {kind: KindDDL, expected: "DDL"},
		"DML":            {kind: KindDML, expected: "DML"},
		"PartitionedDML": {kind: KindPartitionedDML, expected: "PartitionedDML"},
		"unknown kind":   {kind: StatementKind(99), expected: "StatementKind(99)"},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if got := tt.kind.String(); got != tt.expected {
				t.Errorf("String() = %q, want %q", got, tt.expected)
			}
		})
	}
}
