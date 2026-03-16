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
	"strings"
	"testing"

	"github.com/zchee/spanner-manager/sqlutil"
)

func TestDiff_NoDifferences(t *testing.T) {
	ddl := []string{
		"CREATE TABLE Users (UserId INT64 NOT NULL, Name STRING(MAX)) PRIMARY KEY (UserId)",
	}

	from, err := ParseDatabase(ddl)
	if err != nil {
		t.Fatal(err)
	}
	to, err := ParseDatabase(ddl)
	if err != nil {
		t.Fatal(err)
	}

	stmts, err := Diff(from, to)
	if err != nil {
		t.Fatal(err)
	}

	if len(stmts) != 0 {
		t.Errorf("expected no differences, got %d statements", len(stmts))
		for _, s := range stmts {
			t.Logf("  %s: %s", s.Kind, s.SQL)
		}
	}
}

func TestDiff_AddTable(t *testing.T) {
	from, err := ParseDatabase([]string{
		"CREATE TABLE Users (UserId INT64 NOT NULL) PRIMARY KEY (UserId)",
	})
	if err != nil {
		t.Fatal(err)
	}

	to, err := ParseDatabase([]string{
		"CREATE TABLE Users (UserId INT64 NOT NULL) PRIMARY KEY (UserId)",
		"CREATE TABLE Accounts (AccountId INT64 NOT NULL) PRIMARY KEY (AccountId)",
	})
	if err != nil {
		t.Fatal(err)
	}

	stmts, err := Diff(from, to)
	if err != nil {
		t.Fatal(err)
	}

	if len(stmts) == 0 {
		t.Fatal("expected statements for new table")
	}

	found := false
	for _, s := range stmts {
		if strings.Contains(s.SQL, "CREATE TABLE Accounts") {
			found = true
			if s.Kind != sqlutil.KindDDL {
				t.Errorf("expected DDL kind, got %v", s.Kind)
			}
		}
	}
	if !found {
		t.Error("expected CREATE TABLE Accounts statement")
	}
}

func TestDiff_DropTable(t *testing.T) {
	from, err := ParseDatabase([]string{
		"CREATE TABLE Users (UserId INT64 NOT NULL) PRIMARY KEY (UserId)",
		"CREATE TABLE Accounts (AccountId INT64 NOT NULL) PRIMARY KEY (AccountId)",
	})
	if err != nil {
		t.Fatal(err)
	}

	to, err := ParseDatabase([]string{
		"CREATE TABLE Users (UserId INT64 NOT NULL) PRIMARY KEY (UserId)",
	})
	if err != nil {
		t.Fatal(err)
	}

	stmts, err := Diff(from, to)
	if err != nil {
		t.Fatal(err)
	}

	found := false
	for _, s := range stmts {
		if s.SQL == "DROP TABLE Accounts" {
			found = true
		}
	}
	if !found {
		t.Error("expected DROP TABLE Accounts statement")
		for _, s := range stmts {
			t.Logf("  %s: %s", s.Kind, s.SQL)
		}
	}
}

func TestDiff_AddColumn(t *testing.T) {
	from, err := ParseDatabase([]string{
		"CREATE TABLE Users (UserId INT64 NOT NULL) PRIMARY KEY (UserId)",
	})
	if err != nil {
		t.Fatal(err)
	}

	to, err := ParseDatabase([]string{
		"CREATE TABLE Users (UserId INT64 NOT NULL, Name STRING(MAX)) PRIMARY KEY (UserId)",
	})
	if err != nil {
		t.Fatal(err)
	}

	stmts, err := Diff(from, to)
	if err != nil {
		t.Fatal(err)
	}

	found := false
	for _, s := range stmts {
		if strings.Contains(s.SQL, "ADD COLUMN") && strings.Contains(s.SQL, "Name") {
			found = true
		}
	}
	if !found {
		t.Error("expected ALTER TABLE ... ADD COLUMN Name statement")
		for _, s := range stmts {
			t.Logf("  %s: %s", s.Kind, s.SQL)
		}
	}
}

func TestDiff_DropColumn(t *testing.T) {
	from, err := ParseDatabase([]string{
		"CREATE TABLE Users (UserId INT64 NOT NULL, Name STRING(MAX)) PRIMARY KEY (UserId)",
	})
	if err != nil {
		t.Fatal(err)
	}

	to, err := ParseDatabase([]string{
		"CREATE TABLE Users (UserId INT64 NOT NULL) PRIMARY KEY (UserId)",
	})
	if err != nil {
		t.Fatal(err)
	}

	stmts, err := Diff(from, to)
	if err != nil {
		t.Fatal(err)
	}

	found := false
	for _, s := range stmts {
		if strings.Contains(s.SQL, "DROP COLUMN") && strings.Contains(s.SQL, "Name") {
			found = true
		}
	}
	if !found {
		t.Error("expected ALTER TABLE ... DROP COLUMN Name statement")
		for _, s := range stmts {
			t.Logf("  %s: %s", s.Kind, s.SQL)
		}
	}
}

func TestDiff_AddIndex(t *testing.T) {
	from, err := ParseDatabase([]string{
		"CREATE TABLE Users (UserId INT64 NOT NULL, Name STRING(MAX)) PRIMARY KEY (UserId)",
	})
	if err != nil {
		t.Fatal(err)
	}

	to, err := ParseDatabase([]string{
		"CREATE TABLE Users (UserId INT64 NOT NULL, Name STRING(MAX)) PRIMARY KEY (UserId)",
		"CREATE INDEX UsersByName ON Users(Name)",
	})
	if err != nil {
		t.Fatal(err)
	}

	stmts, err := Diff(from, to)
	if err != nil {
		t.Fatal(err)
	}

	found := false
	for _, s := range stmts {
		if strings.Contains(s.SQL, "CREATE INDEX UsersByName") {
			found = true
		}
	}
	if !found {
		t.Error("expected CREATE INDEX UsersByName statement")
		for _, s := range stmts {
			t.Logf("  %s: %s", s.Kind, s.SQL)
		}
	}
}

func TestDiff_DropIndex(t *testing.T) {
	from, err := ParseDatabase([]string{
		"CREATE TABLE Users (UserId INT64 NOT NULL, Name STRING(MAX)) PRIMARY KEY (UserId)",
		"CREATE INDEX UsersByName ON Users(Name)",
	})
	if err != nil {
		t.Fatal(err)
	}

	to, err := ParseDatabase([]string{
		"CREATE TABLE Users (UserId INT64 NOT NULL, Name STRING(MAX)) PRIMARY KEY (UserId)",
	})
	if err != nil {
		t.Fatal(err)
	}

	stmts, err := Diff(from, to)
	if err != nil {
		t.Fatal(err)
	}

	found := false
	for _, s := range stmts {
		if s.SQL == "DROP INDEX UsersByName" {
			found = true
		}
	}
	if !found {
		t.Error("expected DROP INDEX UsersByName statement")
		for _, s := range stmts {
			t.Logf("  %s: %s", s.Kind, s.SQL)
		}
	}
}

func TestDiff_EmptyToEmpty(t *testing.T) {
	from, err := ParseDatabase([]string{})
	if err != nil {
		t.Fatal(err)
	}
	to, err := ParseDatabase([]string{})
	if err != nil {
		t.Fatal(err)
	}

	stmts, err := Diff(from, to)
	if err != nil {
		t.Fatal(err)
	}

	if len(stmts) != 0 {
		t.Errorf("expected no statements, got %d", len(stmts))
	}
}
