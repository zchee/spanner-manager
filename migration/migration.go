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

// Package migration implements versioned schema migration management for Cloud Spanner.
package migration

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"

	"github.com/zchee/spanner-manager/sqlutil"
)

// Migration represents a single versioned migration file.
type Migration struct {
	// Version is the migration version number.
	Version uint

	// Name is the human-readable migration name.
	Name string

	// Statements are the SQL statements to execute.
	Statements []string

	// Kind is the type of statements (DDL, DML, or PartitionedDML).
	Kind sqlutil.StatementKind
}

// migrationFilePattern matches migration filenames like "000001_add_users_table.sql".
var migrationFilePattern = regexp.MustCompile(`^([0-9]+)(?:_([a-zA-Z0-9_\-]+))?(\.up)?\.sql$`)

// ReadMigrations reads and parses all migration files from the migrations/ subdirectory.
// Files must match the pattern: {VERSION}_{NAME}.sql or {VERSION}_{NAME}.up.sql.
func ReadMigrations(dir string) ([]Migration, error) {
	migrationsDir := filepath.Join(dir, "migrations")

	entries, err := os.ReadDir(migrationsDir)
	if err != nil {
		return nil, fmt.Errorf("reading migrations directory: %w", err)
	}

	var migrations []Migration
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		matches := migrationFilePattern.FindStringSubmatch(entry.Name())
		if matches == nil {
			continue
		}

		version, err := strconv.ParseUint(matches[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parsing version from %s: %w", entry.Name(), err)
		}

		name := matches[2]

		data, err := os.ReadFile(filepath.Join(migrationsDir, entry.Name()))
		if err != nil {
			return nil, fmt.Errorf("reading migration file %s: %w", entry.Name(), err)
		}

		stmts, err := sqlutil.SplitStatements(string(data))
		if err != nil {
			return nil, fmt.Errorf("parsing migration file %s: %w", entry.Name(), err)
		}

		if len(stmts) == 0 {
			continue
		}

		// Classify statements — all must be the same kind.
		kind, err := sqlutil.ClassifyStatement(stmts[0])
		if err != nil {
			return nil, fmt.Errorf("classifying statement in %s: %w", entry.Name(), err)
		}

		for i := 1; i < len(stmts); i++ {
			k, err := sqlutil.ClassifyStatement(stmts[i])
			if err != nil {
				return nil, fmt.Errorf("classifying statement %d in %s: %w", i+1, entry.Name(), err)
			}
			if k != kind {
				return nil, fmt.Errorf("mixed statement kinds in %s: statement 1 is %s but statement %d is %s", entry.Name(), kind, i+1, k)
			}
		}

		migrations = append(migrations, Migration{
			Version:    uint(version),
			Name:       name,
			Statements: stmts,
			Kind:       kind,
		})
	}

	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].Version < migrations[j].Version
	})

	return migrations, nil
}
