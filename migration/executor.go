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
	"context"
	"fmt"

	"cloud.google.com/go/spanner"
	"google.golang.org/grpc/codes"

	"github.com/zchee/spanner-manager/spannerutil"
	"github.com/zchee/spanner-manager/sqlutil"
)

// Executor handles migration execution and version tracking.
type Executor struct {
	client  *spannerutil.Client
	backend migrationBackend
	runner  migrationRunner
}

// NewExecutor creates a new migration executor.
func NewExecutor(client *spannerutil.Client) *Executor {
	e := &Executor{
		client:  client,
		backend: spannerMigrationBackend{client: client},
	}
	e.runner = e.defaultRunner()
	return e
}

// EnsureTable creates the SchemaMigrations table if it does not exist.
func (e *Executor) EnsureTable(ctx context.Context) error {
	// Try to query the table; if it fails, create it.
	if err := e.backend.query(ctx, spanner.Statement{
		SQL: "SELECT Version FROM SchemaMigrations LIMIT 1",
	}, func(_ *spanner.Row) error { return nil }); err != nil {
		if spanner.ErrCode(err) == codes.NotFound || isTableNotFoundError(err) {
			return e.backend.updateDatabaseDDL(ctx, []string{SchemaMigrationsTableDDL})
		}
		return fmt.Errorf("checking SchemaMigrations table: %w", err)
	}
	return nil
}

// GetVersion returns the current migration version and dirty flag.
func (e *Executor) GetVersion(ctx context.Context) (version uint, dirty bool, err error) {
	stmt := spanner.Statement{
		SQL: "SELECT Version, Dirty FROM SchemaMigrations ORDER BY Version DESC LIMIT 1",
	}

	var found bool
	if err := e.backend.query(ctx, stmt, func(row *spanner.Row) error {
		var v int64
		var d bool
		if err := row.Columns(&v, &d); err != nil {
			return err
		}
		version = uint(v)
		dirty = d
		found = true
		return nil
	}); err != nil {
		return 0, false, fmt.Errorf("querying migration version: %w", err)
	}

	if !found {
		return 0, false, nil
	}

	return version, dirty, nil
}

// SetVersion sets the migration version and dirty flag.
func (e *Executor) SetVersion(ctx context.Context, version uint, dirty bool) error {
	return e.backend.readWriteTransaction(ctx, func(ctx context.Context, txn migrationTransaction) error {
		// Delete all existing rows.
		if _, err := txn.update(ctx, spanner.Statement{SQL: "DELETE FROM SchemaMigrations WHERE true"}); err != nil {
			return err
		}
		// Insert the new version.
		m := []*spanner.Mutation{
			spanner.InsertOrUpdate("SchemaMigrations",
				[]string{"Version", "Dirty"},
				[]any{int64(version), dirty},
			),
		}
		return txn.bufferWrite(m)
	})
}

// ExecuteMigrations applies pending migrations sequentially.
// It returns the number of migrations applied.
// If limit is 0, all pending migrations are applied.
func (e *Executor) ExecuteMigrations(ctx context.Context, migrations []Migration, limit int) (int, error) {
	return e.runner.execute(ctx, migrations, limit)
}

type migrationRunner struct {
	getVersion          func(context.Context) (uint, bool, error)
	setVersion          func(context.Context, uint, bool) error
	updateDatabaseDDL   func(context.Context, []string) error
	applyDML            func(context.Context, []string) error
	applyPartitionedDML func(context.Context, string) (int64, error)
}

func (e *Executor) defaultRunner() migrationRunner {
	return migrationRunner{
		getVersion: e.GetVersion,
		setVersion: e.SetVersion,
		updateDatabaseDDL: func(ctx context.Context, statements []string) error {
			return e.backend.updateDatabaseDDL(ctx, statements)
		},
		applyDML: func(ctx context.Context, statements []string) error {
			return e.client.ApplyDML(ctx, statements)
		},
		applyPartitionedDML: func(ctx context.Context, statement string) (int64, error) {
			return e.client.ApplyPartitionedDML(ctx, statement)
		},
	}
}

type migrationBackend interface {
	query(context.Context, spanner.Statement, func(*spanner.Row) error) error
	updateDatabaseDDL(context.Context, []string) error
	readWriteTransaction(context.Context, func(context.Context, migrationTransaction) error) error
}

type migrationTransaction interface {
	update(context.Context, spanner.Statement) (int64, error)
	bufferWrite([]*spanner.Mutation) error
}

type spannerMigrationBackend struct {
	client *spannerutil.Client
}

func (s spannerMigrationBackend) query(ctx context.Context, stmt spanner.Statement, f func(*spanner.Row) error) error {
	it := s.client.Single().Query(ctx, stmt)
	return it.Do(f)
}

func (s spannerMigrationBackend) updateDatabaseDDL(ctx context.Context, statements []string) error {
	return s.client.UpdateDatabaseDDL(ctx, statements)
}

func (s spannerMigrationBackend) readWriteTransaction(ctx context.Context, f func(context.Context, migrationTransaction) error) error {
	return s.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		return f(ctx, spannerMigrationTransaction{txn: txn})
	})
}

type spannerMigrationTransaction struct {
	txn *spanner.ReadWriteTransaction
}

func (t spannerMigrationTransaction) update(ctx context.Context, stmt spanner.Statement) (int64, error) {
	return t.txn.Update(ctx, stmt)
}

func (t spannerMigrationTransaction) bufferWrite(mutations []*spanner.Mutation) error {
	return t.txn.BufferWrite(mutations)
}

func (r migrationRunner) execute(ctx context.Context, migrations []Migration, limit int) (int, error) {
	currentVersion, dirty, err := r.getVersion(ctx)
	if err != nil {
		return 0, err
	}

	if dirty {
		return 0, fmt.Errorf("database is dirty at version %d; fix the issue and use 'migrate set' to reset the dirty flag", currentVersion)
	}

	applied := 0
	for _, m := range migrations {
		if m.Version <= currentVersion {
			continue
		}
		if limit > 0 && applied >= limit {
			break
		}

		// Set dirty flag.
		if err := r.setVersion(ctx, m.Version, true); err != nil {
			return applied, fmt.Errorf("setting dirty flag for version %d: %w", m.Version, err)
		}

		// Execute statements.
		switch m.Kind {
		case sqlutil.KindDDL:
			if err := r.updateDatabaseDDL(ctx, m.Statements); err != nil {
				return applied, fmt.Errorf("executing DDL migration %d (%s): %w", m.Version, m.Name, err)
			}
		case sqlutil.KindDML:
			if err := r.applyDML(ctx, m.Statements); err != nil {
				return applied, fmt.Errorf("executing DML migration %d (%s): %w", m.Version, m.Name, err)
			}
		case sqlutil.KindPartitionedDML:
			for _, stmt := range m.Statements {
				if _, err := r.applyPartitionedDML(ctx, stmt); err != nil {
					return applied, fmt.Errorf("executing partitioned DML migration %d (%s): %w", m.Version, m.Name, err)
				}
			}
		default:
			return applied, fmt.Errorf("unsupported migration kind %s for version %d (%s)", m.Kind, m.Version, m.Name)
		}

		// Clear dirty flag.
		if err := r.setVersion(ctx, m.Version, false); err != nil {
			return applied, fmt.Errorf("clearing dirty flag for version %d: %w", m.Version, err)
		}

		applied++
	}

	return applied, nil
}

// isTableNotFoundError checks if the error indicates a table was not found.
func isTableNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	// Spanner returns INVALID_ARGUMENT when querying a non-existent table.
	return spanner.ErrCode(err) == codes.InvalidArgument
}
