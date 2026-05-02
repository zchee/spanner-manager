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
	"errors"
	"fmt"
	"strings"
	"testing"

	"cloud.google.com/go/spanner"
	gocmp "github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/zchee/spanner-manager/spannerutil"
	"github.com/zchee/spanner-manager/sqlutil"
)

type runnerOperation struct {
	Kind       string
	Version    uint
	Dirty      bool
	Statements []string
	Statement  string
}

type recordingRunner struct {
	currentVersion uint
	dirty          bool

	getVersionErr error
	setVersionErr map[string]error
	ddlErr        error
	dmlErr        error
	partitionErr  map[string]error

	operations []runnerOperation
}

type backendOperation struct {
	Kind       string
	SQL        string
	Statements []string
}

type recordingMigrationBackend struct {
	rows     []*spanner.Row
	queryErr error
	ddlErr   error
	txn      *recordingMigrationTransaction

	operations []backendOperation
}

func (s *recordingMigrationBackend) query(_ context.Context, stmt spanner.Statement, f func(*spanner.Row) error) error {
	s.operations = append(s.operations, backendOperation{
		Kind: "query",
		SQL:  stmt.SQL,
	})
	if s.queryErr != nil {
		return s.queryErr
	}
	for _, row := range s.rows {
		if err := f(row); err != nil {
			return err
		}
	}
	return nil
}

func (s *recordingMigrationBackend) updateDatabaseDDL(_ context.Context, statements []string) error {
	s.operations = append(s.operations, backendOperation{
		Kind:       "ddl",
		Statements: append([]string(nil), statements...),
	})
	return s.ddlErr
}

func (s *recordingMigrationBackend) readWriteTransaction(ctx context.Context, f func(context.Context, migrationTransaction) error) error {
	if s.txn == nil {
		s.txn = &recordingMigrationTransaction{}
	}
	return f(ctx, s.txn)
}

type recordingMigrationTransaction struct {
	updateErr error
	bufferErr error

	updates        []spanner.Statement
	bufferedWrites [][]*spanner.Mutation
}

func (t *recordingMigrationTransaction) update(_ context.Context, stmt spanner.Statement) (int64, error) {
	t.updates = append(t.updates, stmt)
	if t.updateErr != nil {
		return 0, t.updateErr
	}
	return 1, nil
}

func (t *recordingMigrationTransaction) bufferWrite(mutations []*spanner.Mutation) error {
	t.bufferedWrites = append(t.bufferedWrites, append([]*spanner.Mutation(nil), mutations...))
	return t.bufferErr
}

func (r *recordingRunner) migrationRunner() migrationRunner {
	return migrationRunner{
		getVersion: func(context.Context) (uint, bool, error) {
			return r.currentVersion, r.dirty, r.getVersionErr
		},
		setVersion: func(_ context.Context, version uint, dirty bool) error {
			r.operations = append(r.operations, runnerOperation{
				Kind:    "setVersion",
				Version: version,
				Dirty:   dirty,
			})
			if r.setVersionErr == nil {
				return nil
			}
			return r.setVersionErr[versionDirtyKey(version, dirty)]
		},
		updateDatabaseDDL: func(_ context.Context, statements []string) error {
			r.operations = append(r.operations, runnerOperation{
				Kind:       "ddl",
				Statements: append([]string(nil), statements...),
			})
			return r.ddlErr
		},
		applyDML: func(_ context.Context, statements []string) error {
			r.operations = append(r.operations, runnerOperation{
				Kind:       "dml",
				Statements: append([]string(nil), statements...),
			})
			return r.dmlErr
		},
		applyPartitionedDML: func(_ context.Context, statement string) (int64, error) {
			r.operations = append(r.operations, runnerOperation{
				Kind:      "partitionedDML",
				Statement: statement,
			})
			if r.partitionErr == nil {
				return 1, nil
			}
			return 0, r.partitionErr[statement]
		},
	}
}

func versionDirtyKey(version uint, dirty bool) string {
	return fmt.Sprintf("%d/%t", version, dirty)
}

func mustSpannerRow(t *testing.T, columnNames []string, columnValues []any) *spanner.Row {
	t.Helper()

	row, err := spanner.NewRow(columnNames, columnValues)
	if err != nil {
		t.Fatalf("spanner.NewRow(%v, %v) error = %v", columnNames, columnValues, err)
	}
	return row
}

func TestNewExecutor(t *testing.T) {
	client := &spannerutil.Client{}
	executor := NewExecutor(client)
	if executor.client != client {
		t.Fatalf("NewExecutor() client = %p, want %p", executor.client, client)
	}
	if executor.runner.getVersion == nil {
		t.Fatal("NewExecutor() runner.getVersion is nil")
	}
	if executor.runner.setVersion == nil {
		t.Fatal("NewExecutor() runner.setVersion is nil")
	}
	if executor.runner.updateDatabaseDDL == nil {
		t.Fatal("NewExecutor() runner.updateDatabaseDDL is nil")
	}
	if executor.runner.applyDML == nil {
		t.Fatal("NewExecutor() runner.applyDML is nil")
	}
	if executor.runner.applyPartitionedDML == nil {
		t.Fatal("NewExecutor() runner.applyPartitionedDML is nil")
	}
}

func TestExecutor_EnsureTable(t *testing.T) {
	errQuery := errors.New("query failed")
	errDDL := errors.New("create table failed")

	tests := map[string]struct {
		configure      func(*recordingMigrationBackend)
		wantErr        string
		wantOperations []backendOperation
	}{
		"success: existing table does not create DDL": {
			wantOperations: []backendOperation{
				{Kind: "query", SQL: "SELECT Version FROM SchemaMigrations LIMIT 1"},
			},
		},
		"success: not found creates schema migrations table": {
			configure: func(store *recordingMigrationBackend) {
				store.queryErr = status.Error(codes.NotFound, "not found")
			},
			wantOperations: []backendOperation{
				{Kind: "query", SQL: "SELECT Version FROM SchemaMigrations LIMIT 1"},
				{Kind: "ddl", Statements: []string{SchemaMigrationsTableDDL}},
			},
		},
		"success: invalid argument table miss creates schema migrations table": {
			configure: func(store *recordingMigrationBackend) {
				store.queryErr = status.Error(codes.InvalidArgument, "Table not found: SchemaMigrations")
			},
			wantOperations: []backendOperation{
				{Kind: "query", SQL: "SELECT Version FROM SchemaMigrations LIMIT 1"},
				{Kind: "ddl", Statements: []string{SchemaMigrationsTableDDL}},
			},
		},
		"error: unrelated query failure is wrapped": {
			configure: func(store *recordingMigrationBackend) {
				store.queryErr = errQuery
			},
			wantErr: "checking SchemaMigrations table: query failed",
			wantOperations: []backendOperation{
				{Kind: "query", SQL: "SELECT Version FROM SchemaMigrations LIMIT 1"},
			},
		},
		"error: create table failure is returned": {
			configure: func(store *recordingMigrationBackend) {
				store.queryErr = status.Error(codes.NotFound, "not found")
				store.ddlErr = errDDL
			},
			wantErr: "create table failed",
			wantOperations: []backendOperation{
				{Kind: "query", SQL: "SELECT Version FROM SchemaMigrations LIMIT 1"},
				{Kind: "ddl", Statements: []string{SchemaMigrationsTableDDL}},
			},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			store := &recordingMigrationBackend{}
			if tt.configure != nil {
				tt.configure(store)
			}
			executor := &Executor{backend: store}

			err := executor.EnsureTable(t.Context())
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("EnsureTable() error = nil, want substring %q", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("EnsureTable() error = %q, want substring %q", err, tt.wantErr)
				}
			} else if err != nil {
				t.Fatalf("EnsureTable() unexpected error = %v", err)
			}
			if diff := gocmp.Diff(tt.wantOperations, store.operations); diff != "" {
				t.Fatalf("EnsureTable() operations mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestExecutor_GetVersion(t *testing.T) {
	errQuery := errors.New("query failed")

	versionRow := mustSpannerRow(t, []string{"Version", "Dirty"}, []any{int64(7), true})
	decodeErrorRow := mustSpannerRow(t, []string{"Version", "Dirty"}, []any{"bad-version", false})

	tests := map[string]struct {
		configure func(*recordingMigrationBackend)
		wantVer   uint
		wantDirty bool
		wantErr   string
	}{
		"success: no rows returns zero clean version": {},
		"success: latest row returns version and dirty flag": {
			configure: func(store *recordingMigrationBackend) {
				store.rows = []*spanner.Row{versionRow}
			},
			wantVer:   7,
			wantDirty: true,
		},
		"error: query failure is wrapped": {
			configure: func(store *recordingMigrationBackend) {
				store.queryErr = errQuery
			},
			wantErr: "querying migration version: query failed",
		},
		"error: row decode failure is wrapped": {
			configure: func(store *recordingMigrationBackend) {
				store.rows = []*spanner.Row{decodeErrorRow}
			},
			wantErr: "querying migration version:",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			store := &recordingMigrationBackend{}
			if tt.configure != nil {
				tt.configure(store)
			}
			executor := &Executor{backend: store}

			gotVer, gotDirty, err := executor.GetVersion(t.Context())
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("GetVersion() error = nil, want substring %q", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("GetVersion() error = %q, want substring %q", err, tt.wantErr)
				}
			} else if err != nil {
				t.Fatalf("GetVersion() unexpected error = %v", err)
			}
			if gotVer != tt.wantVer {
				t.Fatalf("GetVersion() version = %d, want %d", gotVer, tt.wantVer)
			}
			if gotDirty != tt.wantDirty {
				t.Fatalf("GetVersion() dirty = %t, want %t", gotDirty, tt.wantDirty)
			}
			wantOperations := []backendOperation{
				{Kind: "query", SQL: "SELECT Version, Dirty FROM SchemaMigrations ORDER BY Version DESC LIMIT 1"},
			}
			if diff := gocmp.Diff(wantOperations, store.operations); diff != "" {
				t.Fatalf("GetVersion() operations mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestExecutor_SetVersion(t *testing.T) {
	errUpdate := errors.New("delete failed")
	errBuffer := errors.New("buffer failed")

	tests := map[string]struct {
		configure func(*recordingMigrationBackend)
		wantErr   string
	}{
		"success: deletes existing rows and buffers replacement version": {},
		"error: delete failure stops before buffering": {
			configure: func(store *recordingMigrationBackend) {
				store.txn = &recordingMigrationTransaction{updateErr: errUpdate}
			},
			wantErr: "delete failed",
		},
		"error: buffer failure is returned": {
			configure: func(store *recordingMigrationBackend) {
				store.txn = &recordingMigrationTransaction{bufferErr: errBuffer}
			},
			wantErr: "buffer failed",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			store := &recordingMigrationBackend{}
			if tt.configure != nil {
				tt.configure(store)
			}
			executor := &Executor{backend: store}

			err := executor.SetVersion(t.Context(), 9, true)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("SetVersion() error = nil, want substring %q", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("SetVersion() error = %q, want substring %q", err, tt.wantErr)
				}
			} else if err != nil {
				t.Fatalf("SetVersion() unexpected error = %v", err)
			}

			txn := store.txn
			if txn == nil {
				t.Fatal("SetVersion() did not open a transaction")
			}
			wantUpdates := []spanner.Statement{{SQL: "DELETE FROM SchemaMigrations WHERE true"}}
			if diff := gocmp.Diff(wantUpdates, txn.updates); diff != "" {
				t.Fatalf("SetVersion() updates mismatch (-want +got):\n%s", diff)
			}
			if tt.wantErr == "delete failed" {
				if len(txn.bufferedWrites) != 0 {
					t.Fatalf("SetVersion() buffered writes after delete failure = %d, want 0", len(txn.bufferedWrites))
				}
				return
			}
			wantWrites := [][]*spanner.Mutation{{
				spanner.InsertOrUpdate("SchemaMigrations",
					[]string{"Version", "Dirty"},
					[]any{int64(9), true},
				),
			}}
			if diff := gocmp.Diff(wantWrites, txn.bufferedWrites, gocmp.AllowUnexported(spanner.Mutation{})); diff != "" {
				t.Fatalf("SetVersion() buffered writes mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestExecutor_DefaultRunnerUsesBackend(t *testing.T) {
	store := &recordingMigrationBackend{
		rows: []*spanner.Row{mustSpannerRow(t, []string{"Version", "Dirty"}, []any{int64(12), false})},
	}
	executor := &Executor{backend: store}
	runner := executor.defaultRunner()

	gotVer, gotDirty, err := runner.getVersion(t.Context())
	if err != nil {
		t.Fatalf("defaultRunner().getVersion() unexpected error = %v", err)
	}
	if gotVer != 12 {
		t.Fatalf("defaultRunner().getVersion() version = %d, want 12", gotVer)
	}
	if gotDirty {
		t.Fatal("defaultRunner().getVersion() dirty = true, want false")
	}

	if err := runner.setVersion(t.Context(), 13, false); err != nil {
		t.Fatalf("defaultRunner().setVersion() unexpected error = %v", err)
	}
	if err := runner.updateDatabaseDDL(t.Context(), []string{"CREATE TABLE T (ID INT64) PRIMARY KEY (ID)"}); err != nil {
		t.Fatalf("defaultRunner().updateDatabaseDDL() unexpected error = %v", err)
	}

	wantOperations := []backendOperation{
		{Kind: "query", SQL: "SELECT Version, Dirty FROM SchemaMigrations ORDER BY Version DESC LIMIT 1"},
		{Kind: "ddl", Statements: []string{"CREATE TABLE T (ID INT64) PRIMARY KEY (ID)"}},
	}
	if diff := gocmp.Diff(wantOperations, store.operations); diff != "" {
		t.Fatalf("defaultRunner() backend operations mismatch (-want +got):\n%s", diff)
	}
}

func TestExecuteMigrations(t *testing.T) {
	errGetVersion := errors.New("get version failed")
	errSetDirty := errors.New("set dirty failed")
	errDDL := errors.New("ddl failed")
	errDML := errors.New("dml failed")
	errPartitioned := errors.New("partitioned failed")
	errClearDirty := errors.New("clear dirty failed")

	migrations := []Migration{
		{
			Version:    1,
			Name:       "create_users",
			Statements: []string{"CREATE TABLE Users (UserId INT64 NOT NULL) PRIMARY KEY (UserId)"},
			Kind:       sqlutil.KindDDL,
		},
		{
			Version:    2,
			Name:       "seed_users",
			Statements: []string{"INSERT INTO Users (UserId) VALUES (1)"},
			Kind:       sqlutil.KindDML,
		},
		{
			Version: 3,
			Name:    "backfill_users",
			Statements: []string{
				"-- PARTITIONED_DML\nUPDATE Users SET Active = TRUE WHERE Active IS NULL",
				"-- PARTITIONED_DML\nUPDATE Users SET Region = 'global' WHERE Region IS NULL",
			},
			Kind: sqlutil.KindPartitionedDML,
		},
	}

	tests := map[string]struct {
		configure     func(*recordingRunner)
		input         []Migration
		limit         int
		wantApplied   int
		wantErr       string
		wantOperation []runnerOperation
	}{
		"success: applies all pending migration kinds in order": {
			input:       migrations,
			wantApplied: 3,
			wantOperation: []runnerOperation{
				{Kind: "setVersion", Version: 1, Dirty: true},
				{Kind: "ddl", Statements: []string{"CREATE TABLE Users (UserId INT64 NOT NULL) PRIMARY KEY (UserId)"}},
				{Kind: "setVersion", Version: 1, Dirty: false},
				{Kind: "setVersion", Version: 2, Dirty: true},
				{Kind: "dml", Statements: []string{"INSERT INTO Users (UserId) VALUES (1)"}},
				{Kind: "setVersion", Version: 2, Dirty: false},
				{Kind: "setVersion", Version: 3, Dirty: true},
				{Kind: "partitionedDML", Statement: "-- PARTITIONED_DML\nUPDATE Users SET Active = TRUE WHERE Active IS NULL"},
				{Kind: "partitionedDML", Statement: "-- PARTITIONED_DML\nUPDATE Users SET Region = 'global' WHERE Region IS NULL"},
				{Kind: "setVersion", Version: 3, Dirty: false},
			},
		},
		"success: skips applied migrations and respects limit": {
			configure: func(r *recordingRunner) {
				r.currentVersion = 1
			},
			input:       migrations,
			limit:       1,
			wantApplied: 1,
			wantOperation: []runnerOperation{
				{Kind: "setVersion", Version: 2, Dirty: true},
				{Kind: "dml", Statements: []string{"INSERT INTO Users (UserId) VALUES (1)"}},
				{Kind: "setVersion", Version: 2, Dirty: false},
			},
		},
		"success: limit zero with no pending migrations": {
			configure: func(r *recordingRunner) {
				r.currentVersion = 3
			},
			input:       migrations,
			wantApplied: 0,
		},
		"error: get version failure stops before work": {
			configure: func(r *recordingRunner) {
				r.getVersionErr = errGetVersion
			},
			input:   migrations,
			wantErr: "get version failed",
		},
		"error: dirty database stops before work": {
			configure: func(r *recordingRunner) {
				r.currentVersion = 2
				r.dirty = true
			},
			input:   migrations,
			wantErr: "database is dirty at version 2",
		},
		"error: setting dirty flag includes version": {
			configure: func(r *recordingRunner) {
				r.setVersionErr = map[string]error{versionDirtyKey(1, true): errSetDirty}
			},
			input:   migrations,
			wantErr: "setting dirty flag for version 1: set dirty failed",
			wantOperation: []runnerOperation{
				{Kind: "setVersion", Version: 1, Dirty: true},
			},
		},
		"error: DDL failure leaves migration dirty": {
			configure: func(r *recordingRunner) {
				r.ddlErr = errDDL
			},
			input:   migrations[:1],
			wantErr: "executing DDL migration 1 (create_users): ddl failed",
			wantOperation: []runnerOperation{
				{Kind: "setVersion", Version: 1, Dirty: true},
				{Kind: "ddl", Statements: []string{"CREATE TABLE Users (UserId INT64 NOT NULL) PRIMARY KEY (UserId)"}},
			},
		},
		"error: DML failure leaves migration dirty": {
			configure: func(r *recordingRunner) {
				r.dmlErr = errDML
			},
			input:   migrations[1:2],
			wantErr: "executing DML migration 2 (seed_users): dml failed",
			wantOperation: []runnerOperation{
				{Kind: "setVersion", Version: 2, Dirty: true},
				{Kind: "dml", Statements: []string{"INSERT INTO Users (UserId) VALUES (1)"}},
			},
		},
		"error: partitioned DML failure stops before clearing dirty flag": {
			configure: func(r *recordingRunner) {
				r.partitionErr = map[string]error{
					"-- PARTITIONED_DML\nUPDATE Users SET Region = 'global' WHERE Region IS NULL": errPartitioned,
				}
			},
			input:   migrations[2:],
			wantErr: "executing partitioned DML migration 3 (backfill_users): partitioned failed",
			wantOperation: []runnerOperation{
				{Kind: "setVersion", Version: 3, Dirty: true},
				{Kind: "partitionedDML", Statement: "-- PARTITIONED_DML\nUPDATE Users SET Active = TRUE WHERE Active IS NULL"},
				{Kind: "partitionedDML", Statement: "-- PARTITIONED_DML\nUPDATE Users SET Region = 'global' WHERE Region IS NULL"},
			},
		},
		"error: unsupported migration kind stops before dirty flag": {
			input: []Migration{
				{
					Version:    4,
					Name:       "unknown_kind",
					Statements: []string{"SELECT 1"},
					Kind:       sqlutil.StatementKind(99),
				},
			},
			wantErr: "unsupported migration kind StatementKind(99) for version 4 (unknown_kind)",
		},
		"error: clearing dirty flag reports version after successful statements": {
			configure: func(r *recordingRunner) {
				r.setVersionErr = map[string]error{versionDirtyKey(1, false): errClearDirty}
			},
			input:   migrations[:1],
			wantErr: "clearing dirty flag for version 1: clear dirty failed",
			wantOperation: []runnerOperation{
				{Kind: "setVersion", Version: 1, Dirty: true},
				{Kind: "ddl", Statements: []string{"CREATE TABLE Users (UserId INT64 NOT NULL) PRIMARY KEY (UserId)"}},
				{Kind: "setVersion", Version: 1, Dirty: false},
			},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			recorder := &recordingRunner{}
			if tt.configure != nil {
				tt.configure(recorder)
			}

			executor := &Executor{runner: recorder.migrationRunner()}
			gotApplied, err := executor.ExecuteMigrations(t.Context(), tt.input, tt.limit)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("ExecuteMigrations() error = nil, want substring %q", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("ExecuteMigrations() error = %q, want substring %q", err, tt.wantErr)
				}
			} else if err != nil {
				t.Fatalf("ExecuteMigrations() unexpected error = %v", err)
			}
			if gotApplied != tt.wantApplied {
				t.Fatalf("ExecuteMigrations() applied = %d, want %d", gotApplied, tt.wantApplied)
			}
			if diff := gocmp.Diff(tt.wantOperation, recorder.operations); diff != "" {
				t.Fatalf("ExecuteMigrations() operations mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestIsExecutableMigrationKind(t *testing.T) {
	tests := map[string]struct {
		kind sqlutil.StatementKind
		want bool
	}{
		"success: DDL is executable": {
			kind: sqlutil.KindDDL,
			want: true,
		},
		"success: DML is executable": {
			kind: sqlutil.KindDML,
			want: true,
		},
		"success: partitioned DML is executable": {
			kind: sqlutil.KindPartitionedDML,
			want: true,
		},
		"success: unknown kind is rejected": {
			kind: sqlutil.StatementKind(99),
			want: false,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if got := isExecutableMigrationKind(tt.kind); got != tt.want {
				t.Fatalf("isExecutableMigrationKind(%v) = %t, want %t", tt.kind, got, tt.want)
			}
		})
	}
}

func TestIsTableNotFoundError(t *testing.T) {
	tests := map[string]struct {
		err  error
		want bool
	}{
		"success: nil is false": {
			err:  nil,
			want: false,
		},
		"success: invalid argument is table-not-found compatible": {
			err:  status.Error(codes.InvalidArgument, "Table not found: SchemaMigrations"),
			want: true,
		},
		"success: not found is handled by caller": {
			err:  status.Error(codes.NotFound, "database not found"),
			want: false,
		},
		"success: unrelated code is false": {
			err:  status.Error(codes.Internal, "internal"),
			want: false,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if got := isTableNotFoundError(tt.err); got != tt.want {
				t.Fatalf("isTableNotFoundError() = %t, want %t", got, tt.want)
			}
		})
	}
}
