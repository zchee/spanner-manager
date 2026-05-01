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
		"error: unsupported migration kind leaves migration dirty": {
			input: []Migration{
				{
					Version:    4,
					Name:       "unknown_kind",
					Statements: []string{"SELECT 1"},
					Kind:       sqlutil.StatementKind(99),
				},
			},
			wantErr: "unsupported migration kind StatementKind(99) for version 4 (unknown_kind)",
			wantOperation: []runnerOperation{
				{Kind: "setVersion", Version: 4, Dirty: true},
			},
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
