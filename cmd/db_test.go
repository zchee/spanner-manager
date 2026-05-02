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

package cmd

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	gocmp "github.com/google/go-cmp/cmp"
	"github.com/spf13/cobra"

	"github.com/zchee/spanner-manager/spannerutil"
)

func TestDestructiveDBCommandsRequireForce(t *testing.T) {
	flags := &globalFlags{
		project:  "test-project",
		instance: "test-instance",
		database: "test-database",
	}

	tests := map[string]struct {
		command *cobra.Command
		want    string
	}{
		"drop": {
			command: newDBDropCmd(flags),
			want:    "drop database is destructive for projects/test-project/instances/test-instance/databases/test-database; rerun with --force to confirm",
		},
		"reset": {
			command: newDBResetCmd(flags),
			want:    "reset database is destructive for projects/test-project/instances/test-instance/databases/test-database; rerun with --force to confirm",
		},
		"truncate": {
			command: newDBTruncateCmd(flags),
			want:    "truncate database is destructive for projects/test-project/instances/test-instance/databases/test-database; truncation only respects interleave order, so foreign-key-only relationships may need manual handling; rerun with --force to confirm",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			err := executeDBCommandForTest(t, tt.command)
			if err == nil {
				t.Fatal("command succeeded without --force, want safety error")
			}
			if got := err.Error(); got != tt.want {
				t.Fatalf("error mismatch (-want +got):\n%s", gocmp.Diff(tt.want, got))
			}
			if tt.command.Flags().Lookup("force") == nil {
				t.Fatal("missing --force flag")
			}
		})
	}
}

func TestDBCommandsRequireCompleteDatabaseTarget(t *testing.T) {
	flags := &globalFlags{
		project:  "test-project",
		instance: "test-instance",
	}

	tests := map[string]*cobra.Command{
		"create":   newDBCreateCmd(flags),
		"drop":     newDBDropCmd(flags),
		"reset":    newDBResetCmd(flags),
		"truncate": newDBTruncateCmd(flags),
		"load":     newDBLoadCmd(flags),
	}

	for name, command := range tests {
		t.Run(name, func(t *testing.T) {
			var stderr bytes.Buffer
			err := executeDBCommandForTest(t, command, withCommandErr(&stderr))
			if err == nil {
				t.Fatal("command succeeded with incomplete database target, want validation error")
			}
			if got := err.Error(); got != "invalid config: database is required" {
				t.Fatalf("error mismatch (-want +got):\n%s", gocmp.Diff("invalid config: database is required", got))
			}
			if got := stderr.String(); got != "" {
				t.Fatalf("stderr = %q, want no progress before validation succeeds", got)
			}
		})
	}
}

func TestDestructiveDBCommandsDoNotReportProgressWithoutForce(t *testing.T) {
	flags := &globalFlags{
		project:  "test-project",
		instance: "test-instance",
		database: "test-database",
	}

	tests := map[string]*cobra.Command{
		"drop":     newDBDropCmd(flags),
		"reset":    newDBResetCmd(flags),
		"truncate": newDBTruncateCmd(flags),
	}

	for name, command := range tests {
		t.Run(name, func(t *testing.T) {
			var stderr bytes.Buffer
			err := executeDBCommandForTest(t, command, withCommandErr(&stderr))
			if err == nil {
				t.Fatal("command succeeded without --force, want safety error")
			}
			if got := stderr.String(); got != "" {
				t.Fatalf("stderr = %q, want no progress before destructive confirmation succeeds", got)
			}
		})
	}
}

func TestRequireDestructiveConfirmation(t *testing.T) {
	fullConfig := spannerutil.Config{
		Project:  "test-project",
		Instance: "test-instance",
		Database: "test-database",
	}

	tests := map[string]struct {
		operation string
		config    spannerutil.Config
		force     bool
		wantErr   string
	}{
		"force allows operation": {
			operation: "drop database",
			config:    fullConfig,
			force:     true,
		},
		"full target included in error": {
			operation: "truncate database",
			config:    fullConfig,
			wantErr:   "truncate database is destructive for projects/test-project/instances/test-instance/databases/test-database; truncation only respects interleave order, so foreign-key-only relationships may need manual handling; rerun with --force to confirm",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			err := requireDestructiveConfirmation(tt.operation, tt.config, tt.force)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("requireDestructiveConfirmation() error = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatal("requireDestructiveConfirmation() error = nil, want error")
			}
			if got := err.Error(); got != tt.wantErr {
				t.Fatalf("error mismatch (-want +got):\n%s", gocmp.Diff(tt.wantErr, got))
			}
		})
	}
}

func TestDBCommandsRejectPositionalArguments(t *testing.T) {
	flags := &globalFlags{}
	tests := map[string]*cobra.Command{
		"create":   newDBCreateCmd(flags),
		"drop":     newDBDropCmd(flags),
		"reset":    newDBResetCmd(flags),
		"truncate": newDBTruncateCmd(flags),
		"load":     newDBLoadCmd(flags),
	}

	for name, command := range tests {
		t.Run(name, func(t *testing.T) {
			err := command.Args(command, []string{"unexpected"})
			if err == nil {
				t.Fatal("Args() error = nil, want positional argument rejection")
			}
			if !strings.Contains(err.Error(), "unknown command") && !strings.Contains(err.Error(), "accepts 0 arg(s)") {
				t.Fatalf("Args() error = %q, want Cobra no-args rejection", err.Error())
			}
		})
	}
}

func TestDBTruncateDocumentsForeignKeyLimitation(t *testing.T) {
	cmd := newDBTruncateCmd(&globalFlags{})
	if !strings.Contains(cmd.Long, "non-interleaved foreign keys may require manual cleanup order") {
		t.Fatalf("truncate long description = %q, want explicit non-interleaved FK limitation", cmd.Long)
	}
}

func TestTopologicalSortOrdersChildrenBeforeParents(t *testing.T) {
	tables := []tableRelation{
		{name: "Parents"},
		{name: "Children", parent: "Parents"},
		{name: "Grandchildren", parent: "Children"},
		{name: "Orphan", parent: "MissingParent"},
	}

	got := topologicalSort(tables)
	want := []string{"Grandchildren", "Children", "Parents", "Orphan"}
	if diff := gocmp.Diff(want, got); diff != "" {
		t.Fatalf("topologicalSort() mismatch (-want +got):\n%s", diff)
	}
}

func TestTruncateTablesSQLFiltersViews(t *testing.T) {
	tests := map[string]string{
		"base tables":       "t.TABLE_TYPE = 'BASE TABLE'",
		"schema migrations": "t.TABLE_NAME != 'SchemaMigrations'",
	}

	for name, want := range tests {
		t.Run(name, func(t *testing.T) {
			if !strings.Contains(truncateTablesSQL, want) {
				t.Fatalf("truncateTablesSQL = %q, want %q", truncateTablesSQL, want)
			}
		})
	}
}

func TestQuoteIdentifier(t *testing.T) {
	tests := map[string]struct {
		name string
		want string
	}{
		"success: ordinary table name": {
			name: "Users",
			want: "`Users`",
		},
		"success: embedded backtick is doubled": {
			name: "a`b",
			want: "`a``b`",
		},
		"success: multiple embedded backticks are doubled": {
			name: "a`b`c",
			want: "`a``b``c`",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if got := quoteIdentifier(tt.name); got != tt.want {
				t.Fatalf("quoteIdentifier(%q) = %q, want %q", tt.name, got, tt.want)
			}
		})
	}
}

func TestDBResetDoesNotDuplicateDropProgressLine(t *testing.T) {
	flags := &globalFlags{
		project:  "test-project",
		instance: "test-instance",
		database: "test-database",
	}

	var stderr bytes.Buffer
	fakeClient := &fakeAdminDatabaseClient{}
	previousFactory := newAdminDatabaseClient
	newAdminDatabaseClient = func(ctx context.Context, cfg spannerutil.Config) (adminDatabaseClient, error) {
		return fakeClient, nil
	}
	t.Cleanup(func() {
		newAdminDatabaseClient = previousFactory
	})

	err := executeDBCommandForTest(t, newDBResetCmd(flags), withCommandErr(&stderr), "--force")
	if err != nil {
		t.Fatalf("reset command error = %v, want nil", err)
	}
	if fakeClient.dropCalls != 1 {
		t.Fatalf("DropDatabase calls = %d, want 1", fakeClient.dropCalls)
	}
	if fakeClient.createCalls != 1 {
		t.Fatalf("CreateDatabase calls = %d, want 1", fakeClient.createCalls)
	}
	if got := stderr.String(); strings.Contains(got, "Dropping database if it exists: projects/test-project/instances/test-instance/databases/test-database") {
		t.Fatalf("stderr = %q, want no duplicate drop progress line with database path", got)
	}
	if !strings.Contains(stderr.String(), "Resetting database: projects/test-project/instances/test-instance/databases/test-database") {
		t.Fatalf("stderr = %q, want reset progress line", stderr.String())
	}
}

type commandTestOption func(*cobra.Command)

func withCommandErr(w io.Writer) commandTestOption {
	return func(command *cobra.Command) {
		command.SetErr(w)
	}
}

func executeDBCommandForTest(t *testing.T, command *cobra.Command, optsOrArgs ...any) error {
	t.Helper()

	command.SetOut(io.Discard)
	command.SetErr(io.Discard)

	var args []string
	for _, optOrArg := range optsOrArgs {
		switch v := optOrArg.(type) {
		case commandTestOption:
			v(command)
		case string:
			args = append(args, v)
		default:
			t.Fatalf("unsupported test command option %T", optOrArg)
		}
	}

	command.SetArgs(args)
	command.SetContext(t.Context())
	command.SilenceUsage = true
	command.SilenceErrors = true

	return command.Execute()
}

type fakeAdminDatabaseClient struct {
	dropCalls   int
	createCalls int
}

func (c *fakeAdminDatabaseClient) Close() error { return nil }

func (c *fakeAdminDatabaseClient) CreateDatabase(context.Context, []string) error {
	c.createCalls++
	return nil
}

func (c *fakeAdminDatabaseClient) DropDatabase(context.Context) error {
	c.dropCalls++
	return nil
}

func (c *fakeAdminDatabaseClient) GetDatabaseDDL(context.Context) ([]string, error) {
	return nil, nil
}
