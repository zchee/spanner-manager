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
	"context"
	"fmt"
	"os"
	"strings"

	"cloud.google.com/go/spanner"
	"github.com/spf13/cobra"
	"google.golang.org/grpc/codes"

	"github.com/zchee/spanner-manager/spannerutil"
	"github.com/zchee/spanner-manager/sqlutil"
)

type adminDatabaseClient interface {
	Close() error
	CreateDatabase(ctx context.Context, statements []string) error
	DropDatabase(ctx context.Context) error
	GetDatabaseDDL(ctx context.Context) ([]string, error)
}

var newAdminDatabaseClient = func(ctx context.Context, cfg spannerutil.Config) (adminDatabaseClient, error) {
	return spannerutil.NewAdminClient(ctx, cfg)
}

const truncateTablesSQL = `SELECT t.TABLE_NAME, COALESCE(t.PARENT_TABLE_NAME, '') AS PARENT_TABLE_NAME
					  FROM INFORMATION_SCHEMA.TABLES t
					  WHERE t.TABLE_SCHEMA = '' AND t.TABLE_TYPE = 'BASE TABLE' AND t.TABLE_NAME != 'SchemaMigrations'
					  ORDER BY t.TABLE_NAME`

func newDBCmd(flags *globalFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "db",
		Short: "Database management commands",
		Long:  "Create, drop, reset, truncate, and export Cloud Spanner databases.",
	}

	cmd.AddCommand(
		newDBCreateCmd(flags),
		newDBDropCmd(flags),
		newDBResetCmd(flags),
		newDBTruncateCmd(flags),
		newDBLoadCmd(flags),
	)

	return cmd
}

func newDBCreateCmd(flags *globalFlags) *cobra.Command {
	var schemaFile string

	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create a new database",
		Long:  "Create a new Cloud Spanner database, optionally initialized with a schema DDL file.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			ctx := cmd.Context()
			cfg := flags.spannerConfig()

			if err := requireDatabaseConfig(cfg); err != nil {
				return err
			}

			if err := writeProgress(cmd, "Creating database: %s", cfg.DatabasePath()); err != nil {
				return err
			}

			var statements []string
			if schemaFile != "" {
				if err := writeProgress(cmd, "Loading schema file: %s", schemaFile); err != nil {
					return err
				}
				data, err := os.ReadFile(schemaFile)
				if err != nil {
					return fmt.Errorf("reading schema file: %w", err)
				}
				stmts, err := sqlutil.SplitStatements(string(data))
				if err != nil {
					return fmt.Errorf("parsing schema file: %w", err)
				}
				statements = stmts
			}

			client, err := newAdminDatabaseClient(ctx, cfg)
			if err != nil {
				return err
			}
			defer func() {
				if cerr := client.Close(); cerr != nil && err == nil {
					err = cerr
				}
			}()

			if err := client.CreateDatabase(ctx, statements); err != nil {
				return err
			}

			if _, err := fmt.Fprintf(cmd.OutOrStdout(), "Created database: %s\n", cfg.DatabasePath()); err != nil {
				return err
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&schemaFile, "schema", "", "path to DDL schema file")

	return cmd
}

func newDBDropCmd(flags *globalFlags) *cobra.Command {
	var force bool

	cmd := &cobra.Command{
		Use:   "drop",
		Short: "Drop a database",
		Long:  "Drop a Cloud Spanner database. This is irreversible and requires --force.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			ctx := cmd.Context()
			cfg := flags.spannerConfig()

			if err := requireDatabaseConfig(cfg); err != nil {
				return err
			}

			if err := requireDestructiveConfirmation("drop database", cfg, force); err != nil {
				return err
			}

			if err := writeProgress(cmd, "Dropping database: %s", cfg.DatabasePath()); err != nil {
				return err
			}

			client, err := newAdminDatabaseClient(ctx, cfg)
			if err != nil {
				return err
			}
			defer func() {
				if cerr := client.Close(); cerr != nil && err == nil {
					err = cerr
				}
			}()

			if err := client.DropDatabase(ctx); err != nil {
				return err
			}

			if _, err := fmt.Fprintf(cmd.OutOrStdout(), "Dropped database: %s\n", cfg.DatabasePath()); err != nil {
				return err
			}
			return nil
		},
	}

	cmd.Flags().BoolVar(&force, "force", false, "confirm irreversible database drop")

	return cmd
}

func newDBResetCmd(flags *globalFlags) *cobra.Command {
	var schemaFile string

	var force bool

	cmd := &cobra.Command{
		Use:   "reset",
		Short: "Drop and recreate a database",
		Long:  "Drop the existing database and recreate it, optionally with a schema DDL file. This is destructive and requires --force.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			ctx := cmd.Context()
			cfg := flags.spannerConfig()

			if err := requireDatabaseConfig(cfg); err != nil {
				return err
			}

			if err := requireDestructiveConfirmation("reset database", cfg, force); err != nil {
				return err
			}

			if err := writeProgress(cmd, "Resetting database: %s", cfg.DatabasePath()); err != nil {
				return err
			}

			client, err := newAdminDatabaseClient(ctx, cfg)
			if err != nil {
				return err
			}
			defer func() {
				if cerr := client.Close(); cerr != nil && err == nil {
					err = cerr
				}
			}()

			// Drop (ignore error if database doesn't exist).
			if err := runWithProgress(cmd, "Dropping database if it exists", func() error {
				return client.DropDatabase(ctx)
			}); err != nil && spanner.ErrCode(err) != codes.NotFound {
				return err
			}

			var statements []string
			if schemaFile != "" {
				if err := writeProgress(cmd, "Loading schema file: %s", schemaFile); err != nil {
					return err
				}
				data, err := os.ReadFile(schemaFile)
				if err != nil {
					return fmt.Errorf("reading schema file: %w", err)
				}
				stmts, err := sqlutil.SplitStatements(string(data))
				if err != nil {
					return fmt.Errorf("parsing schema file: %w", err)
				}
				statements = stmts
			}

			if err := runWithProgress(cmd, "Creating database", func() error {
				return client.CreateDatabase(ctx, statements)
			}); err != nil {
				return err
			}

			if _, err := fmt.Fprintf(cmd.OutOrStdout(), "Reset database: %s\n", cfg.DatabasePath()); err != nil {
				return err
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&schemaFile, "schema", "", "path to DDL schema file")
	cmd.Flags().BoolVar(&force, "force", false, "confirm destructive database reset")

	return cmd
}

func newDBTruncateCmd(flags *globalFlags) *cobra.Command {
	var force bool

	cmd := &cobra.Command{
		Use:   "truncate",
		Short: "Truncate all tables",
		Long:  "Truncate all base tables in the database, preserving the SchemaMigrations table. Respects interleave order (child tables first); non-interleaved foreign keys may require manual cleanup order. This is destructive and requires --force.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			ctx := cmd.Context()
			cfg := flags.spannerConfig()

			if err := requireDatabaseConfig(cfg); err != nil {
				return err
			}

			if err := requireDestructiveConfirmation("truncate database", cfg, force); err != nil {
				return err
			}

			if err := writeProgress(cmd, "Truncating database: %s", cfg.DatabasePath()); err != nil {
				return err
			}

			client, err := spannerutil.NewClient(ctx, cfg)
			if err != nil {
				return err
			}
			defer func() {
				if cerr := client.Close(); cerr != nil && err == nil {
					err = cerr
				}
			}()

			// Query table names and their parent relationships from INFORMATION_SCHEMA.
			iter := client.Single().Query(ctx, spanner.Statement{
				SQL: truncateTablesSQL,
			})

			var tables []tableRelation
			if err := iter.Do(func(row *spanner.Row) error {
				var name, parent string
				if err := row.Columns(&name, &parent); err != nil {
					return err
				}
				tables = append(tables, tableRelation{name: name, parent: parent})
				return nil
			}); err != nil {
				return fmt.Errorf("querying tables: %w", err)
			}

			// Build deletion order: child tables before parent tables.
			ordered := topologicalSort(tables)

			if err := writeProgress(cmd, "Deleting rows from %d table(s)", len(ordered)); err != nil {
				return err
			}

			// Truncate each table.
			for _, name := range ordered {
				if err := writeProgress(cmd, "Deleting rows from table: %s", name); err != nil {
					return err
				}
				if _, err := client.ApplyPartitionedDML(ctx, fmt.Sprintf("DELETE FROM `%s` WHERE true", name)); err != nil {
					return fmt.Errorf("truncating table %s: %w", name, err)
				}
			}

			if _, err := fmt.Fprintf(cmd.OutOrStdout(), "Truncated %d tables in %s\n", len(ordered), cfg.DatabasePath()); err != nil {
				return err
			}
			return nil
		},
	}

	cmd.Flags().BoolVar(&force, "force", false, "confirm destructive database truncate")

	return cmd
}

func newDBLoadCmd(flags *globalFlags) *cobra.Command {
	var outputFile string

	cmd := &cobra.Command{
		Use:   "load",
		Short: "Export DDL from database",
		Long:  "Export the DDL schema from a Cloud Spanner database to stdout or a file.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			ctx := cmd.Context()
			cfg := flags.spannerConfig()

			if err := requireDatabaseConfig(cfg); err != nil {
				return err
			}

			if err := writeProgress(cmd, "Loading database DDL: %s", cfg.DatabasePath()); err != nil {
				return err
			}

			client, err := newAdminDatabaseClient(ctx, cfg)
			if err != nil {
				return err
			}
			defer func() {
				if cerr := client.Close(); cerr != nil && err == nil {
					err = cerr
				}
			}()

			ddl, err := client.GetDatabaseDDL(ctx)
			if err != nil {
				return err
			}

			var output strings.Builder
			for _, stmt := range ddl {
				output.WriteString(stmt)
				output.WriteString(";\n\n")
			}

			if outputFile != "" {
				if err := writeProgress(cmd, "Writing DDL output: %s", outputFile); err != nil {
					return err
				}
				if err := os.WriteFile(outputFile, []byte(output.String()), 0o644); err != nil {
					return fmt.Errorf("writing output file: %w", err)
				}
				if _, err := fmt.Fprintf(cmd.OutOrStdout(), "Exported DDL to %s\n", outputFile); err != nil {
					return err
				}
			} else {
				if _, err := fmt.Fprint(cmd.OutOrStdout(), output.String()); err != nil {
					return err
				}
			}

			return nil
		},
	}

	cmd.Flags().StringVar(&outputFile, "output", "", "output file path (default: stdout)")

	return cmd
}

func requireDestructiveConfirmation(operation string, cfg spannerutil.Config, force bool) error {
	if force {
		return nil
	}

	target := cfg.DatabasePath()
	if cfg.Project == "" || cfg.Instance == "" || cfg.Database == "" {
		target = "the configured database"
	}
	if operation == "truncate database" {
		return fmt.Errorf("%s is destructive for %s; truncation only respects interleave order, so foreign-key-only relationships may need manual handling; rerun with --force to confirm", operation, target)
	}

	return fmt.Errorf("%s is destructive for %s; rerun with --force to confirm", operation, target)
}

func requireDatabaseConfig(cfg spannerutil.Config) error {
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}
	return nil
}

func requireInstanceConfig(cfg spannerutil.Config) error {
	if err := cfg.ValidateInstance(); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}
	return nil
}

// tableRelation represents a table and its parent table name (empty if root).
type tableRelation struct {
	name   string
	parent string
}

// topologicalSort sorts tables so that interleaved child tables come before
// parent tables.
// Non-interleaved foreign key dependencies are not represented in
// INFORMATION_SCHEMA.TABLES.PARENT_TABLE_NAME and may still fail at delete time.
func topologicalSort(tables []tableRelation) []string {
	// Build a map of parent -> children.
	children := make(map[string][]string)
	var roots []string

	for _, t := range tables {
		if t.parent == "" {
			roots = append(roots, t.name)
		} else {
			children[t.parent] = append(children[t.parent], t.name)
		}
	}

	// DFS post-order: children before parents.
	var result []string
	visited := make(map[string]bool)
	var visit func(name string)
	visit = func(name string) {
		if visited[name] {
			return
		}
		visited[name] = true
		for _, child := range children[name] {
			visit(child)
		}
		result = append(result, name)
	}

	for _, root := range roots {
		visit(root)
	}

	// Add any orphan tables not reachable from roots.
	for _, t := range tables {
		if !visited[t.name] {
			visit(t.name)
		}
	}

	return result
}
