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
	"fmt"
	"os"
	"strings"

	"cloud.google.com/go/spanner"
	"github.com/spf13/cobra"

	"github.com/zchee/spanner-manager/spannerutil"
	"github.com/zchee/spanner-manager/sqlutil"
)

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
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			cfg := flags.spannerConfig()

			var statements []string
			if schemaFile != "" {
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

			client, err := spannerutil.NewAdminClient(ctx, cfg)
			if err != nil {
				return err
			}
			defer client.Close()

			if err := client.CreateDatabase(ctx, statements); err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "Created database: %s\n", cfg.DatabasePath())
			return nil
		},
	}

	cmd.Flags().StringVar(&schemaFile, "schema", "", "path to DDL schema file")

	return cmd
}

func newDBDropCmd(flags *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "drop",
		Short: "Drop a database",
		Long:  "Drop a Cloud Spanner database. This is irreversible.",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			cfg := flags.spannerConfig()

			client, err := spannerutil.NewAdminClient(ctx, cfg)
			if err != nil {
				return err
			}
			defer client.Close()

			if err := client.DropDatabase(ctx); err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "Dropped database: %s\n", cfg.DatabasePath())
			return nil
		},
	}
}

func newDBResetCmd(flags *globalFlags) *cobra.Command {
	var schemaFile string

	cmd := &cobra.Command{
		Use:   "reset",
		Short: "Drop and recreate a database",
		Long:  "Drop the existing database and recreate it, optionally with a schema DDL file.",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			cfg := flags.spannerConfig()

			client, err := spannerutil.NewAdminClient(ctx, cfg)
			if err != nil {
				return err
			}
			defer client.Close()

			// Drop (ignore error if database doesn't exist).
			_ = client.DropDatabase(ctx)

			var statements []string
			if schemaFile != "" {
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

			if err := client.CreateDatabase(ctx, statements); err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "Reset database: %s\n", cfg.DatabasePath())
			return nil
		},
	}

	cmd.Flags().StringVar(&schemaFile, "schema", "", "path to DDL schema file")

	return cmd
}

func newDBTruncateCmd(flags *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "truncate",
		Short: "Truncate all tables",
		Long:  "Truncate all tables in the database, preserving the SchemaMigrations table. Respects interleave order (child tables first).",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			cfg := flags.spannerConfig()

			client, err := spannerutil.NewClient(ctx, cfg)
			if err != nil {
				return err
			}
			defer client.Close()

			// Query table names and their parent relationships from INFORMATION_SCHEMA.
			iter := client.Single().Query(ctx, spanner.Statement{
				SQL: `SELECT t.TABLE_NAME, COALESCE(t.PARENT_TABLE_NAME, '') AS PARENT_TABLE_NAME
					  FROM INFORMATION_SCHEMA.TABLES t
					  WHERE t.TABLE_SCHEMA = '' AND t.TABLE_NAME != 'SchemaMigrations'
					  ORDER BY t.TABLE_NAME`,
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

			// Truncate each table.
			for _, name := range ordered {
				if _, err := client.ApplyPartitionedDML(ctx, fmt.Sprintf("DELETE FROM `%s` WHERE true", name)); err != nil {
					return fmt.Errorf("truncating table %s: %w", name, err)
				}
			}

			fmt.Fprintf(cmd.OutOrStdout(), "Truncated %d tables in %s\n", len(ordered), cfg.DatabasePath())
			return nil
		},
	}
}

func newDBLoadCmd(flags *globalFlags) *cobra.Command {
	var outputFile string

	cmd := &cobra.Command{
		Use:   "load",
		Short: "Export DDL from database",
		Long:  "Export the DDL schema from a Cloud Spanner database to stdout or a file.",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			cfg := flags.spannerConfig()

			client, err := spannerutil.NewAdminClient(ctx, cfg)
			if err != nil {
				return err
			}
			defer client.Close()

			ddl, err := client.GetDatabaseDDL(ctx)
			if err != nil {
				return err
			}

			var output strings.Builder
			for _, stmt := range ddl {
				output.WriteString(stmt + ";\n\n")
			}

			if outputFile != "" {
				if err := os.WriteFile(outputFile, []byte(output.String()), 0o644); err != nil {
					return fmt.Errorf("writing output file: %w", err)
				}
				fmt.Fprintf(cmd.OutOrStdout(), "Exported DDL to %s\n", outputFile)
			} else {
				fmt.Fprint(cmd.OutOrStdout(), output.String())
			}

			return nil
		},
	}

	cmd.Flags().StringVar(&outputFile, "output", "", "output file path (default: stdout)")

	return cmd
}

// tableRelation represents a table and its parent table name (empty if root).
type tableRelation struct {
	name   string
	parent string
}

// topologicalSort sorts tables so that child tables come before parent tables.
// This ensures that DELETE operations respect foreign key / interleave constraints.
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
