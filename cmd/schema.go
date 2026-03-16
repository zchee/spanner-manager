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

	"github.com/spf13/cobra"

	"github.com/zchee/spanner-manager/diff"
	"github.com/zchee/spanner-manager/spannerutil"
	"github.com/zchee/spanner-manager/sqlutil"
)

func newSchemaCmd(flags *globalFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "schema",
		Short: "Schema management commands",
		Long:  "Export, diff, and apply Cloud Spanner schemas.",
	}

	cmd.AddCommand(
		newSchemaExportCmd(flags),
		newSchemaDiffCmd(flags),
		newSchemaApplyCmd(flags),
	)

	return cmd
}

func newSchemaExportCmd(flags *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "export SOURCE",
		Short: "Export DDL from a source",
		Long:  "Export DDL from a Spanner database (spanner://...) or a local DDL file.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			source := args[0]

			ddl, err := loadDDLFromSource(ctx, source, flags)
			if err != nil {
				return err
			}

			for _, stmt := range ddl {
				fmt.Fprintf(cmd.OutOrStdout(), "%s;\n\n", stmt)
			}
			return nil
		},
	}
}

func newSchemaDiffCmd(_ *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "diff SOURCE1 SOURCE2",
		Short: "Compare two schemas",
		Long:  "Compare two DDL sources and output the ALTER DDL needed to migrate from SOURCE1 to SOURCE2.",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

			fromDDL, err := loadDDLFromSource(ctx, args[0], nil)
			if err != nil {
				return fmt.Errorf("loading source1: %w", err)
			}

			toDDL, err := loadDDLFromSource(ctx, args[1], nil)
			if err != nil {
				return fmt.Errorf("loading source2: %w", err)
			}

			fromDB, err := diff.ParseDatabase(fromDDL)
			if err != nil {
				return fmt.Errorf("parsing source1 DDL: %w", err)
			}

			toDB, err := diff.ParseDatabase(toDDL)
			if err != nil {
				return fmt.Errorf("parsing source2 DDL: %w", err)
			}

			statements, err := diff.Diff(fromDB, toDB)
			if err != nil {
				return fmt.Errorf("diffing schemas: %w", err)
			}

			if len(statements) == 0 {
				fmt.Fprintln(cmd.OutOrStdout(), "No differences found.")
				return nil
			}

			for _, stmt := range statements {
				fmt.Fprintf(cmd.OutOrStdout(), "%s;\n\n", stmt.SQL)
			}
			return nil
		},
	}
}

func newSchemaApplyCmd(flags *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "apply SOURCE",
		Short: "Apply schema changes to database",
		Long:  "Diff the current database schema against the desired DDL source and apply the resulting changes.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			cfg := flags.spannerConfig()

			// Load desired schema.
			desiredDDL, err := loadDDLFromSource(ctx, args[0], nil)
			if err != nil {
				return fmt.Errorf("loading desired schema: %w", err)
			}

			client, err := spannerutil.NewClient(ctx, cfg)
			if err != nil {
				return err
			}
			defer client.Close()

			// Load current schema from database.
			currentDDL, err := client.GetDatabaseDDL(ctx)
			if err != nil {
				return fmt.Errorf("getting current schema: %w", err)
			}

			fromDB, err := diff.ParseDatabase(currentDDL)
			if err != nil {
				return fmt.Errorf("parsing current DDL: %w", err)
			}

			toDB, err := diff.ParseDatabase(desiredDDL)
			if err != nil {
				return fmt.Errorf("parsing desired DDL: %w", err)
			}

			statements, err := diff.Diff(fromDB, toDB)
			if err != nil {
				return fmt.Errorf("diffing schemas: %w", err)
			}

			if len(statements) == 0 {
				fmt.Fprintln(cmd.OutOrStdout(), "Schema is up to date.")
				return nil
			}

			// Separate DDL and DML statements.
			var ddlStmts []string
			var dmlStmts []string
			for _, stmt := range statements {
				switch stmt.Kind {
				case sqlutil.KindDDL:
					ddlStmts = append(ddlStmts, stmt.SQL)
				case sqlutil.KindDML:
					dmlStmts = append(dmlStmts, stmt.SQL)
				}
			}

			// Apply DML first (e.g., default value updates for nullable→NOT NULL transitions).
			if len(dmlStmts) > 0 {
				if err := client.ApplyDML(ctx, dmlStmts); err != nil {
					return fmt.Errorf("applying DML: %w", err)
				}
			}

			// Apply DDL.
			if len(ddlStmts) > 0 {
				if err := client.UpdateDatabaseDDL(ctx, ddlStmts); err != nil {
					return fmt.Errorf("applying DDL: %w", err)
				}
			}

			fmt.Fprintf(cmd.OutOrStdout(), "Applied %d statement(s) to %s\n", len(statements), cfg.DatabasePath())
			return nil
		},
	}
}

// loadDDLFromSource loads DDL statements from a source.
// The source can be a file path or a spanner:// URI.
func loadDDLFromSource(ctx context.Context, source string, flags *globalFlags) ([]string, error) {
	if strings.HasPrefix(source, "spanner://") {
		cfg, err := parseSpannerURI(source)
		if err != nil {
			return nil, err
		}
		if flags != nil {
			// Inherit emulator/credentials settings from global flags.
			gcfg := flags.spannerConfig()
			cfg.EmulatorHost = gcfg.EmulatorHost
			cfg.CredentialsFile = gcfg.CredentialsFile
			cfg.Timeout = gcfg.Timeout
		}
		client, err := spannerutil.NewAdminClient(ctx, cfg)
		if err != nil {
			return nil, err
		}
		defer client.Close()
		return client.GetDatabaseDDL(ctx)
	}

	// Treat as file path.
	data, err := os.ReadFile(source)
	if err != nil {
		return nil, fmt.Errorf("reading file %s: %w", source, err)
	}

	return sqlutil.SplitStatements(string(data))
}

// parseSpannerURI parses a spanner:// URI into a Config.
// Format: spanner://projects/P/instances/I/databases/D
func parseSpannerURI(uri string) (spannerutil.Config, error) {
	const prefix = "spanner://projects/"
	if !strings.HasPrefix(uri, prefix) {
		return spannerutil.Config{}, fmt.Errorf("invalid spanner URI: %s (expected spanner://projects/P/instances/I/databases/D)", uri)
	}

	rest := strings.TrimPrefix(uri, prefix)
	parts := strings.Split(rest, "/")
	// Expect: P/instances/I/databases/D → 5 parts.
	if len(parts) != 5 || parts[1] != "instances" || parts[3] != "databases" {
		return spannerutil.Config{}, fmt.Errorf("invalid spanner URI: %s (expected spanner://projects/P/instances/I/databases/D)", uri)
	}

	return spannerutil.Config{
		Project:  parts[0],
		Instance: parts[2],
		Database: parts[4],
	}, nil
}
