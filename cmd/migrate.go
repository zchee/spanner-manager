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
	"path/filepath"
	"strconv"

	"github.com/spf13/cobra"

	"github.com/zchee/spanner-manager/migration"
	"github.com/zchee/spanner-manager/spannerutil"
)

func newMigrateCmd(flags *globalFlags) *cobra.Command {
	var directory string

	cmd := &cobra.Command{
		Use:   "migrate",
		Short: "Schema migration commands",
		Long:  "Create, apply, and track versioned schema migrations.",
	}

	cmd.PersistentFlags().StringVar(&directory, "directory", ".", "migration directory (contains migrations/ subdirectory)")

	cmd.AddCommand(
		newMigrateCreateCmd(flags, &directory),
		newMigrateUpCmd(flags, &directory),
		newMigrateVersionCmd(flags),
		newMigrateSetCmd(flags),
	)

	return cmd
}

func newMigrateCreateCmd(_ *globalFlags, directory *string) *cobra.Command {
	return &cobra.Command{
		Use:   "create NAME",
		Short: "Create a new migration file",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			dir := filepath.Join(*directory, "migrations")

			// Ensure migrations directory exists.
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return fmt.Errorf("creating migrations directory: %w", err)
			}

			// Read existing migrations to determine next version.
			migrations, err := migration.ReadMigrations(*directory)
			if err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("reading existing migrations: %w", err)
			}

			nextVersion := uint(1)
			if len(migrations) > 0 {
				nextVersion = migrations[len(migrations)-1].Version + 1
			}

			filename := fmt.Sprintf("%06d_%s.sql", nextVersion, name)
			path := filepath.Join(dir, filename)

			if err := os.WriteFile(path, []byte(""), 0o644); err != nil {
				return fmt.Errorf("creating migration file: %w", err)
			}

			fmt.Fprintf(cmd.OutOrStdout(), "Created migration: %s\n", path)
			return nil
		},
	}
}

func newMigrateUpCmd(flags *globalFlags, directory *string) *cobra.Command {
	return &cobra.Command{
		Use:   "up [N]",
		Short: "Apply pending migrations",
		Long:  "Apply all pending migrations, or up to N migrations if specified.",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			cfg := flags.spannerConfig()

			limit := 0 // 0 means apply all.
			if len(args) > 0 {
				n, err := strconv.Atoi(args[0])
				if err != nil {
					return fmt.Errorf("invalid migration count %q: %w", args[0], err)
				}
				limit = n
			}

			migrations, err := migration.ReadMigrations(*directory)
			if err != nil {
				return fmt.Errorf("reading migrations: %w", err)
			}

			if len(migrations) == 0 {
				fmt.Fprintln(cmd.OutOrStdout(), "No migrations found.")
				return nil
			}

			client, err := spannerutil.NewClient(ctx, cfg)
			if err != nil {
				return err
			}
			defer client.Close()

			executor := migration.NewExecutor(client)
			if err := executor.EnsureTable(ctx); err != nil {
				return err
			}

			applied, err := executor.ExecuteMigrations(ctx, migrations, limit)
			if err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "Applied %d migration(s).\n", applied)
			return nil
		},
	}
}

func newMigrateVersionCmd(flags *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print current migration version",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			cfg := flags.spannerConfig()

			client, err := spannerutil.NewClient(ctx, cfg)
			if err != nil {
				return err
			}
			defer client.Close()

			executor := migration.NewExecutor(client)
			version, dirty, err := executor.GetVersion(ctx)
			if err != nil {
				return err
			}

			if dirty {
				fmt.Fprintf(cmd.OutOrStdout(), "%d (dirty)\n", version)
			} else {
				fmt.Fprintf(cmd.OutOrStdout(), "%d\n", version)
			}
			return nil
		},
	}
}

func newMigrateSetCmd(flags *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "set VERSION",
		Short: "Set migration version without running migrations",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			cfg := flags.spannerConfig()

			version, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return fmt.Errorf("invalid version %q: %w", args[0], err)
			}

			client, err := spannerutil.NewClient(ctx, cfg)
			if err != nil {
				return err
			}
			defer client.Close()

			executor := migration.NewExecutor(client)
			if err := executor.SetVersion(ctx, uint(version), false); err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "Set migration version to %d.\n", version)
			return nil
		},
	}
}
