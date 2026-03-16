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

// Package cmd defines the Cobra command tree for spanner-manager.
package cmd

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/zchee/spanner-manager/spannerutil"
)

const defaultTimeout = 1 * time.Hour

// globalFlags holds the values parsed from global persistent flags.
type globalFlags struct {
	project      string
	instance     string
	database     string
	credentials  string
	emulatorHost string
	timeout      time.Duration
}

// spannerConfig builds a spanner.Config from the parsed global flags.
func (f *globalFlags) spannerConfig() spannerutil.Config {
	return spannerutil.Config{
		Project:         f.project,
		Instance:        f.instance,
		Database:        f.database,
		CredentialsFile: f.credentials,
		EmulatorHost:    f.emulatorHost,
		Timeout:         f.timeout,
	}
}

// Execute runs the root command with the given context.
func Execute(ctx context.Context) error {
	return newRootCmd().ExecuteContext(ctx)
}

func newRootCmd() *cobra.Command {
	flags := &globalFlags{}

	cmd := &cobra.Command{
		Use:   "spanner-manager",
		Short: "Unified Cloud Spanner CLI tool",
		Long: `spanner-manager unifies Cloud Spanner schema migration, schema diffing,
and Go ORM code generation into a single CLI.

It replaces three separate tools (wrench, hammer, yo) with a coherent
command structure, shared connection management, and a common SQL parser.`,
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	// Register global persistent flags.
	pf := cmd.PersistentFlags()
	pf.StringVarP(&flags.project, "project", "p", envDefault("SPANNER_PROJECT_ID", envDefault("GOOGLE_CLOUD_PROJECT", "")), "GCP project ID")
	pf.StringVarP(&flags.instance, "instance", "i", envDefault("SPANNER_INSTANCE_ID", ""), "Spanner instance ID")
	pf.StringVarP(&flags.database, "database", "d", envDefault("SPANNER_DATABASE_ID", ""), "Spanner database ID")
	pf.StringVar(&flags.credentials, "credentials", envDefault("GOOGLE_APPLICATION_CREDENTIALS", ""), "path to credentials JSON")
	pf.StringVar(&flags.emulatorHost, "emulator-host", envDefault("SPANNER_EMULATOR_HOST", ""), "Spanner emulator address (e.g., localhost:9010)")
	pf.DurationVar(&flags.timeout, "timeout", defaultTimeout, "operation timeout")

	// Register subcommand groups.
	cmd.AddCommand(
		newDBCmd(flags),
		newMigrateCmd(flags),
		newSchemaCmd(flags),
		newGenerateCmd(flags),
		newInstanceCmd(flags),
		newVersionCmd(),
	)

	return cmd
}

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the version of spanner-manager",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("spanner-manager version dev")
		},
	}
}

// envDefault returns the value of the named environment variable, or fallback if unset or empty.
func envDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
