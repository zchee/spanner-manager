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

	"github.com/spf13/cobra"

	"github.com/zchee/spanner-manager/spannerutil"
)

func newInstanceCmd(flags *globalFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "instance",
		Short: "Instance management commands",
		Long:  "Create and delete Cloud Spanner instances.",
	}

	cmd.AddCommand(
		newInstanceCreateCmd(flags),
		newInstanceDeleteCmd(flags),
	)

	return cmd
}

func newInstanceCreateCmd(flags *globalFlags) *cobra.Command {
	var (
		nodes    int32
		configID string
	)

	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create a Spanner instance",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			cfg := flags.spannerConfig()

			client, err := spannerutil.NewAdminClient(ctx, cfg)
			if err != nil {
				return err
			}
			defer client.Close()

			displayName := cfg.Instance
			if err := client.CreateInstance(ctx, displayName, configID, nodes); err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "Created instance: %s\n", cfg.InstancePath())
			return nil
		},
	}

	cmd.Flags().Int32Var(&nodes, "nodes", 1, "number of nodes")
	cmd.Flags().StringVar(&configID, "config", "regional-us-central1", "instance configuration ID")

	return cmd
}

func newInstanceDeleteCmd(flags *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "delete",
		Short: "Delete a Spanner instance",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			cfg := flags.spannerConfig()

			client, err := spannerutil.NewAdminClient(ctx, cfg)
			if err != nil {
				return err
			}
			defer client.Close()

			if err := client.DeleteInstance(ctx); err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "Deleted instance: %s\n", cfg.InstancePath())
			return nil
		},
	}
}
