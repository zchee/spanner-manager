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

	"github.com/zchee/spanner-manager/codegen"
	"github.com/zchee/spanner-manager/spannerutil"
)

func newGenerateCmd(flags *globalFlags) *cobra.Command {
	var (
		fromDDL         bool
		outDir          string
		packageName     string
		configFile      string
		language        string
		ignoreTables    []string
		includeTables   []string
		suffix          string
		templatePath    string
		singularizeRows bool
		rowSuffix       string
	)

	cmd := &cobra.Command{
		Use:   "generate [SCHEMA_SOURCE]",
		Short: "Generate ORM code from schema",
		Long: `Generate type-safe ORM code from a Cloud Spanner schema.

By default, reads schema from a live Spanner database via INFORMATION_SCHEMA.
Use --from-ddl to parse schema from a DDL file instead.`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

			if outDir == "" {
				return fmt.Errorf("--out flag is required")
			}

			opts := codegen.Options{
				OutDir:          outDir,
				PackageName:     packageName,
				Language:        language,
				IgnoreTables:    ignoreTables,
				IncludeTables:   includeTables,
				Suffix:          suffix,
				TemplatePath:    templatePath,
				SingularizeRows: singularizeRows,
				RowSuffix:       rowSuffix,
			}

			// Load config if specified.
			if configFile != "" {
				cfg, err := codegen.LoadConfig(configFile)
				if err != nil {
					return fmt.Errorf("loading config: %w", err)
				}
				opts.Config = cfg
			}

			var source codegen.SchemaSource
			if fromDDL {
				if len(args) == 0 {
					return fmt.Errorf("schema source file required with --from-ddl")
				}
				source = codegen.NewDDLFileSource(args[0])
			} else {
				cfg := flags.spannerConfig()
				client, err := spannerutil.NewClient(ctx, cfg)
				if err != nil {
					return err
				}
				defer client.Close()
				source = codegen.NewInformationSchemaSource(client)
			}

			gen := codegen.NewGenerator(opts)
			schema, err := source.Load(ctx)
			if err != nil {
				return fmt.Errorf("loading schema: %w", err)
			}

			if err := gen.Generate(schema); err != nil {
				return fmt.Errorf("generating code: %w", err)
			}

			fmt.Fprintf(cmd.OutOrStdout(), "Generated %s code in %s\n", language, outDir)
			return nil
		},
	}

	f := cmd.Flags()
	f.BoolVar(&fromDDL, "from-ddl", false, "parse schema from DDL file instead of live database")
	f.StringVarP(&outDir, "out", "o", "", "output directory (required)")
	f.StringVar(&packageName, "package", "", "Go package name (default: directory name)")
	f.StringVar(&configFile, "config", "", "YAML config file for custom type mappings")
	f.StringVar(&language, "language", "go", "target language")
	f.StringSliceVar(&ignoreTables, "ignore-tables", nil, "tables to skip")
	f.StringSliceVar(&includeTables, "tables", nil, "tables to generate")
	f.StringSliceVar(&includeTables, "include-tables", nil, "alias of --tables")
	f.StringVar(&suffix, "suffix", ".spanner.go", "output file suffix")
	f.BoolVar(&singularizeRows, "singularize-rows", false, "generate singular row type names from plural table names")
	f.StringVar(&rowSuffix, "row-suffix", "", "suffix to append to generated row type names")
	f.StringVar(&templatePath, "template-path", "", "override embedded templates directory")

	return cmd
}
