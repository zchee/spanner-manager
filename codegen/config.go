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

package codegen

import (
	"fmt"
	"os"

	yaml "go.yaml.in/yaml/v4"
)

// Config holds YAML configuration for custom type mappings.
type Config struct {
	Tables      []TableConfig `yaml:"tables,omitempty"`
	Inflections []Inflection  `yaml:"inflections,omitempty"`
}

// TableConfig configures code generation for a specific table.
type TableConfig struct {
	Name    string         `yaml:"name"`
	RowName string         `yaml:"row_name,omitempty"`
	Columns []ColumnConfig `yaml:"columns,omitempty"`
}

// ColumnConfig configures code generation for a specific column.
type ColumnConfig struct {
	Name            string   `yaml:"name"`
	CustomType      string   `yaml:"custom_type,omitempty"`
	Imports         []string `yaml:"imports,omitempty"`
	JSONType        string   `yaml:"json_type,omitempty"`
	JSONTypeImports []string `yaml:"json_type_imports,omitempty"`
}

// Inflection defines a custom singular/plural inflection.
type Inflection struct {
	Singular string `yaml:"singular"`
	Plural   string `yaml:"plural"`
}

// LoadConfig reads a YAML config file.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}

	var cfg Config
	if err := yaml.Load(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing config file: %w", err)
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func (c *Config) validate() error {
	for _, table := range c.Tables {
		for _, column := range table.Columns {
			if column.CustomType != "" && column.JSONType != "" {
				return fmt.Errorf("table %q column %q cannot set both custom_type and json_type", table.Name, column.Name)
			}
			if len(column.JSONTypeImports) > 0 && column.JSONType == "" {
				return fmt.Errorf("table %q column %q cannot set json_type_imports without json_type", table.Name, column.Name)
			}
			if len(column.Imports) > 0 && column.CustomType == "" && column.JSONType == "" {
				return fmt.Errorf("table %q column %q cannot set imports without custom_type or json_type", table.Name, column.Name)
			}
		}
	}
	return nil
}
