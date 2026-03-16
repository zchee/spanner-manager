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
	Columns []ColumnConfig `yaml:"columns,omitempty"`
}

// ColumnConfig configures code generation for a specific column.
type ColumnConfig struct {
	Name       string `yaml:"name"`
	CustomType string `yaml:"custom_type,omitempty"`
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

	return &cfg, nil
}
