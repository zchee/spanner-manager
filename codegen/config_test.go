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
	"os"
	"path/filepath"
	"strings"
	"testing"

	gocmp "github.com/google/go-cmp/cmp"
)

func TestLoadConfig(t *testing.T) {
	tests := map[string]struct {
		content string
		want    *Config
		wantErr string
	}{
		"success: loads table column and inflection settings": {
			content: `
tables:
  - name: Users
    row_name: Account
    columns:
      - name: Profile
        custom_type: example.com/project.Profile
        imports:
          - example.com/project
      - name: Payload
        json_type: example.com/project.Payload
        json_type_imports:
          - payloads=example.com/project/payloads
inflections:
  - singular: person
    plural: people
`,
			want: &Config{
				Tables: []TableConfig{{
					Name:    "Users",
					RowName: "Account",
					Columns: []ColumnConfig{
						{
							Name:       "Profile",
							CustomType: "example.com/project.Profile",
							Imports:    []string{"example.com/project"},
						},
						{
							Name:            "Payload",
							JSONType:        "example.com/project.Payload",
							JSONTypeImports: []string{"payloads=example.com/project/payloads"},
						},
					},
				}},
				Inflections: []Inflection{{
					Singular: "person",
					Plural:   "people",
				}},
			},
		},
		"success: imports are allowed with json type": {
			content: `
tables:
  - name: Events
    columns:
      - name: Payload
        json_type: example.com/project.Payload
        imports:
          - example.com/project
`,
			want: &Config{
				Tables: []TableConfig{{
					Name: "Events",
					Columns: []ColumnConfig{{
						Name:     "Payload",
						JSONType: "example.com/project.Payload",
						Imports:  []string{"example.com/project"},
					}},
				}},
			},
		},
		"error: invalid yaml is wrapped": {
			content: "tables: [",
			wantErr: "parsing config file:",
		},
		"error: custom type and json type are mutually exclusive": {
			content: `
tables:
  - name: Users
    columns:
      - name: Profile
        custom_type: Profile
        json_type: ProfileJSON
`,
			wantErr: `table "Users" column "Profile" cannot set both custom_type and json_type`,
		},
		"error: json type imports require json type": {
			content: `
tables:
  - name: Users
    columns:
      - name: Profile
        json_type_imports:
          - example.com/project
`,
			wantErr: `table "Users" column "Profile" cannot set json_type_imports without json_type`,
		},
		"error: imports require custom type or json type": {
			content: `
tables:
  - name: Users
    columns:
      - name: Profile
        imports:
          - example.com/project
`,
			wantErr: `table "Users" column "Profile" cannot set imports without custom_type or json_type`,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "codegen.yaml")
			if err := os.WriteFile(path, []byte(tt.content), 0o600); err != nil {
				t.Fatalf("writing test config: %v", err)
			}

			got, err := LoadConfig(path)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("LoadConfig() error = nil, want substring %q", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("LoadConfig() error = %q, want substring %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("LoadConfig() unexpected error = %v", err)
			}
			if diff := gocmp.Diff(tt.want, got); diff != "" {
				t.Fatalf("LoadConfig() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestLoadConfig_MissingFile(t *testing.T) {
	_, err := LoadConfig(filepath.Join(t.TempDir(), "missing.yaml"))
	if err == nil {
		t.Fatal("LoadConfig() error = nil, want missing-file error")
	}
	if !strings.Contains(err.Error(), "reading config file:") {
		t.Fatalf("LoadConfig() error = %q, want reading config file wrapper", err)
	}
}
