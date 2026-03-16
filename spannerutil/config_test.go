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

package spannerutil

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestConfig_DatabasePath(t *testing.T) {
	tests := map[string]struct {
		config   Config
		expected string
	}{
		"success: standard path": {
			config: Config{
				Project:  "my-project",
				Instance: "my-instance",
				Database: "my-database",
			},
			expected: "projects/my-project/instances/my-instance/databases/my-database",
		},
		"success: empty fields produce empty segments": {
			config:   Config{},
			expected: "projects//instances//databases/",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got := tt.config.DatabasePath()
			if diff := cmp.Diff(tt.expected, got); diff != "" {
				t.Errorf("DatabasePath() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestConfig_InstancePath(t *testing.T) {
	cfg := Config{Project: "p", Instance: "i"}
	want := "projects/p/instances/i"
	if got := cfg.InstancePath(); got != want {
		t.Errorf("InstancePath() = %q, want %q", got, want)
	}
}

func TestConfig_ProjectPath(t *testing.T) {
	cfg := Config{Project: "p"}
	want := "projects/p"
	if got := cfg.ProjectPath(); got != want {
		t.Errorf("ProjectPath() = %q, want %q", got, want)
	}
}

func TestConfig_IsEmulator(t *testing.T) {
	tests := map[string]struct {
		config   Config
		expected bool
	}{
		"success: emulator set": {
			config:   Config{EmulatorHost: "localhost:9010"},
			expected: true,
		},
		"success: emulator not set": {
			config:   Config{},
			expected: false,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if got := tt.config.IsEmulator(); got != tt.expected {
				t.Errorf("IsEmulator() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestConfig_Validate(t *testing.T) {
	tests := map[string]struct {
		config  Config
		wantErr bool
	}{
		"success: all fields set": {
			config: Config{
				Project:  "p",
				Instance: "i",
				Database: "d",
				Timeout:  time.Minute,
			},
			wantErr: false,
		},
		"error: missing project": {
			config: Config{
				Instance: "i",
				Database: "d",
			},
			wantErr: true,
		},
		"error: missing instance": {
			config: Config{
				Project:  "p",
				Database: "d",
			},
			wantErr: true,
		},
		"error: missing database": {
			config: Config{
				Project:  "p",
				Instance: "i",
			},
			wantErr: true,
		},
		"error: all missing": {
			config:  Config{},
			wantErr: true,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestConfig_ValidateInstance(t *testing.T) {
	tests := map[string]struct {
		config  Config
		wantErr bool
	}{
		"success: project and instance set": {
			config: Config{
				Project:  "p",
				Instance: "i",
			},
			wantErr: false,
		},
		"success: database not required": {
			config: Config{
				Project:  "p",
				Instance: "i",
				Database: "d",
			},
			wantErr: false,
		},
		"error: missing project": {
			config: Config{
				Instance: "i",
			},
			wantErr: true,
		},
		"error: missing instance": {
			config: Config{
				Project: "p",
			},
			wantErr: true,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			err := tt.config.ValidateInstance()
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateInstance() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
