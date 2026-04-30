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
	"bytes"
	"testing"

	gocmp "github.com/google/go-cmp/cmp"
	"github.com/spf13/cobra"
)

func TestInstanceCommandsRequireCompleteInstanceTarget(t *testing.T) {
	tests := map[string]struct {
		flags *globalFlags
		want  string
	}{
		"error: missing project": {
			flags: &globalFlags{instance: "test-instance"},
			want:  "invalid config: project is required",
		},
		"error: missing instance": {
			flags: &globalFlags{project: "test-project"},
			want:  "invalid config: instance is required",
		},
		"error: missing project and instance": {
			flags: &globalFlags{},
			want:  "invalid config: project is required\ninstance is required",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			commands := map[string]*cobra.Command{
				"create": newInstanceCreateCmd(tt.flags),
				"delete": newInstanceDeleteCmd(tt.flags),
			}

			for commandName, command := range commands {
				t.Run(commandName, func(t *testing.T) {
					var stderr bytes.Buffer
					err := executeDBCommandForTest(t, command, withCommandErr(&stderr))
					if err == nil {
						t.Fatal("command succeeded with incomplete instance target, want validation error")
					}
					if got := err.Error(); got != tt.want {
						t.Fatalf("error mismatch (-want +got):\n%s", gocmp.Diff(tt.want, got))
					}
					if got := stderr.String(); got != "" {
						t.Fatalf("stderr = %q, want no progress before validation succeeds", got)
					}
				})
			}
		})
	}
}
