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
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/zchee/spanner-manager/spannerutil"
)

func TestParseSpannerURI(t *testing.T) {
	tests := map[string]struct {
		uri      string
		expected spannerutil.Config
		wantErr  bool
	}{
		"success: valid URI": {
			uri: "spanner://projects/my-project/instances/my-instance/databases/my-database",
			expected: spannerutil.Config{
				Project:  "my-project",
				Instance: "my-instance",
				Database: "my-database",
			},
		},
		"error: missing prefix": {
			uri:     "projects/my-project/instances/my-instance/databases/my-database",
			wantErr: true,
		},
		"error: wrong format - missing databases segment": {
			uri:     "spanner://projects/my-project/instances/my-instance",
			wantErr: true,
		},
		"error: wrong format - wrong segments": {
			uri:     "spanner://projects/my-project/wrong/my-instance/databases/my-database",
			wantErr: true,
		},
		"error: empty URI": {
			uri:     "",
			wantErr: true,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := parseSpannerURI(tt.uri)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseSpannerURI() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr {
				if diff := cmp.Diff(tt.expected, got); diff != "" {
					t.Errorf("parseSpannerURI() mismatch (-want +got):\n%s", diff)
				}
			}
		})
	}
}
