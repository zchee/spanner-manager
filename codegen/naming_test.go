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

import "testing"

func TestSingularizeIdentifier(t *testing.T) {
	tests := map[string]struct {
		name        string
		inflections []Inflection
		want        string
	}{
		"uncountable series": {
			name: "Series",
			want: "Series",
		},
		"canvas remains canvas": {
			name: "Canvas",
			want: "Canvas",
		},
		"irregular analyses": {
			name: "Analyses",
			want: "Analysis",
		},
		"camel case suffix": {
			name: "FactoryControls",
			want: "FactoryControl",
		},
		"custom inflection": {
			name: "UserSprocki",
			inflections: []Inflection{
				{Singular: "sprocket", Plural: "sprocki"},
			},
			want: "UserSprocket",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if got := singularizeIdentifier(tt.name, tt.inflections); got != tt.want {
				t.Fatalf("singularizeIdentifier(%q) = %q, want %q", tt.name, got, tt.want)
			}
		})
	}
}
