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

// Package sqlutil provides SQL parsing utilities wrapping the memefish Spanner SQL parser.
package sqlutil

import (
	"strings"

	"github.com/cloudspannerecosystem/memefish"
	"github.com/cloudspannerecosystem/memefish/ast"
)

// ParseDDLs parses a SQL string containing multiple DDL statements and returns the AST nodes.
func ParseDDLs(sql string) ([]ast.DDL, error) {
	return memefish.ParseDDLs("", sql)
}

// ParseStatements parses a SQL string containing multiple statements of any kind.
func ParseStatements(sql string) ([]ast.Statement, error) {
	return memefish.ParseStatements("", sql)
}

// SplitStatements splits raw SQL text at semicolons and returns the individual statement strings.
// Unlike ParseStatements, this does not fully parse the SQL — it only splits on statement boundaries.
func SplitStatements(sql string) ([]string, error) {
	raw, err := memefish.SplitRawStatements("", sql)
	if err != nil {
		return nil, err
	}

	stmts := make([]string, 0, len(raw))
	for _, r := range raw {
		s := strings.TrimSpace(r.Statement)
		if s != "" {
			stmts = append(stmts, r.Statement)
		}
	}
	return stmts, nil
}
