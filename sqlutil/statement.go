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

package sqlutil

import (
	"fmt"
	"strings"

	"github.com/cloudspannerecosystem/memefish"
	"github.com/cloudspannerecosystem/memefish/ast"
)

// StatementKind represents the type of a SQL statement.
type StatementKind int

const (
	// KindDDL represents a Data Definition Language statement (CREATE, ALTER, DROP).
	KindDDL StatementKind = iota

	// KindDML represents a Data Manipulation Language statement (INSERT, UPDATE, DELETE).
	KindDML

	// KindPartitionedDML represents a partitioned DML statement.
	// These are UPDATE or DELETE statements that operate on entire tables
	// and are executed as partitioned operations for large-scale changes.
	KindPartitionedDML
)

// String returns a human-readable name for the statement kind.
func (k StatementKind) String() string {
	switch k {
	case KindDDL:
		return "DDL"
	case KindDML:
		return "DML"
	case KindPartitionedDML:
		return "PartitionedDML"
	default:
		return fmt.Sprintf("StatementKind(%d)", k)
	}
}

// ClassifyStatement determines the kind of a SQL statement by parsing it with memefish.
func ClassifyStatement(sql string) (StatementKind, error) {
	stmt, err := memefish.ParseStatement("", sql)
	if err != nil {
		return 0, fmt.Errorf("parsing statement: %w", err)
	}

	switch stmt.(type) {
	case ast.DDL:
		return KindDDL, nil
	case ast.DML:
		if isPartitionedDML(sql) {
			return KindPartitionedDML, nil
		}
		return KindDML, nil
	default:
		return 0, fmt.Errorf("unsupported statement type: %T", stmt)
	}
}

// IsDDL reports whether the given SQL string is a DDL statement.
func IsDDL(sql string) bool {
	kind, err := ClassifyStatement(sql)
	return err == nil && kind == KindDDL
}

// IsDML reports whether the given SQL string is a DML statement (including partitioned DML).
func IsDML(sql string) bool {
	kind, err := ClassifyStatement(sql)
	return err == nil && (kind == KindDML || kind == KindPartitionedDML)
}

// isPartitionedDML detects whether a DML statement should be executed as partitioned DML.
// Partitioned DML is indicated by a "-- PARTITIONED_DML" or "/* PARTITIONED_DML */" comment prefix.
func isPartitionedDML(sql string) bool {
	s := strings.TrimSpace(sql)
	return strings.HasPrefix(s, "-- PARTITIONED_DML") || strings.HasPrefix(s, "/* PARTITIONED_DML */")
}
