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

// Package codegen implements ORM code generation from Cloud Spanner schemas.
package codegen

// Schema represents a parsed database schema for code generation.
type Schema struct {
	Types []Type
}

// Type represents a table type for code generation.
type Type struct {
	Name             string
	Table            string
	Fields           []Field
	PrimaryKeyFields []Field
	Indexes          []IndexInfo
}

// Field represents a column field in a generated type.
type Field struct {
	Name         string
	ColumnName   string
	GoType       string
	SpannerType  string
	NotNull      bool
	IsGenerated  bool
	IsPrimaryKey bool
}

// IndexInfo represents an index for code generation.
type IndexInfo struct {
	Name     string
	FuncName string
	Fields   []Field
	IsUnique bool
}
