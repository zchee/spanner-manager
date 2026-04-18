# Index Metadata Parity Review

This document records the implementation and verification outcome for the index-metadata prerequisite work described in `.omx/context/fix-index-metadata-prereqs-20260418T102135Z.md`.

## Goal

Preserve the current generated Go contract:

- CRUD mutations
- `FindByPrimaryKey`
- `SpannerDB` as `ReadRow + Query`

while improving internal index metadata parity between live INFORMATION_SCHEMA loading and DDL loading.

## What landed

### Schema model

- `codegen/schema.go`
  - `IndexInfo` now carries:
    - `Name`
    - `FuncName`
    - `Fields []Field` for ordered key-field resolution
    - `KeyColumns []IndexKeyColumn`
    - `StoringColumns []string`
    - `IsUnique`
  - `IndexKeyColumn` records:
    - `ColumnName`
    - `OrdinalPosition`
    - `Desc`

This keeps metadata explicit without widening the generated public API.

### Live schema loading parity

- `codegen/loader.go`
  - `InformationSchemaSource.loadType` still loads table columns, primary keys, and commit timestamp metadata.
  - It now also loads index metadata from `INFORMATION_SCHEMA.INDEX_COLUMNS`.
  - Key-column rows are mapped into `IndexInfo.KeyColumns`.
  - Stored columns are mapped into `IndexInfo.StoringColumns`.
  - Ordered key fields are resolved into `IndexInfo.Fields`.

### DDL schema loading parity

- `codegen/loader.go`
  - `DDLFileSource.Load` still parses `CREATE TABLE` definitions into `Type` values.
  - It now performs a second pass for `CREATE INDEX` statements.
  - Each parsed index is attached to the owning table with:
    - ordered key-column metadata,
    - ordered resolved key fields,
    - uniqueness,
    - `STORING (...)` columns when present.

### Generated surface stays unchanged

- `codegen/languages/go/templates/type.go.tmpl`
  - Still generates only:
    - row struct,
    - columns helper,
    - primary key helper,
    - CRUD mutations,
    - `Find<Type>ByPrimaryKey`,
    - commit timestamp helpers.
- `codegen/languages/go/templates/spanner_db.go.tmpl`
  - Still exposes only:
    - `ReadRow`
    - `Query`
- `README.md`
  - Now explicitly documents that live and DDL generation keep the same public codegen surface and that index metadata parity does not add generated index helpers or query-builder APIs.

## Scope guardrails that were preserved

This slice does **not** introduce:

- generated `FindBy<Index>` helpers,
- generated `ListBy<Index>` helpers,
- query builders,
- pagination helpers,
- repository/runtime abstractions,
- `SpannerDB` interface expansion.

## Tests and verification

### Loader coverage

- `codegen/loader_test.go`
  - verifies DDL index loading for:
    - unique indexes,
    - ordered key columns,
    - `STORING` metadata,
    - key-field resolution,
    - missing-column failure paths.

### Generator regression coverage

- `codegen/generator_test.go`
  - verifies that DDLs containing indexes still generate only:
    - the shared header,
    - the shared `spanner_db` helper,
    - the per-table row file.
  - explicitly checks that generated output does **not** contain:
    - `ReadUsingIndex`,
    - `Find<Type>By<Index>`,
    - `List<Type>By<Index>`,
    - query-builder APIs.
  - compiles the generated code to confirm the unchanged surface remains valid.

### Command evidence

- `go test ./codegen` → PASS
- `go vet ./codegen` → PASS
- `go test ./...` → PASS
- `golangci-lint run ./codegen` → PASS
- `git diff --check` → PASS

## Review note

The `diff` package remains a useful reference for broader DDL semantics, but this prerequisite slice intentionally keeps codegen metadata narrow and internal. That keeps the change reviewable, maintains source parity, and avoids committing the project to any generated index-helper direction prematurely.
