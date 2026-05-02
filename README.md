# spanner-manager

[![Test](https://github.com/zchee/spanner-manager/actions/workflows/test.yaml/badge.svg)](https://github.com/zchee/spanner-manager/actions/workflows/test.yaml)
[![codecov](https://codecov.io/github/zchee/spanner-manager/graph/badge.svg?token=cpoqSGB5FL)](https://codecov.io/github/zchee/spanner-manager)

Unified CLI for Cloud Spanner schema migration, schema diffing, and Go ORM code generation.

Replaces three separate tools ([wrench](https://github.com/cloudspannerecosystem/wrench), [hammer](https://github.com/daichirata/hammer), [yo](https://github.com/cloudspannerecosystem/yo)) with a single binary, shared connection management, and a common SQL parser powered by [memefish](https://github.com/cloudspannerecosystem/memefish).

## Features

- **Database lifecycle** -- create, drop, reset, truncate, and export DDL
- **Schema diffing** -- AST-based diff between any two sources (files or live databases), producing ready-to-apply ALTER statements
- **Versioned migrations** -- sequential, version-tracked DDL/DML migration files with dirty-state protection
- **Code generation** -- type-safe Go structs with CRUD mutations and primary-key lookups from a live database or DDL file
- **Instance management** -- create and delete Spanner instances
- **Emulator support** -- first-class `SPANNER_EMULATOR_HOST` integration for local development

## Installation

```console
go install github.com/zchee/spanner-manager@latest
```

## Quick Start

### 1. Authenticate for real Cloud Spanner

For production or staging projects, use Application Default Credentials or a service account key:

```console
# Option 1: local development with gcloud
gcloud auth application-default login

# Option 2: explicit service account key
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
```

If you are using the Spanner emulator, skip authentication and set `SPANNER_EMULATOR_HOST` instead.

### 2. Export common connection settings

You can pass flags on every command, but exporting environment variables is less repetitive:

```console
export SPANNER_PROJECT_ID=my-project
export SPANNER_INSTANCE_ID=my-instance
export SPANNER_DATABASE_ID=my-database
```

Then run commands without repeating `-p`, `-i`, and `-d` each time:

```console
spanner-manager db load
spanner-manager migrate version
spanner-manager generate -o ./models
```

### 3. Discover commands and flags

```console
spanner-manager --help
spanner-manager schema --help
spanner-manager schema apply --help
```

## Configuration

All connection parameters can be set via flags or environment variables. Flags take precedence.

| Flag | Short | Env Var | Description |
|---|---|---|---|
| `--project` | `-p` | `SPANNER_PROJECT_ID`, `GOOGLE_CLOUD_PROJECT` | GCP project ID |
| `--instance` | `-i` | `SPANNER_INSTANCE_ID` | Spanner instance ID |
| `--database` | `-d` | `SPANNER_DATABASE_ID` | Spanner database ID |
| `--credentials` | | `GOOGLE_APPLICATION_CREDENTIALS` | Path to service account JSON key |
| `--emulator-host` | | `SPANNER_EMULATOR_HOST` | Emulator address (e.g. `localhost:9010`) |
| `--timeout` | | | Operation timeout (default: `1h`) |

When `--emulator-host` is set, the tool automatically uses insecure gRPC credentials and skips authentication.

### Connection Requirements by Command

Not every command needs a live Spanner connection. The common cases are:

| Command | Project | Instance | Database | Notes |
|---|---|---|---|---|
| `instance create`, `instance delete` | required | required | not used | Creates or deletes an instance |
| `db create`, `db drop`, `db reset`, `db load`, `db truncate` | required | required | required | `db create` and `db reset` can also read a local `--schema` file |
| `migrate up`, `migrate version`, `migrate set` | required | required | required | Reads migration files from `migrations/` under `--directory` |
| `schema apply` | required | required | required | Target database comes from global flags |
| `schema export FILE`, `schema diff FILE1 FILE2` | not needed | not needed | not needed | Works entirely on local DDL files |
| `schema export spanner://...`, `schema diff spanner://... FILE` | required inside URI | required inside URI | required inside URI | Global `--credentials` and `--emulator-host` still apply |
| `generate --from-ddl schema.sql -o ./models` | not needed | not needed | not needed | Generates code without contacting Spanner |
| `generate -o ./models` | required | required | required | Reads schema from `INFORMATION_SCHEMA` |
| `migrate create NAME` | not needed | not needed | not needed | Creates the next file under `migrations/` |

## Usage

### End-to-End Example on Managed Cloud Spanner

This is the fastest way to understand the intended workflow:

```console
# 1. Create an instance once
spanner-manager instance create --config regional-us-central1 --nodes 1

# 2. Create the database with an initial schema
spanner-manager db create --schema schema.sql

# 3. Export the current DDL for review or version control
spanner-manager db load --output current.sql

# 4. Compare the current schema with a desired schema file
spanner-manager schema diff current.sql desired.sql

# 5. Apply the desired schema to the live database
spanner-manager schema apply desired.sql

# 6. Create and apply versioned migrations for incremental changes
spanner-manager migrate create add_users_table
spanner-manager migrate up

# 7. Generate Go models from the live database
spanner-manager generate -o ./models
```

If you prefer to inspect the live database directly during diffing, use a `spanner://` URI:

```console
spanner-manager schema diff \
  spanner://projects/my-project/instances/my-instance/databases/my-database \
  desired.sql
```

### `db` -- Database Management

```console
# Create a database
spanner-manager db create -p my-project -i my-instance -d my-db

# Create with initial schema
spanner-manager db create -p my-project -i my-instance -d my-db --schema schema.sql

# Drop a database (irreversible)
spanner-manager db drop -p my-project -i my-instance -d my-db --force

# Drop and recreate (useful for dev reset)
spanner-manager db reset -p my-project -i my-instance -d my-db --schema schema.sql --force

# Truncate all tables (preserves schema and SchemaMigrations table)
# Respects interleave order: child tables are deleted before parents
spanner-manager db truncate -p my-project -i my-instance -d my-db --force

# Export current DDL to stdout
spanner-manager db load -p my-project -i my-instance -d my-db

# Export current DDL to file
spanner-manager db load -p my-project -i my-instance -d my-db --output schema.sql
```

Use `db create` or `db reset` when you already have a canonical DDL file. Use `db load` when you want to snapshot the live schema before a diff or commit it back to version control.

### `schema` -- Schema Diff and Apply

Schema sources can be a local DDL file path or a `spanner://` URI:

```
spanner://projects/{PROJECT}/instances/{INSTANCE}/databases/{DATABASE}
```

```console
# Export DDL from a source
spanner-manager schema export schema.sql
spanner-manager schema export spanner://projects/my-project/instances/my-instance/databases/my-db

# Diff two schemas -- outputs ALTER DDL to migrate from SOURCE1 to SOURCE2
spanner-manager schema diff old.sql new.sql
spanner-manager schema diff spanner://projects/P/instances/I/databases/D desired.sql

# Apply: diff current database against desired DDL, then execute the changes
spanner-manager schema apply desired.sql -p my-project -i my-instance -d my-db
```

Recommended review flow before applying changes:

```console
# Inspect the exact ALTER statements first
spanner-manager schema diff \
  spanner://projects/my-project/instances/my-instance/databases/my-db \
  desired.sql

# Apply only after the diff looks correct
spanner-manager schema apply desired.sql -p my-project -i my-instance -d my-db
```

#### Cloud Spanner DDL support matrix

This section is the maintenance contract for schema diff coverage. It is
intentionally conservative: "parsed" means the pinned memefish version can build
an AST node, while "diffed" means `diff.ParseDatabase` records enough semantic
state for `diff.Diff` to generate migration DDL.

Current parser reference:

- `go.mod` pins `github.com/cloudspannerecosystem/memefish`
  `v0.6.3-0.20260429070454-64f857b2c61e`.
- `sqlutil.ParseDDLs` delegates to `memefish.ParseDDLs`.
- `diff.ParseDatabase` currently records only the statement families handled
  in its switch. Unknown `ast.DDL` nodes are a correctness gap until production
  fail-loud handling rejects them explicitly.

##### Status legend

| Status | Meaning |
|---|---|
| Supported | Parsed into `diff.Database` and diff generation exists. |
| Partial | Parsed or stored, but some alter/drop/equality paths are incomplete. |
| Parse-only gap | memefish parses the DDL, but `diff.ParseDatabase` does not model it. |
| Unsupported | Not intentionally supported by this diff engine. |
| Live validation gap | Local AST output exists, but Cloud Spanner acceptance still needs disposable database validation. |

##### Support matrix

| DDL family | memefish parse | ParseDatabase model | Diff output | Notes and guardrails |
|---|---:|---:|---:|---|
| Schemas: `CREATE SCHEMA`, `DROP SCHEMA` | Yes | Supported | Partial | Creates are emitted before tables and drops after removed objects. Schema option changes are not modeled. |
| Database options: `ALTER DATABASE ... SET OPTIONS` | Yes | Partial | Partial | Database options are stored and diffed, but only as whole option-set changes. Validate option-specific semantics before broadening. |
| Tables: `CREATE TABLE`, `DROP TABLE` | Yes | Supported | Supported | Drop/create is destructive. Default policy must remain conservative: do not use table recreation as a fallback for uncertain changes. |
| Table columns: add/drop/default/generated/options/type/nullability | Yes | Supported | Partial | Add/drop and selected alter paths exist. Data-preserving ALTER is preferred; unknown conversions must fail loudly or require an explicit destructive mode. |
| Primary keys and interleave clauses | Yes | Supported | Partial | Primary-key changes and parent interleave changes are high risk. Do not silently recreate tables for these without explicit destructive-policy documentation. `SET ON DELETE` and `SET INTERLEAVE` need focused validation. |
| Table constraints: named FK/check constraints | Yes | Supported | Partial | Constraint add/drop exists. Named constraints should be tracked by table plus constraint name, not raw SQL formatting, to avoid skipped recreates. |
| Row deletion policies | Yes | Supported | Supported | Create, replace, and drop row deletion policy diffs are modeled. Keep behavior covered because it changes data retention. |
| Secondary indexes | Yes | Supported | Supported | Create/drop and storing-column ALTER paths exist. Changed index definitions may require drop/recreate; this is destructive to index state but not table data. |
| Search indexes | Yes | Supported | Partial | Create/drop is modeled. Changed definitions generally drop and recreate the search index. Live validation is needed for newer search-index options. |
| Vector indexes | Yes | Supported | Partial | Create/drop is modeled. Changed definitions generally drop and recreate the vector index. Live validation is needed for vector option semantics. |
| Change streams | Yes | Supported | Partial | Create/drop and selected ALTER paths are modeled. Table-scoped stream rewrites must preserve grants and avoid invalid ordering. |
| Sequences | Yes | Supported | Partial | Create/drop exists. Changed sequence options must use `ALTER SEQUENCE` where safe; drop/recreate loses sequence state and must not be the default fallback. |
| Views | Yes | Supported | Partial | Create/drop and replace exist. Unchanged views should produce no statement; changed views should use `CREATE OR REPLACE VIEW` only when definitions differ semantically. |
| Roles | Yes | Supported | Supported | Create/drop is modeled. Role drops must account for grants. |
| Grants and revokes | Yes | Partial | Partial | Grants are modeled; missing grants emit `REVOKE`. `REVOKE` input statements themselves are not modeled as desired-state objects. |
| Table synonyms | Yes | Parse-only gap | Unsupported | memefish parses table synonyms in `CREATE TABLE` and `ALTER TABLE ADD/DROP SYNONYM`; `diff.Database` does not model them. They should fail loudly instead of being ignored. |
| Proto bundles | Yes | Parse-only gap | Unsupported | `CREATE/ALTER/DROP PROTO BUNDLE` AST nodes exist in memefish, but the diff engine does not model proto bundle state. |
| Locality groups | Yes | Parse-only gap | Unsupported | `CREATE/ALTER/DROP LOCALITY GROUP` parse in the pinned memefish version, but are not represented in `diff.Database`. |
| Placements | Yes | Parse-only gap | Unsupported | `CREATE PLACEMENT` parses, but no diff model exists. Drop/alter support must be checked against current Spanner docs before implementation. |
| Models | Yes | Parse-only gap | Unsupported | `CREATE/ALTER/DROP MODEL` parse in memefish, but model state is not represented or diffed. Treat as unsupported until a full semantic model exists. |
| Property graphs | Yes | Parse-only gap | Unsupported | `CREATE/DROP PROPERTY GRAPH` parse in memefish, but the diff engine does not model graph definitions or dependencies. |
| Functions | Yes | Parse-only gap | Unsupported | `CREATE/DROP FUNCTION` parse in memefish, but no desired-state or dependency model exists. |
| Statistics/analyze statements | Yes | Parse-only gap | Unsupported | `ALTER STATISTICS` and `ANALYZE` are operational/statistics statements, not schema desired state for this diff engine. |
| Other future memefish `ast.DDL` nodes | Maybe | Unsupported | Unsupported | New AST types must be added to this matrix and either modeled or rejected explicitly. |

##### Fail-loud policy

The diff engine must not silently ignore parsed DDL that affects schema state.
When memefish accepts a DDL statement but `diff.ParseDatabase` cannot model it,
the safe behavior is an explicit unsupported-statement error that includes the
AST type and enough SQL context for the user to remove, split, or manually
manage the object.

Fail loud for these cases:

1. Parsed statement families not represented in `diff.Database`, such as proto
   bundles, models, property graphs, locality groups, placements, functions, and
   table synonyms.
2. Parsed table alterations that are not yet modeled, such as synonym changes,
   table renames, table options, interleave mutations, and column identity
   changes.
3. Diff changes with no verified data-preserving ALTER path, including unknown
   column type conversions, primary-key changes, and unsupported interleave
   changes.
4. Dependency-sensitive destructive changes when ordering cannot be proven safe,
   such as dropping sequences referenced by defaults or identity columns.

A future destructive mode may be added, but it must be explicit, non-default,
and documented at the command boundary. The default diff path should prefer:

1. safe ALTER statements,
2. explicit drop statements only for objects removed from the target schema, and
3. unsupported errors for uncertain or lossy transformations.

##### Live Spanner validation gaps

Unit tests and memefish SQL rendering prove local AST behavior, not complete
Cloud Spanner acceptance. Use a disposable Cloud Spanner database, or an emulator
only when the emulator supports the syntax, before documenting any of these as
fully supported:

- `ALTER SEQUENCE` option changes, including skip-range behavior and whether
  existing generated values are unaffected.
- `ALTER TABLE ... SET ON DELETE` and any `SET INTERLEAVE IN PARENT` path.
- Column type, default, generated-column, and identity alterations that preserve
  data.
- Search and vector index option changes.
- Model, property graph, placement, and locality group DDL, because availability
  can depend on product release stage and database configuration.
- Drop restrictions for sequences, change streams, grants, and dependent indexes.

Record live validation evidence in the PR or release notes with the exact DDL,
database environment, and pass/fail result.

##### Update checklist

When changing DDL support:

1. Add regression tests before production logic changes.
2. Update this matrix in the same change or a preceding docs-only change.
3. Preserve data-safety defaults: no unrequested `DROP TABLE`, `DROP COLUMN`,
   or `DROP SEQUENCE` fallback.
4. Add explicit unsupported errors for newly parsed but unmodeled AST nodes.
5. Run `go test ./diff ./sqlutil -count=1`, `go test ./... -count=1`, and
   `git diff --check`.

#### Diff execution phases

The diff engine operates in phases to ensure correctness:

1. Drop indexes that no longer exist or have changed
2. Drop removed tables (children before parents)
3. Alter existing tables (add/drop/modify columns)
4. Create new tables (parents before children)
5. Create new or changed indexes
6. Handle change streams

For `schema apply`, DML statements (e.g. backfilling NULLs before a `NOT NULL` constraint) are executed before DDL.

### `migrate` -- Versioned Migrations

Migration files live in a `migrations/` subdirectory with the naming convention:

```
{VERSION}_{NAME}.sql        e.g. 000001_create_users.sql
{VERSION}_{NAME}.up.sql     e.g. 000002_add_email.up.sql
```

All statements in a single migration file must be the same kind (DDL, DML, or partitioned DML). Use a `-- PARTITIONED_DML` or `/* PARTITIONED_DML */` comment prefix to mark partitioned DML:

```sql
-- PARTITIONED_DML
UPDATE Users SET active = true WHERE active IS NULL;
```

```console
# Create a new migration file (auto-increments version)
spanner-manager migrate create add_users_table
# -> migrations/000001_add_users_table.sql

# Apply all pending migrations
spanner-manager migrate up -p my-project -i my-instance -d my-db

# Apply up to N pending migrations
spanner-manager migrate up 3 -p my-project -i my-instance -d my-db

# Check current migration version
spanner-manager migrate version -p my-project -i my-instance -d my-db

# Force-set version (use to recover from a dirty state)
spanner-manager migrate set 5 -p my-project -i my-instance -d my-db

# Use a custom directory (looks for migrations/ inside it)
spanner-manager migrate --directory ./db up -p my-project -i my-instance -d my-db
```

Migrations are tracked in a `SchemaMigrations` table (created automatically):

```sql
CREATE TABLE SchemaMigrations (
  Version INT64 NOT NULL,
  Dirty   BOOL NOT NULL,
) PRIMARY KEY (Version)
```

If a migration fails mid-execution, the version is marked dirty. Fix the issue, then run `migrate set` to clear the dirty flag before retrying.

### `generate` -- Go Code Generation

Generates type-safe Go structs with Spanner column tags, CRUD mutation methods
(`Insert`, `Update`, `InsertOrUpdate`, `Delete`), commit timestamp-aware helper
methods, and `FindByPrimaryKey` functions.

```console
# Generate from a live database
spanner-manager generate -o ./models -p my-project -i my-instance -d my-db

# Generate from a DDL file (no database connection needed)
spanner-manager generate --from-ddl schema.sql -o ./models

# Full example with all options
spanner-manager generate --from-ddl schema.sql -o ./models \
  --package models \
  --tables Runs,Projects \
  --singularize-rows \
  --row-suffix Row \
  --suffix .gen.go \
  --ignore-tables SchemaMigrations \
  --config codegen.yaml
```

Generation writes a shared `spanner_db` helper and one file per table into the output directory. The `header.go.tmpl` template is used as the header section of each generated file rather than producing a standalone `spanner_header` file. This is useful when you want generated models checked into the repository.

| Flag | Description |
|---|---|
| `-o, --out` | Output directory (required) |
| `--from-ddl` | Parse schema from DDL file instead of live database |
| `--package` | Go package name (default: output directory name) |
| `--language` | Target language (default: `go`) |
| `--config` | YAML config file for custom type mappings |
| `--tables` | Comma-separated or repeated table names to generate |
| `--ignore-tables` | Comma-separated or repeated table names to skip |
| `--singularize-rows` | Generate singular row type names from plural table names |
| `--row-suffix` | Append a suffix to generated row type names |
| `--suffix` | Output file suffix (default: `.spanner.go`) |
| `--template-path` | Override embedded templates directory |

#### Codegen Config

The `--config` YAML file supports custom type mappings, typed JSON helpers, row
name overrides, and inflections:

```yaml
tables:
  - name: Users
    row_name: UserRow
    columns:
      - name: metadata
        json_type: "ProjectMetadata"
        json_type_imports:
          - "github.com/acme/project/models"
      - name: raw_payload
        custom_type: "json.RawMessage"
        imports:
          - "encoding/json"
      - name: role
        custom_type: "UserRole"
        imports:
          - "github.com/acme/project/roles"

inflections:
  - singular: "status"
    plural: "statuses"
```

Use `json_type` for columns whose underlying Spanner type is `STRING` or `JSON`
but whose payload should be exposed as a typed Go value. `json_type_imports`
uses the same import syntax as `imports`, and import aliases can be expressed as
`alias=github.com/acme/project/pkg`.

#### Spanner Type Mapping

| Spanner Type | Go Type (NOT NULL) | Go Type (nullable) |
|---|---|---|
| `BOOL` | `bool` | `spanner.NullBool` |
| `INT64` | `int64` | `spanner.NullInt64` |
| `FLOAT32` | `float32` | `spanner.NullFloat32` |
| `FLOAT64` | `float64` | `spanner.NullFloat64` |
| `STRING` | `string` | `spanner.NullString` |
| `BYTES` | `[]byte` | `[]byte` |
| `DATE` | `civil.Date` | `spanner.NullDate` |
| `TIMESTAMP` | `time.Time` | `spanner.NullTime` |
| `NUMERIC` | `big.Rat` | `spanner.NullNumeric` |
| `JSON` | `spanner.NullJSON` | `spanner.NullJSON` |

Array columns are mapped to Go slices when the element type is known. For
example, `ARRAY<STRING(MAX)>` becomes `[]string` and `ARRAY<TIMESTAMP>` becomes
`[]time.Time`.

### `instance` -- Instance Management

```console
# Create a Spanner instance
spanner-manager instance create -p my-project -i my-instance \
  --nodes 1 --config regional-us-central1

# Delete a Spanner instance
spanner-manager instance delete -p my-project -i my-instance
```

### `version`

```console
spanner-manager version
```

## Emulator Workflow

```console
# Start the Spanner emulator
docker run --rm -p 9010:9010 -p 9020:9020 gcr.io/cloud-spanner-emulator/emulator

# Point spanner-manager at the emulator
export SPANNER_EMULATOR_HOST=localhost:9010
export SPANNER_PROJECT_ID=test-project
export SPANNER_INSTANCE_ID=test-instance
export SPANNER_DATABASE_ID=test-db

# Create the emulator instance first
spanner-manager instance create --config emulator-config --nodes 1

# Create the database and load a schema
spanner-manager db create --schema schema.sql

# Inspect or update schema just like a real Spanner database
spanner-manager db load
spanner-manager schema apply desired.sql

# Run migrations and generate code
spanner-manager migrate up
spanner-manager generate -o ./models
```

The emulator does not require credentials. The `emulator-config` instance config is the expected config name for local emulator instances in Cloud Spanner tooling.

## Architecture

```
spanner-manager
├── cmd/           CLI command definitions (cobra)
├── codegen/       Go ORM code generation from schema
├── diff/          AST-based schema comparison engine
├── migration/     Versioned migration reader and executor
├── spannerutil/   Spanner client wrapper (admin + data plane)
└── sqlutil/       SQL parsing utilities (memefish wrapper)
```

## License

[Apache License 2.0](LICENSE)
