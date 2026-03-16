# spanner-manager

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

## Usage

### `db` -- Database Management

```console
# Create a database
spanner-manager db create -p my-project -i my-instance -d my-db

# Create with initial schema
spanner-manager db create -p my-project -i my-instance -d my-db --schema schema.sql

# Drop a database (irreversible)
spanner-manager db drop -p my-project -i my-instance -d my-db

# Drop and recreate (useful for dev reset)
spanner-manager db reset -p my-project -i my-instance -d my-db --schema schema.sql

# Truncate all tables (preserves schema and SchemaMigrations table)
# Respects interleave order: child tables are deleted before parents
spanner-manager db truncate -p my-project -i my-instance -d my-db

# Export current DDL to stdout
spanner-manager db load -p my-project -i my-instance -d my-db

# Export current DDL to file
spanner-manager db load -p my-project -i my-instance -d my-db --output schema.sql
```

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

Generates type-safe Go structs with Spanner column tags, CRUD mutation methods (`Insert`, `Update`, `InsertOrUpdate`, `Delete`), and `FindByPrimaryKey` functions.

```console
# Generate from a live database
spanner-manager generate -o ./models -p my-project -i my-instance -d my-db

# Generate from a DDL file (no database connection needed)
spanner-manager generate --from-ddl schema.sql -o ./models

# Full example with all options
spanner-manager generate --from-ddl schema.sql -o ./models \
  --package models \
  --suffix .gen.go \
  --ignore-tables SchemaMigrations \
  --config codegen.yaml
```

| Flag | Description |
|---|---|
| `-o, --out` | Output directory (required) |
| `--from-ddl` | Parse schema from DDL file instead of live database |
| `--package` | Go package name (default: output directory name) |
| `--language` | Target language (default: `go`) |
| `--config` | YAML config file for custom type mappings |
| `--ignore-tables` | Comma-separated tables to skip |
| `--suffix` | Output file suffix (default: `.yo.go`) |
| `--template-path` | Override embedded templates directory |

#### Codegen Config

The `--config` YAML file supports custom type mappings and inflections:

```yaml
tables:
  - name: Users
    columns:
      - name: metadata
        custom_type: "json.RawMessage"
      - name: role
        custom_type: "UserRole"

inflections:
  - singular: "status"
    plural: "statuses"
```

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
docker run -p 9010:9010 -p 9020:9020 gcr.io/cloud-spanner-emulator/emulator

# Use spanner-manager with the emulator
export SPANNER_EMULATOR_HOST=localhost:9010
spanner-manager db create -p test-project -i test-instance -d test-db --schema schema.sql
spanner-manager migrate up -p test-project -i test-instance -d test-db
spanner-manager generate --from-ddl schema.sql -o ./models
```

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
