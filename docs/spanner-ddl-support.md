# Cloud Spanner DDL support matrix

This document is the maintenance contract for schema diff coverage. It is
intentionally conservative: "parsed" means the pinned memefish version can build
an AST node, while "diffed" means `diff.ParseDatabase` records enough semantic
state for `diff.Diff` to generate migration DDL.

Current parser reference:

- `go.mod` pins `github.com/cloudspannerecosystem/memefish`
  `v0.6.3-0.20260220015148-e01a84ded886`.
- `sqlutil.ParseDDLs` delegates to `memefish.ParseDDLs`.
- `diff.ParseDatabase` currently records only the statement families handled
  in its switch. Unknown `ast.DDL` nodes are a correctness gap until production
  fail-loud handling rejects them explicitly.

## Status legend

| Status | Meaning |
|---|---|
| Supported | Parsed into `diff.Database` and diff generation exists. |
| Partial | Parsed or stored, but some alter/drop/equality paths are incomplete. |
| Parse-only gap | memefish parses the DDL, but `diff.ParseDatabase` does not model it. |
| Unsupported | Not intentionally supported by this diff engine. |
| Live validation gap | Local AST output exists, but Cloud Spanner acceptance still needs disposable database validation. |

## Support matrix

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

## Fail-loud policy

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

## Live Spanner validation gaps

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

## Update checklist

When changing DDL support:

1. Add regression tests before production logic changes.
2. Update this matrix in the same change or a preceding docs-only change.
3. Preserve data-safety defaults: no unrequested `DROP TABLE`, `DROP COLUMN`,
   or `DROP SEQUENCE` fallback.
4. Add explicit unsupported errors for newly parsed but unmodeled AST nodes.
5. Run `go test ./diff ./sqlutil -count=1`, `go test ./... -count=1`, and
   `git diff --check`.

