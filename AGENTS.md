# Repository Guidelines

`spanner-manager` is a Go CLI that unifies Cloud Spanner schema migration, schema diffing, and Go ORM code generation behind a single binary built on `cobra` and `memefish`.

## Project Structure & Module Organization

- `main.go` — entrypoint; wires signal handling and calls `cmd.Execute`.
- `cmd/` — Cobra command tree (`db`, `migrate`, `schema`, `generate`, `instance`, `version`) and global flag plumbing.
- `codegen/` — Go ORM generator. `languages/go/` holds embedded templates; `testdata/` carries golden DDL and expected output.
- `diff/` — AST-based desired-vs-current schema diff engine (`Database`, `Diff`).
- `migration/` — Versioned migration reader, executor, and `SchemaMigrations` bookkeeping.
- `spannerutil/` — Admin/data-plane client wrappers and connection config.
- `sqlutil/` — Thin `memefish` wrapper for parsing DDL and splitting statements.
- `vendor/` — Checked-in dependencies; do not edit by hand.
- `.github/workflows/` — CI (`test.yaml`, `release.yaml`) and `CODEOWNERS`.
- `README.md` — User-facing docs; the **DDL support matrix** there is the maintenance contract for diff coverage and must be updated alongside engine changes.

## Build, Test, and Development Commands

- `go build ./...` — compile every package; the resulting binary lives at `./spanner-manager` after `go build .`.
- `go test ./... -count=1` — run the full unit suite (no caching).
- `go test -race -count=1 -shuffle=on -cover -coverpkg=./... ./...` — mirrors CI; use before pushing.
- `go test ./diff ./sqlutil -count=1` — required after touching the diff/parse path.
- `go vet ./...` — must pass; CI fails otherwise.
- `gofumpt -w -extra .` — required formatter (`mvdan.cc/gofumpt v0.9.2` is pinned in `go.mod`).
- `docker build -t spanner-manager .` — produce the distroless image declared in `Dockerfile`.

## Coding Style & Naming Conventions

- Go 1.26; tabs for indentation; identifiers in `CamelCase` / `camelCase`, packages lowercase single-word.
- Use `any` over `interface{}`; prefer generics where they reduce duplication.
- Every Go file starts with the `Copyright 2026 The spanner-manager Authors.` Apache 2.0 header.
- Godoc comments end with a period and name the symbol they document.
- Prefer standard library; reach for third-party packages only when behavior or performance demands it.
- Keep diff and codegen logic free of global mutable state — recent fixes specifically removed leakage between runs.

## Testing Guidelines

- Framework: standard `testing`. Use `github.com/google/go-cmp/cmp` (aliased `gocmp`) for assertions; do not introduce `testify`.
- Use `t.Context()` instead of `context.Background()`.
- Table tests **must** use `tests := map[string]struct{...}{...}` with descriptive keys (`"success: <case>"`, `"error: <case>"`).
- Place new fixtures under the package’s `testdata/` directory (see `codegen/testdata/`).
- For benchmarks, use `b.Loop()` over `b.N`.
- Add regression tests **before** changing diff/codegen production logic; CI gates on coverage uploaded via Codecov.

## Commit & Pull Request Guidelines

- Commit subjects follow `<scope>: <imperative subject>` — observed scopes include `cmd`, `codegen`, `diff`, `migration`, `sqlutil`, `docs`, `test`, `ci`, `docker`, `go.mod`, `github/workflows`. Keep them lowercase and under ~72 chars.
- Sign commits with `git commit --gpg-sign` (or `-S`).
- One logical change per commit; group related test/docs updates with the code change they cover.
- Pull requests use the `## Why` template — explain motivation, not diff contents. Link issues, list verification commands run, and call out any DDL-support-matrix updates.
- Never bypass hooks (`--no-verify`) or push force to `main`.

## Security & Configuration Tips

- Never commit credentials. Authenticate via `gcloud auth application-default login` or `GOOGLE_APPLICATION_CREDENTIALS`; for local work prefer the emulator with `SPANNER_EMULATOR_HOST=localhost:9010`.
- Destructive DB operations (`db drop`, `db reset`, `db truncate`) require explicit confirmation by design — do not weaken those prompts.
- The diff engine **must** fail loudly on unmodeled DDL rather than silently skipping it; new `memefish` AST nodes need either a model or an explicit unsupported error plus a matrix update.

## MCP Servers

- Always use the `gopls` MCP server for Go code navigation, diagnostics, symbol lookup, and refactors.
- When GCP related tasks, Always use `google-developer-knowledge`
