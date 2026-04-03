# Repo Context

Use this context whenever working in this repository.

## Identity
This is a standalone Yggdrasil integration worker repository.

## What good changes look like
- Adapter contract remains honest.
- README, tests, and examples move together.
- Startup is explicit and production-safe.
- Runtime behavior is isolated from Yggdrasil-core internals.

## Default workflow
1. Inspect `internal/adapter` and `controllers/message` before editing.
2. Update tests in the same pass as capability changes.
3. Run `go test ./...`.
4. If runtime/bootstrap changed, run `task config` and `task build:image`.
