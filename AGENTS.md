# AGENTS

## Repo role
This repository is a standalone Yggdrasil integration worker. It exposes an honest adapter contract through `describe` and executes capabilities through `execute`.

## Non-negotiable rules
- Keep the plugin standalone. Do not import runtime/domain code from the Yggdrasil monorepo.
- Keep protocol types local to this repository.
- `describe` must stay aligned with what `execute` actually accepts.
- If you add or rename capabilities, update tests, examples, and README in the same change.
- Prefer failing fast over silently degrading adapter behavior.
- This worker owns integration runtime behavior only. Business authority stays in `yggdrasil-core`.

## Runtime expectations
- The worker connects to RabbitMQ through `BROKER_URL`.
- `/healthz` is liveness only.
- `/readyz` must reflect whether the worker is still connected to RabbitMQ.
- Production changes should preserve graceful shutdown on `SIGINT`/`SIGTERM`.

## Commands
- `go test ./...`
- `task config`
- `task build:image`
- `task up`
- `task down`

## Change checklist
- Update adapter tests before claiming a new capability works.
- Keep examples under `examples/` aligned with the adapter contract.
- Prefer explicit env vars and documented defaults.
- Do not add Yggdrasil-core-specific data models here; use the public contract only.
