This repository is a standalone Yggdrasil integration worker.

Rules:
- Keep adapter protocol types local to this repo.
- Do not import internal code from the Yggdrasil monorepo.
- Keep `describe` and `execute` aligned.
- Update tests and examples with every capability change.
- Prefer explicit env vars and predictable startup/shutdown behavior.
