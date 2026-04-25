# Changelog

All notable changes to integration-kubernetes are documented here.

## [Unreleased]

### Added
- `observe_objects` metadata now includes `.spec` of each resource (alongside `.status`).
- `observe_objects` accepts `label_selectors: [{api_version, kind, namespace, match_labels}]` to find objects by labels (e.g. pods by Deployment labels) when names are unknown.
- `ensure_docker_registry_secret` operation — idempotent upsert of a Secret of type `kubernetes.io/dockerconfigjson` for image pull authentication.

### Fixed
- `observe_objects` now returns `status: not_found, observed: false` (instead of misreporting `observed`) when zero resources match.
- `ExecuteHandler` switch now consistently routes all 4 operations (added `apply_manifest` + `ensure_docker_registry_secret`).
- Decoders trim whitespace from string inputs before validation.
- `decodeLabelSelectors` rejects selectors with empty `match_labels` (would over-broadly select every object of that kind).
- `BuildDockerConfigJSON` no longer emits plaintext `username`/`password` alongside the `auth` blob (only `auth` is required by k8s `imagePullSecrets`).
