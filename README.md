# integration-kubernetes

`integration-kubernetes` is the target-side ecosystem plugin that closes the installation loop for Yggdrasil products. It speaks RabbitMQ RPC, exposes the adapter handshake expected by `yggdrasil-core`, and applies or observes Kubernetes objects on a real cluster.

This repository keeps its own local protocol types on purpose. The public wire
contract lives in [/Users/dakasa/projects/yggdrasil-core/docs/contracts](/Users/dakasa/projects/yggdrasil-core/docs/contracts), not in `yggdrasil-core/model`.

## Current scope

- `describe` queue for adapter introspection
- `execute` queue with:
  - `declarative_apply`
  - `observe_objects`
- real cluster execution through Kubernetes `dynamic` client
- server-side apply as the current write mode

## Queues

- `yggdrasil.adapter.kubernetes.describe`
- `yggdrasil.adapter.kubernetes.execute`

## Environment

- `BROKER_URL`: RabbitMQ connection string used by the worker itself

## Running

```bash
go run .
```

## GitHub dogfooding

This repository now ships:

- `.github/workflows/emit-deploy-event.yml`
- `.github/workflows/deploy.yml`

`emit-deploy-event.yml` uses the official GitHub Action
[`dakasa-yggdrasil/action-emit-workflow-run`](https://github.com/dakasa-yggdrasil/action-emit-workflow-run)
to send one workflow run request into `yggdrasil-core`. The core bootstrap
workflow `global/ecosystem-repository-commit` then dispatches this repository's
`deploy.yml`.

## Instance configuration

The integration instance can use:

- `in_cluster`
- `kubeconfig_path`
- `kubeconfig`
- `context`
- `default_namespace`
- `field_manager`

At least one of `in_cluster`, `kubeconfig`, or `kubeconfig_path` must be provided.

## Example integration_type

```json
{
  "apiVersion": "yggdrasil.io/v1alpha1",
  "kind": "integration_type",
  "metadata": {
    "name": "kubernetes",
    "namespace": "global"
  },
  "spec": {
    "provider": "kubernetes",
    "adapter": {
      "transport": "rabbitmq",
      "version": "1.0.0",
      "queues": {
        "describe": "yggdrasil.adapter.kubernetes.describe",
        "execute": "yggdrasil.adapter.kubernetes.execute"
      },
      "timeout_seconds": 30
    },
    "capabilities": ["describe", "execute"]
  }
}
```

## Example integration_instance

```json
{
  "apiVersion": "yggdrasil.io/v1alpha1",
  "kind": "integration_instance",
  "metadata": {
    "name": "kubernetes-platform-prod",
    "namespace": "global"
  },
  "spec": {
    "type_ref": {
      "name": "kubernetes",
      "namespace": "global"
    },
    "status": "active",
    "config": {
      "kubeconfig_path": "/etc/yggdrasil/kubeconfigs/platform-prod",
      "context": "platform-prod",
      "default_namespace": "default",
      "field_manager": "yggdrasil"
    },
    "discovery": {
      "enabled": false,
      "mode": "manual"
    }
  }
}
```

## Current semantics

- `declarative_apply` applies the desired object set with server-side apply
- `observe_objects` reads the live objects that correspond to the desired set
- namespaced resources inherit `target.namespace` or `default_namespace` when omitted in the object itself
- cluster-scoped resources keep their original scope
