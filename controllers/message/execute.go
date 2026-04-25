package message

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/dakasa-yggdrasil/yggdrasil-sdk-go/rpc"
	"go.uber.org/zap"

	"github.com/dakasa-yggdrasil/integration-kubernetes/internal/adapter"
	model "github.com/dakasa-yggdrasil/integration-kubernetes/internal/protocol"
)

// ExecuteHandler returns an SDK-shaped handler for the execute
// capability. Callers send an AdapterExecuteIntegrationRequest; for
// backwards compat we also accept direct
// declarative_apply / observe_objects request shapes posted to the
// same endpoint (the spec package handles the shape detection and
// routes to the right adapter function).
func ExecuteHandler(logger *zap.Logger) Handler {
	return func(ctx context.Context, d rpc.Delivery) ([]byte, string, error) {
		var request model.AdapterExecuteIntegrationRequest
		if err := json.Unmarshal(d.Body, &request); err != nil {
			return failure("bad_request", err, logger)
		}

		operation := adapter.NormalizeExecuteOperation(request.Operation)
		if !adapter.SupportsExecuteOperation(operation) {
			return failure("unsupported_operation", fmt.Errorf("unsupported operation %q", request.Operation), logger)
		}

		if len(request.Input) > 0 || request.Integration.Instance.Name != "" || request.Integration.Type.Name != "" {
			response, err := adapter.Execute(ctx, request)
			if err != nil {
				return failure("execute_failed", err, logger)
			}
			return success(response)
		}

		switch operation {
		case adapter.OperationDeclarativeApply, adapter.OperationApplyManifest:
			var req model.AdapterDeclarativeApplyRequest
			if err := json.Unmarshal(d.Body, &req); err != nil {
				return failure("bad_request", err, logger)
			}
			response, err := adapter.DeclarativeApply(ctx, req)
			if err != nil {
				return failure("apply_failed", err, logger)
			}
			return success(response)
		case adapter.OperationObserveObjects:
			var req model.AdapterObserveObjectsRequest
			if err := json.Unmarshal(d.Body, &req); err != nil {
				return failure("bad_request", err, logger)
			}
			response, err := adapter.ObserveObjects(ctx, req)
			if err != nil {
				return failure("observe_failed", err, logger)
			}
			return success(response)
		case adapter.OperationEnsureDockerRegistrySecret:
			var req model.AdapterEnsureDockerRegistrySecretRequest
			if err := json.Unmarshal(d.Body, &req); err != nil {
				return failure("bad_request", err, logger)
			}
			response, err := adapter.EnsureDockerRegistrySecret(ctx, req)
			if err != nil {
				return failure("execute_failed", err, logger)
			}
			return success(response)
		}

		return failure("unsupported_operation", fmt.Errorf("unsupported operation %q", request.Operation), logger)
	}
}
