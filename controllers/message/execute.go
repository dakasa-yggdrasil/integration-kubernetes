package message

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/dakasa-co/yggdrasil-integration-kubernetes/internal/adapter"
	model "github.com/dakasa-co/yggdrasil-integration-kubernetes/internal/protocol"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

func executeHandler(conn *amqp.Connection, logger *zap.Logger) ConsumerHandler {
	return func(ctx context.Context, d amqp.Delivery) error {
		var envelope struct {
			Operation string `json:"operation"`
		}
		if err := json.Unmarshal(d.Body, &envelope); err != nil {
			return replyFailure(ctx, conn, d, "bad_request", err, logger)
		}

		operation := adapter.NormalizeExecuteOperation(envelope.Operation)
		if !adapter.SupportsExecuteOperation(operation) {
			return replyFailure(ctx, conn, d, "unsupported_operation", fmt.Errorf("unsupported operation %q", envelope.Operation), logger)
		}

		switch operation {
		case adapter.OperationDeclarativeApply:
			var req model.AdapterDeclarativeApplyRequest
			if err := json.Unmarshal(d.Body, &req); err != nil {
				return replyFailure(ctx, conn, d, "bad_request", err, logger)
			}
			response, err := adapter.DeclarativeApply(ctx, req)
			if err != nil {
				return replyFailure(ctx, conn, d, "apply_failed", err, logger)
			}
			return replySuccess(ctx, conn, d, response, logger)
		case adapter.OperationObserveObjects:
			var req model.AdapterObserveObjectsRequest
			if err := json.Unmarshal(d.Body, &req); err != nil {
				return replyFailure(ctx, conn, d, "bad_request", err, logger)
			}
			response, err := adapter.ObserveObjects(ctx, req)
			if err != nil {
				return replyFailure(ctx, conn, d, "observe_failed", err, logger)
			}
			return replySuccess(ctx, conn, d, response, logger)
		}

		return replyFailure(ctx, conn, d, "unsupported_operation", fmt.Errorf("unsupported operation %q", envelope.Operation), logger)
	}
}
