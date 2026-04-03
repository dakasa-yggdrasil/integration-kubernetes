package message

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/dakasa-co/yggdrasil-integration-kubernetes/internal/adapter"
	model "github.com/dakasa-co/yggdrasil-integration-kubernetes/internal/protocol"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

func describeHandler(conn *amqp.Connection, logger *zap.Logger) ConsumerHandler {
	return func(ctx context.Context, d amqp.Delivery) error {
		var req model.AdapterDescribeRequest
		if len(strings.TrimSpace(string(d.Body))) > 0 {
			if err := json.Unmarshal(d.Body, &req); err != nil {
				return replyFailure(ctx, conn, d, "bad_request", err, logger)
			}
		}

		if provider := strings.TrimSpace(req.Provider); provider != "" && !strings.EqualFold(provider, adapter.Provider) {
			return replyFailure(ctx, conn, d, "bad_request", fmt.Errorf("unsupported provider %q", req.Provider), logger)
		}
		if expected := strings.TrimSpace(req.ExpectedVersion); expected != "" && expected != adapter.AdapterVersion {
			return replyFailure(ctx, conn, d, "version_mismatch", fmt.Errorf("expected version %q but adapter is %q", expected, adapter.AdapterVersion), logger)
		}

		return replySuccess(ctx, conn, d, adapter.Describe(), logger)
	}
}
