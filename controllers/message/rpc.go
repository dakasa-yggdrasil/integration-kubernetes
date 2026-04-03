package message

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

type rpcError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type rpcResponse struct {
	OK    bool      `json:"ok"`
	Data  any       `json:"data,omitempty"`
	Error *rpcError `json:"error,omitempty"`
}

func replySuccess(ctx context.Context, conn *amqp.Connection, d amqp.Delivery, data any, logger *zap.Logger) error {
	return replyJSON(ctx, conn, d, rpcResponse{
		OK:   true,
		Data: data,
	}, logger)
}

func replyFailure(ctx context.Context, conn *amqp.Connection, d amqp.Delivery, code string, err error, logger *zap.Logger) error {
	if logger != nil {
		logger.Error("rabbitmq rpc handler failed",
			zap.String("queue", d.RoutingKey),
			zap.String("reply_to", d.ReplyTo),
			zap.String("correlation_id", d.CorrelationId),
			zap.String("error_code", code),
			zap.Error(err),
		)
	}

	return replyJSON(ctx, conn, d, rpcResponse{
		OK: false,
		Error: &rpcError{
			Code:    code,
			Message: err.Error(),
		},
	}, logger)
}

func replyJSON(ctx context.Context, conn *amqp.Connection, d amqp.Delivery, response rpcResponse, logger *zap.Logger) error {
	if strings.TrimSpace(d.ReplyTo) == "" {
		return errors.New("reply_to is required for kubernetes integration rpc consumers")
	}

	body, err := json.Marshal(response)
	if err != nil {
		return err
	}

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	if err := ch.PublishWithContext(ctx, "", d.ReplyTo, false, false, amqp.Publishing{
		ContentType:   "application/json",
		CorrelationId: d.CorrelationId,
		Body:          body,
	}); err != nil {
		return err
	}

	if logger != nil {
		logger.Debug("rabbitmq rpc reply sent",
			zap.String("queue", d.RoutingKey),
			zap.String("reply_to", d.ReplyTo),
			zap.String("correlation_id", d.CorrelationId),
		)
	}

	return nil
}
