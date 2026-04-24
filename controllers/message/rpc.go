package message

import (
	"encoding/json"

	"github.com/dakasa-yggdrasil/yggdrasil-sdk-go/adapter"
	"go.uber.org/zap"
)

// Handler aliases adapter.Handler so the rest of this package can
// declare handler returns without importing the SDK everywhere.
type Handler = adapter.Handler

// rpcError is the shape of the error field inside rpcResponse.
type rpcError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// rpcResponse preserves the wire shape the kubernetes adapter has
// always published: `{ ok, data? , error? }`. The SDK handles
// transport-level framing (HTTP body, AMQP publishing) around this
// JSON.
type rpcResponse struct {
	OK    bool      `json:"ok"`
	Data  any       `json:"data,omitempty"`
	Error *rpcError `json:"error,omitempty"`
}

// success is a handler-return helper: marshal a success rpcResponse
// and hand it back as body/content-type/err=nil. The SDK will Ack and
// Reply on the Delivery automatically.
func success(data any) ([]byte, string, error) {
	body, err := json.Marshal(rpcResponse{OK: true, Data: data})
	if err != nil {
		return nil, "", err
	}
	return body, "application/json", nil
}

// failure is the error-path equivalent of success. We do NOT return a
// non-nil error because the adapter's protocol-level failure is
// expressed as ok=false inside the reply body, not as a transport
// error. Returning err from the handler would cause the SDK to Nack
// the delivery and log a generic "handler failed" — we want the
// caller to see the structured `code` + `message` instead.
func failure(code string, cause error, logger *zap.Logger) ([]byte, string, error) {
	if logger != nil {
		logger.Error("adapter rpc handler failed",
			zap.String("error_code", code),
			zap.Error(cause),
		)
	}
	body, err := json.Marshal(rpcResponse{
		OK: false,
		Error: &rpcError{
			Code:    code,
			Message: cause.Error(),
		},
	})
	if err != nil {
		return nil, "", err
	}
	return body, "application/json", nil
}
