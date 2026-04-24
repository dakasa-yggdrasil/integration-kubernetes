package message

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/dakasa-yggdrasil/yggdrasil-sdk-go/rpc"
	"go.uber.org/zap"

	"github.com/dakasa-yggdrasil/integration-kubernetes/internal/adapter"
	model "github.com/dakasa-yggdrasil/integration-kubernetes/internal/protocol"
)

// DescribeHandler returns an SDK-shaped handler for the describe
// capability. The core calls this during the describe-handshake that
// precedes every execute to verify adapter version / queues / endpoint
// shape against the stored integration_type manifest.
func DescribeHandler(logger *zap.Logger) Handler {
	return func(ctx context.Context, d rpc.Delivery) ([]byte, string, error) {
		var req model.AdapterDescribeRequest
		if len(strings.TrimSpace(string(d.Body))) > 0 {
			if err := json.Unmarshal(d.Body, &req); err != nil {
				return failure("bad_request", err, logger)
			}
		}

		if provider := strings.TrimSpace(req.Provider); provider != "" && !strings.EqualFold(provider, adapter.Provider) {
			return failure("bad_request", fmt.Errorf("unsupported provider %q", req.Provider), logger)
		}
		if expected := strings.TrimSpace(req.ExpectedVersion); expected != "" && expected != adapter.AdapterVersion {
			return failure("version_mismatch", fmt.Errorf("expected version %q but adapter is %q", expected, adapter.AdapterVersion), logger)
		}

		return success(adapter.Describe())
	}
}
