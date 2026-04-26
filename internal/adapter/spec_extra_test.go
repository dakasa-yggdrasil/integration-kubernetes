package adapter

import (
	"context"
	"strings"
	"testing"

	model "github.com/dakasa-yggdrasil/integration-kubernetes/internal/protocol"
)

func TestExecuteCreateJobFromCronJob_DispatchesToTypedHandler(t *testing.T) {
	previous := createJobFromCronJobFn
	defer func() { createJobFromCronJobFn = previous }()

	called := false
	createJobFromCronJobFn = func(_ context.Context, _ clusterConfig, req model.AdapterCreateJobFromCronJobRequest) (model.AdapterCreateJobFromCronJobResponse, error) {
		called = true
		if req.CronJobName != "dakasa-postgres-backup" {
			t.Fatalf("unexpected cronjob_name: %s", req.CronJobName)
		}
		if req.Namespace != "dakasa" {
			t.Fatalf("unexpected namespace: %s", req.Namespace)
		}
		if !req.WaitForCompletion {
			t.Fatal("expected WaitForCompletion=true")
		}
		return model.AdapterCreateJobFromCronJobResponse{
			Operation:   OperationCreateJobFromCronJob,
			Status:      "succeeded",
			JobName:     "dakasa-postgres-backup-manual-abc",
			Namespace:   "dakasa",
			CronJobName: "dakasa-postgres-backup",
			Succeeded:   1,
		}, nil
	}

	resp, err := Execute(context.Background(), model.AdapterExecuteIntegrationRequest{
		Operation: OperationCreateJobFromCronJob,
		Input: map[string]any{
			"cronjob_name":        "dakasa-postgres-backup",
			"namespace":           "dakasa",
			"wait_for_completion": true,
			"timeout_seconds":     1800,
		},
		Integration: model.AdapterExecuteIntegrationContext{
			InstanceSpec: model.IntegrationInstanceManifestSpec{
				Config: map[string]any{
					"kubeconfig_path": "/tmp/test",
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !called {
		t.Fatal("expected createJobFromCronJobFn to be called")
	}
	if resp.Status != "succeeded" {
		t.Fatalf("status: %v", resp.Status)
	}
}

func TestDecodeCreateJobFromCronJobRequest_RequiresCronjobName(t *testing.T) {
	_, err := decodeCreateJobFromCronJobRequest(model.AdapterExecuteIntegrationRequest{
		Input: map[string]any{},
	})
	if err == nil || !strings.Contains(err.Error(), "cronjob_name") {
		t.Fatalf("expected cronjob_name error, got %v", err)
	}
}

func TestDecodeCreateJobFromCronJobRequest_RequiresInput(t *testing.T) {
	_, err := decodeCreateJobFromCronJobRequest(model.AdapterExecuteIntegrationRequest{})
	if err == nil || !strings.Contains(err.Error(), "input") {
		t.Fatalf("expected input error, got %v", err)
	}
}

func TestDescribeKubernetes_NewActionAppearsInCatalog(t *testing.T) {
	desc := Describe()
	found := false
	for _, action := range desc.ActionCatalog {
		if action.Name == OperationCreateJobFromCronJob {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("create_job_from_cronjob missing from action catalog")
	}
}
