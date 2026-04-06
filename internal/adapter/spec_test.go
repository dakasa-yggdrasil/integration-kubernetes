package adapter

import (
	"context"
	"testing"

	model "github.com/dakasa-yggdrasil/integration-kubernetes/internal/protocol"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestDescribe(t *testing.T) {
	response := Describe()

	if response.Provider != Provider {
		t.Fatalf("expected provider %q, got %q", Provider, response.Provider)
	}
	if response.Adapter.Queues.Execute != QueueExecute {
		t.Fatalf("expected execute queue %q, got %q", QueueExecute, response.Adapter.Queues.Execute)
	}
	if len(response.ActionCatalog) != 2 {
		t.Fatalf("expected 2 actions, got %d", len(response.ActionCatalog))
	}
	if len(response.Execution.IdempotentActions) != len(SupportedExecuteOperations) {
		t.Fatalf("idempotent actions = %#v, want %d operations", response.Execution.IdempotentActions, len(SupportedExecuteOperations))
	}
}

func TestSupportedExecuteOperationsStayAligned(t *testing.T) {
	for _, operation := range SupportedExecuteOperations {
		if !SupportsExecuteOperation(operation) {
			t.Fatalf("SupportsExecuteOperation(%q) = false", operation)
		}
	}
}

func TestResolveClusterConfigRequiresAccessMode(t *testing.T) {
	_, err := resolveClusterConfig(model.IntegrationInstanceManifestSpec{})
	if err == nil {
		t.Fatal("expected missing cluster access mode to fail")
	}
}

func TestResolveClusterConfigPrefersCredentials(t *testing.T) {
	cfg, err := resolveClusterConfig(model.IntegrationInstanceManifestSpec{
		Credentials: map[string]any{
			"kubeconfig": "credentials-value",
		},
		Config: map[string]any{
			"kubeconfig":        "config-value",
			"default_namespace": "platform",
		},
	})
	if err != nil {
		t.Fatalf("resolve cluster config: %v", err)
	}

	if cfg.Kubeconfig != "credentials-value" {
		t.Fatalf("expected credentials kubeconfig to win, got %q", cfg.Kubeconfig)
	}
	if cfg.DefaultNamespace != "platform" {
		t.Fatalf("expected namespace fallback from config, got %q", cfg.DefaultNamespace)
	}
}

func TestFakeExecutorApplyAndObserveNamespacedObject(t *testing.T) {
	executor := newFakeExecutor()

	objects := []map[string]any{
		{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]any{
				"name": "platform-settings",
			},
			"data": map[string]any{
				"region": "sa-east1",
			},
		},
	}

	applied, err := executor.Apply(context.Background(), objects, "platform-system", DefaultFieldManager)
	if err != nil {
		t.Fatalf("apply objects: %v", err)
	}
	if len(applied) != 1 {
		t.Fatalf("expected 1 applied object, got %d", len(applied))
	}
	if applied[0].Namespace != "platform-system" {
		t.Fatalf("expected namespace fallback to be used, got %q", applied[0].Namespace)
	}

	observed, err := executor.Observe(context.Background(), objects, "platform-system")
	if err != nil {
		t.Fatalf("observe objects: %v", err)
	}
	if len(observed) != 1 {
		t.Fatalf("expected 1 observed object, got %d", len(observed))
	}
	if !observed[0].Observed {
		t.Fatal("expected object to be observed")
	}
	if observed[0].Status != "observed" {
		t.Fatalf("expected observed status, got %q", observed[0].Status)
	}
}

func TestFakeExecutorObserveMissingObject(t *testing.T) {
	executor := newFakeExecutor(&unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "Namespace",
			"metadata": map[string]any{
				"name": "platform-system",
			},
		},
	})

	observed, err := executor.Observe(context.Background(), []map[string]any{
		{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]any{
				"name":      "missing-config",
				"namespace": "platform-system",
			},
		},
	}, "platform-system")
	if err != nil {
		t.Fatalf("observe objects: %v", err)
	}
	if len(observed) != 1 {
		t.Fatalf("expected 1 observed object, got %d", len(observed))
	}
	if observed[0].Observed {
		t.Fatal("expected missing object to be unobserved")
	}
	if observed[0].Status != "not_found" {
		t.Fatalf("expected not_found, got %q", observed[0].Status)
	}
}

func TestExecuteSupportsGenericDeclarativeApplyRequests(t *testing.T) {
	previous := newKubernetesExecutor
	newKubernetesExecutor = func(cfg clusterConfig) (kubernetesExecutor, error) {
		return newFakeExecutor(), nil
	}
	defer func() {
		newKubernetesExecutor = previous
	}()

	response, err := Execute(context.Background(), model.AdapterExecuteIntegrationRequest{
		Operation:  OperationDeclarativeApply,
		Capability: OperationDeclarativeApply,
		Input: map[string]any{
			"namespace": "platform-system",
			"objects": []any{
				map[string]any{
					"apiVersion": "v1",
					"kind":       "ConfigMap",
					"metadata": map[string]any{
						"name": "platform-settings",
					},
					"data": map[string]any{
						"region": "sa-east1",
					},
				},
			},
			"reconcile": map[string]any{
				"strategy": "server_side_apply",
				"wait":     true,
			},
		},
		Integration: model.AdapterExecuteIntegrationContext{
			Type: model.ManifestReference{
				Namespace: "global",
				Name:      "kubernetes",
			},
			Instance: model.ManifestReference{
				Namespace: "global",
				Name:      "kubernetes-platform-prod",
			},
			InstanceSpec: model.IntegrationInstanceManifestSpec{
				Credentials: map[string]any{
					"kubeconfig": "apiVersion: v1\nclusters: []\ncontexts: []\ncurrent-context: ''\nkind: Config\npreferences: {}\nusers: []\n",
				},
				Config: map[string]any{
					"default_namespace": "default",
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if response.Status != "applied" {
		t.Fatalf("status = %q, want applied", response.Status)
	}

	output, ok := response.Output.(model.AdapterDeclarativeApplyResponse)
	if !ok {
		t.Fatalf("output type = %T, want AdapterDeclarativeApplyResponse", response.Output)
	}
	if !output.Applied {
		t.Fatal("expected apply response to report applied=true")
	}
	if len(output.Resources) != 1 {
		t.Fatalf("resources = %d, want 1", len(output.Resources))
	}
	if output.Resources[0].Namespace != "platform-system" {
		t.Fatalf("resource namespace = %q, want platform-system", output.Resources[0].Namespace)
	}
}
