package adapter

import (
	"context"
	"testing"

	model "github.com/dakasa-yggdrasil/integration-kubernetes/internal/protocol"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestDescribe(t *testing.T) {
	t.Setenv("YGGDRASIL_TRANSPORT", "http")
	response := Describe()

	if response.Provider != Provider {
		t.Fatalf("expected provider %q, got %q", Provider, response.Provider)
	}
	if response.Adapter.Transport != "http_json" {
		t.Fatalf("expected http_json transport when YGGDRASIL_TRANSPORT=http, got %q", response.Adapter.Transport)
	}
	if response.Adapter.Endpoints.Execute != "/rpc/execute" {
		t.Fatalf("expected /rpc/execute endpoint, got %q", response.Adapter.Endpoints.Execute)
	}
	if len(response.ActionCatalog) != len(SupportedExecuteOperations) {
		t.Fatalf("expected %d actions, got %d", len(SupportedExecuteOperations), len(response.ActionCatalog))
	}
	if len(response.Execution.IdempotentActions) != len(SupportedExecuteOperations) {
		t.Fatalf("idempotent actions = %#v, want %d operations", response.Execution.IdempotentActions, len(SupportedExecuteOperations))
	}
}

func TestDescribeAMQPWhenTransportIsRabbitMQ(t *testing.T) {
	t.Setenv("YGGDRASIL_TRANSPORT", "amqp")
	response := Describe()
	if response.Adapter.Transport != "rabbitmq" {
		t.Fatalf("expected rabbitmq transport when YGGDRASIL_TRANSPORT=amqp, got %q", response.Adapter.Transport)
	}
	if response.Adapter.Queues.Execute != QueueExecute {
		t.Fatalf("expected execute queue %q, got %q", QueueExecute, response.Adapter.Queues.Execute)
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

// TestExecuteSupportsApplyManifestRequests exercises the apply_manifest
// operation, which is the single-object variant of declarative_apply. The
// workflow step input carries one Kubernetes object under with.manifest;
// the adapter wraps it as a 1-element slice and reuses the same apply
// pipeline (server-side apply via dynamic client). This is the operation
// that integration_quickstart templates compile to for adapter-deployment,
// service-account, and credential-secret steps.
func TestExecuteSupportsApplyManifestRequests(t *testing.T) {
	previous := newKubernetesExecutor
	newKubernetesExecutor = func(cfg clusterConfig) (kubernetesExecutor, error) {
		return newFakeExecutor(), nil
	}
	defer func() {
		newKubernetesExecutor = previous
	}()

	response, err := Execute(context.Background(), model.AdapterExecuteIntegrationRequest{
		Operation:  OperationApplyManifest,
		Capability: OperationApplyManifest,
		Input: map[string]any{
			"namespace": "yggdrasil-adapters",
			"manifest": map[string]any{
				"apiVersion": "v1",
				"kind":       "ServiceAccount",
				"metadata": map[string]any{
					"name":      "schema-migrations-goose-postgres",
					"namespace": "yggdrasil-adapters",
				},
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
	if response.Operation != OperationApplyManifest {
		t.Fatalf("operation echoed as %q, want %q", response.Operation, OperationApplyManifest)
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
	if output.Resources[0].Kind != "ServiceAccount" {
		t.Fatalf("resource kind = %q, want ServiceAccount", output.Resources[0].Kind)
	}
	if output.Resources[0].Name != "schema-migrations-goose-postgres" {
		t.Fatalf("resource name = %q", output.Resources[0].Name)
	}
}

func TestExecuteApplyManifestRejectsMissingManifest(t *testing.T) {
	_, err := Execute(context.Background(), model.AdapterExecuteIntegrationRequest{
		Operation:  OperationApplyManifest,
		Capability: OperationApplyManifest,
		Input:      map[string]any{},
		Integration: model.AdapterExecuteIntegrationContext{
			Type:     model.ManifestReference{Namespace: "global", Name: "kubernetes"},
			Instance: model.ManifestReference{Namespace: "global", Name: "kubernetes-platform-prod"},
		},
	})
	if err == nil {
		t.Fatal("expected missing with.manifest to fail")
	}
}

func TestExecuteApplyManifestRejectsNonObjectManifest(t *testing.T) {
	_, err := Execute(context.Background(), model.AdapterExecuteIntegrationRequest{
		Operation:  OperationApplyManifest,
		Capability: OperationApplyManifest,
		Input: map[string]any{
			"manifest": "not an object",
		},
		Integration: model.AdapterExecuteIntegrationContext{
			Type:     model.ManifestReference{Namespace: "global", Name: "kubernetes"},
			Instance: model.ManifestReference{Namespace: "global", Name: "kubernetes-platform-prod"},
		},
	})
	if err == nil {
		t.Fatal("expected non-object with.manifest to fail")
	}
}

func TestObserveObjectsByLabelSelector(t *testing.T) {
	mkPod := func(name string, labels map[string]string) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "Pod",
			"metadata": map[string]any{
				"name":      name,
				"namespace": "ns",
				"labels":    asAnyMap(labels),
			},
		}}
	}
	fake := &fakeExecutor{objects: map[string]*unstructured.Unstructured{
		"a": mkPod("a", map[string]string{"app": "foo"}),
		"b": mkPod("b", map[string]string{"app": "foo"}),
		"c": mkPod("c", map[string]string{"app": "bar"}),
	}}

	selectors := []model.LabelSelector{{
		APIVersion: "v1", Kind: "Pod", Namespace: "ns",
		MatchLabels: map[string]string{"app": "foo"},
	}}

	results, err := fake.List(context.Background(), selectors, "default")
	if err != nil {
		t.Fatalf("List error: %v", err)
	}
	if got := len(results); got != 2 {
		t.Errorf("List result count = %d, want 2", got)
	}
}

func asAnyMap(in map[string]string) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func TestStateFromObjectIncludesSpec(t *testing.T) {
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata": map[string]any{
			"name":      "foo",
			"namespace": "ns",
		},
		"spec": map[string]any{
			"replicas": int64(3),
			"selector": map[string]any{
				"matchLabels": map[string]any{"app": "foo"},
			},
		},
		"status": map[string]any{
			"readyReplicas": int64(3),
		},
	}}

	state := stateFromObject(obj, true, "ready")
	specMap, _ := state.Metadata["spec"].(map[string]any)
	if specMap == nil {
		t.Fatal("stateFromObject must surface .spec in metadata")
	}
	if specMap["replicas"] != int64(3) {
		t.Errorf("spec.replicas = %v, want 3", specMap["replicas"])
	}
	// existing .status surfacing still works
	if _, ok := state.Metadata["status"].(map[string]any); !ok {
		t.Error("stateFromObject must still surface .status")
	}
}
