package adapter

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	model "github.com/dakasa-yggdrasil/integration-kubernetes/internal/protocol"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/discovery"
	memcache "k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	Provider                  = "kubernetes"
	AdapterVersion            = "1.0.0"
	OperationDeclarativeApply = "declarative_apply"
	// OperationApplyManifest is the single-object variant of
	// declarative_apply. Callers pass exactly one Kubernetes object under
	// with.manifest; the adapter wraps it as a 1-element slice and runs
	// the same server-side apply pipeline. integration_quickstart steps
	// (apply-service-account, apply-adapter-deployment, apply-credentials-secret)
	// compile to this operation — one manifest per step gives adopters
	// explicit, auditable YAML rather than opaque object arrays.
	OperationApplyManifest  = "apply_manifest"
	OperationObserveObjects = "observe_objects"
	ModeServerSideApply     = "server_side_apply"
	DefaultFieldManager     = "yggdrasil"

	QueueDescribe = "yggdrasil.adapter.kubernetes.describe"
	QueueExecute  = "yggdrasil.adapter.kubernetes.execute"
)

var SupportedExecuteOperations = []string{
	OperationDeclarativeApply,
	OperationApplyManifest,
	OperationObserveObjects,
}

type clusterConfig struct {
	InCluster        bool
	KubeconfigPath   string
	Kubeconfig       string
	Context          string
	DefaultNamespace string
	FieldManager     string
}

type kubernetesExecutor interface {
	Apply(ctx context.Context, objects []map[string]any, namespace string, fieldManager string) ([]model.InstallationResourceState, error)
	Observe(ctx context.Context, objects []map[string]any, namespace string) ([]model.InstallationResourceState, error)
}

type dynamicExecutor struct {
	dynamicClient dynamic.Interface
	mapper        meta.RESTMapper
}

type fakeExecutor struct {
	mapper  meta.RESTMapper
	objects map[string]*unstructured.Unstructured
}

var newKubernetesExecutor = buildExecutor

// Describe returns the normalized contract exposed by this adapter.
// Transport + addressing mirror what main.go chose at startup via
// YGGDRASIL_TRANSPORT (default http_json). The core uses this to
// verify the adapter's live shape matches the stored
// integration_type manifest — mismatches abort execution before a
// dispatch crosses the wire.
func Describe() model.AdapterDescribeResponse {
	transport := strings.ToLower(strings.TrimSpace(os.Getenv("YGGDRASIL_TRANSPORT")))
	if transport == "" {
		transport = "http"
	}
	adapterSpec := model.IntegrationAdapterSpec{
		Version:        AdapterVersion,
		TimeoutSeconds: 30,
	}
	switch transport {
	case "amqp", "rabbitmq":
		adapterSpec.Transport = "rabbitmq"
		adapterSpec.Queues = model.IntegrationAdapterQueue{
			Describe: QueueDescribe,
			Execute:  QueueExecute,
		}
	default:
		adapterSpec.Transport = "http_json"
		adapterSpec.Endpoints = model.IntegrationAdapterRoute{
			Describe: "/rpc/describe",
			Execute:  "/rpc/execute",
		}
	}
	return model.AdapterDescribeResponse{
		Provider: Provider,
		Adapter:  adapterSpec,
		Capabilities: []string{"describe", "execute"},
		CredentialSchema: model.IntegrationSchemaSpec{
			Mode: "inline",
			Properties: map[string]model.IntegrationSchemaProperty{
				"kubeconfig": {
					Type:        "string",
					Description: "Inline kubeconfig content.",
					Secret:      true,
				},
			},
		},
		InstanceSchema: model.IntegrationSchemaSpec{
			Mode: "inline",
			Properties: map[string]model.IntegrationSchemaProperty{
				"base_url": {
					Type:        "string",
					Description: "HTTP adapter base URL used when the integration_type declares transport=http_json.",
				},
				"in_cluster": {
					Type:        "boolean",
					Description: "Use in-cluster Kubernetes authentication.",
					Default:     false,
				},
				"kubeconfig_path": {
					Type:        "string",
					Description: "Path to a kubeconfig file on the worker filesystem.",
				},
				"kubeconfig": {
					Type:        "string",
					Description: "Inline kubeconfig content.",
					Secret:      true,
				},
				"context": {
					Type:        "string",
					Description: "Optional kubeconfig context override.",
				},
				"default_namespace": {
					Type:        "string",
					Description: "Namespace used when desired objects omit one.",
					Default:     "default",
				},
				"field_manager": {
					Type:        "string",
					Description: "Field manager used for server-side apply.",
					Default:     DefaultFieldManager,
				},
			},
		},
		ResourceTypes: []model.IntegrationResourceType{
			{
				Name:             "object",
				CanonicalPrefix:  "thirdparty.kubernetes.object",
				IdentityTemplate: "object.{namespace}.{kind}.{name}",
				Discoverable:     false,
				DefaultActions:   append([]string(nil), SupportedExecuteOperations...),
			},
		},
		ActionCatalog: describeActionCatalog(),
		Discovery: model.IntegrationDiscoverySpec{
			Mode:   "push",
			Cursor: "none",
		},
		Normalization: model.IntegrationNormalizationSpec{
			ExternalIDPath:         "metadata.uid",
			NamePath:               "metadata.name",
			OwnerPath:              "metadata.namespace",
			FallbackResourcePrefix: "thirdparty.kubernetes.custom",
		},
		Execution: model.IntegrationExecutionSpec{
			SupportsDryRun:    false,
			IdempotentActions: append([]string(nil), SupportedExecuteOperations...),
		},
		Extensions: model.IntegrationExtensionsSpec{
			AllowCustomResourceTypes: true,
			PreserveRawPayload:       true,
		},
	}
}

func NormalizeExecuteOperation(operation string) string {
	return strings.TrimSpace(operation)
}

func SupportsExecuteOperation(value string) bool {
	value = strings.TrimSpace(value)
	for _, supported := range SupportedExecuteOperations {
		if value == supported {
			return true
		}
	}
	return false
}

func decodeGenericDeclarativeApplyRequest(req model.AdapterExecuteIntegrationRequest) (model.AdapterDeclarativeApplyRequest, error) {
	if req.Integration.Instance.Name == "" || req.Integration.Type.Name == "" {
		return model.AdapterDeclarativeApplyRequest{}, fmt.Errorf("generic declarative_apply requires integration context")
	}

	objects := objectSlice(req.Input["objects"])
	if len(objects) == 0 {
		return model.AdapterDeclarativeApplyRequest{}, fmt.Errorf("generic declarative_apply requires at least one object")
	}

	return model.AdapterDeclarativeApplyRequest{
		Operation: OperationDeclarativeApply,
		Context:   decodeGenericInstallationContext(req.Input["context"]),
		Target: model.AdapterTargetIntegrationContext{
			Type:         req.Integration.Type,
			TypeSpec:     req.Integration.TypeSpec,
			Instance:     req.Integration.Instance,
			InstanceSpec: req.Integration.InstanceSpec,
		},
		Objects:   objects,
		Namespace: stringValue(req.Input["namespace"]),
		Reconcile: decodeGenericReconcile(req.Input["reconcile"]),
	}, nil
}

// decodeGenericApplyManifestRequest shapes an apply_manifest request into
// the AdapterDeclarativeApplyRequest used by the shared apply pipeline.
// The with.manifest field is expected to be a single Kubernetes object
// (map); it is wrapped as a 1-element slice so DeclarativeApply can handle
// it unchanged. Bubble up a clear error when the shape is wrong instead
// of silently persisting nothing.
func decodeGenericApplyManifestRequest(req model.AdapterExecuteIntegrationRequest) (model.AdapterDeclarativeApplyRequest, error) {
	if req.Integration.Instance.Name == "" || req.Integration.Type.Name == "" {
		return model.AdapterDeclarativeApplyRequest{}, fmt.Errorf("apply_manifest requires integration context")
	}

	raw, ok := req.Input["manifest"]
	if !ok {
		return model.AdapterDeclarativeApplyRequest{}, fmt.Errorf("apply_manifest requires with.manifest")
	}
	manifestMap, ok := raw.(map[string]any)
	if !ok {
		return model.AdapterDeclarativeApplyRequest{}, fmt.Errorf("apply_manifest: with.manifest must be an object")
	}
	if len(manifestMap) == 0 {
		return model.AdapterDeclarativeApplyRequest{}, fmt.Errorf("apply_manifest: with.manifest is empty")
	}

	return model.AdapterDeclarativeApplyRequest{
		Operation: OperationApplyManifest,
		Context:   decodeGenericInstallationContext(req.Input["context"]),
		Target: model.AdapterTargetIntegrationContext{
			Type:         req.Integration.Type,
			TypeSpec:     req.Integration.TypeSpec,
			Instance:     req.Integration.Instance,
			InstanceSpec: req.Integration.InstanceSpec,
		},
		Objects:   []map[string]any{manifestMap},
		Namespace: stringValue(req.Input["namespace"]),
		Reconcile: decodeGenericReconcile(req.Input["reconcile"]),
	}, nil
}

func decodeGenericObserveObjectsRequest(req model.AdapterExecuteIntegrationRequest) (model.AdapterObserveObjectsRequest, error) {
	if req.Integration.Instance.Name == "" || req.Integration.Type.Name == "" {
		return model.AdapterObserveObjectsRequest{}, fmt.Errorf("generic observe_objects requires integration context")
	}

	objects := objectSlice(req.Input["objects"])
	if len(objects) == 0 {
		return model.AdapterObserveObjectsRequest{}, fmt.Errorf("generic observe_objects requires at least one object")
	}

	return model.AdapterObserveObjectsRequest{
		Operation: OperationObserveObjects,
		Context:   decodeGenericInstallationContext(req.Input["context"]),
		Target: model.AdapterTargetIntegrationContext{
			Type:         req.Integration.Type,
			TypeSpec:     req.Integration.TypeSpec,
			Instance:     req.Integration.Instance,
			InstanceSpec: req.Integration.InstanceSpec,
		},
		Objects:   objects,
		Namespace: stringValue(req.Input["namespace"]),
	}, nil
}

func decodeGenericInstallationContext(value any) model.AdapterGenerateInstallationContext {
	object := mapValue(value)
	product := mapValue(object["product"])
	return model.AdapterGenerateInstallationContext{
		Product: model.ManifestReference{
			Kind:      stringValue(product["kind"]),
			Namespace: stringValue(product["namespace"]),
			Name:      stringValue(product["name"]),
		},
		Component: stringValue(object["component"]),
		Category:  stringValue(object["category"]),
		Class:     stringValue(object["class"]),
	}
}

func decodeGenericReconcile(value any) model.ProductReconcileSpec {
	object := mapValue(value)
	return model.ProductReconcileSpec{
		Strategy: stringValue(object["strategy"]),
		Prune:    boolValue(object["prune"]),
		Wait:     boolValue(object["wait"]),
	}
}

func Execute(ctx context.Context, req model.AdapterExecuteIntegrationRequest) (model.AdapterExecuteIntegrationResponse, error) {
	operation := NormalizeExecuteOperation(req.Operation)
	if !SupportsExecuteOperation(operation) {
		return model.AdapterExecuteIntegrationResponse{}, fmt.Errorf("unsupported operation %q", req.Operation)
	}

	switch operation {
	case OperationDeclarativeApply:
		typedReq, err := decodeGenericDeclarativeApplyRequest(req)
		if err != nil {
			return model.AdapterExecuteIntegrationResponse{}, err
		}
		response, err := DeclarativeApply(ctx, typedReq)
		if err != nil {
			return model.AdapterExecuteIntegrationResponse{}, err
		}
		return model.AdapterExecuteIntegrationResponse{
			Operation:  operation,
			Capability: firstNonEmptyString(strings.TrimSpace(req.Capability), operation),
			Status:     "applied",
			Output:     response,
			Metadata:   response.Metadata,
		}, nil
	case OperationApplyManifest:
		typedReq, err := decodeGenericApplyManifestRequest(req)
		if err != nil {
			return model.AdapterExecuteIntegrationResponse{}, err
		}
		response, err := DeclarativeApply(ctx, typedReq)
		if err != nil {
			return model.AdapterExecuteIntegrationResponse{}, err
		}
		return model.AdapterExecuteIntegrationResponse{
			Operation:  operation,
			Capability: firstNonEmptyString(strings.TrimSpace(req.Capability), operation),
			Status:     "applied",
			Output:     response,
			Metadata:   response.Metadata,
		}, nil
	case OperationObserveObjects:
		typedReq, err := decodeGenericObserveObjectsRequest(req)
		if err != nil {
			return model.AdapterExecuteIntegrationResponse{}, err
		}
		response, err := ObserveObjects(ctx, typedReq)
		if err != nil {
			return model.AdapterExecuteIntegrationResponse{}, err
		}
		return model.AdapterExecuteIntegrationResponse{
			Operation:  operation,
			Capability: firstNonEmptyString(strings.TrimSpace(req.Capability), operation),
			Status:     response.Status,
			Output:     response,
			Metadata:   response.Metadata,
		}, nil
	default:
		return model.AdapterExecuteIntegrationResponse{}, fmt.Errorf("unsupported operation %q", req.Operation)
	}
}

// DeclarativeApply applies one desired object set to a Kubernetes target.
func DeclarativeApply(ctx context.Context, req model.AdapterDeclarativeApplyRequest) (model.AdapterDeclarativeApplyResponse, error) {
	cfg, err := resolveClusterConfig(req.Target.InstanceSpec)
	if err != nil {
		return model.AdapterDeclarativeApplyResponse{}, err
	}

	executor, err := newKubernetesExecutor(cfg)
	if err != nil {
		return model.AdapterDeclarativeApplyResponse{}, err
	}

	fieldManager := strings.TrimSpace(cfg.FieldManager)
	if fieldManager == "" {
		fieldManager = DefaultFieldManager
	}

	resources, err := executor.Apply(ctx, req.Objects, effectiveNamespace(req.Namespace, cfg.DefaultNamespace), fieldManager)
	if err != nil {
		return model.AdapterDeclarativeApplyResponse{}, err
	}

	return model.AdapterDeclarativeApplyResponse{
		Operation: OperationDeclarativeApply,
		Applied:   true,
		Mode:      ModeServerSideApply,
		Resources: resources,
		Metadata: map[string]any{
			"provider":           Provider,
			"field_manager":      fieldManager,
			"default_namespace":  cfg.DefaultNamespace,
			"applied_resources":  len(resources),
			"target_instance":    req.Target.Instance.Name,
			"target_integration": req.Target.Type.Name,
			"reconcile_strategy": req.Reconcile.Strategy,
			"reconcile_wait":     req.Reconcile.Wait,
			"reconcile_prune":    req.Reconcile.Prune,
		},
	}, nil
}

// ObserveObjects observes one desired object set on a Kubernetes target.
func ObserveObjects(ctx context.Context, req model.AdapterObserveObjectsRequest) (model.AdapterObserveObjectsResponse, error) {
	cfg, err := resolveClusterConfig(req.Target.InstanceSpec)
	if err != nil {
		return model.AdapterObserveObjectsResponse{}, err
	}

	executor, err := newKubernetesExecutor(cfg)
	if err != nil {
		return model.AdapterObserveObjectsResponse{}, err
	}

	resources, err := executor.Observe(ctx, req.Objects, effectiveNamespace(req.Namespace, cfg.DefaultNamespace))
	if err != nil {
		return model.AdapterObserveObjectsResponse{}, err
	}

	observed := true
	status := "observed"
	for _, resource := range resources {
		if !resource.Observed {
			observed = false
			status = "partial"
			break
		}
		if strings.TrimSpace(resource.Status) == "not_ready" {
			status = "not_ready"
		}
	}

	return model.AdapterObserveObjectsResponse{
		Operation: OperationObserveObjects,
		Status:    status,
		Observed:  observed,
		Resources: resources,
		Metadata: map[string]any{
			"provider":           Provider,
			"default_namespace":  cfg.DefaultNamespace,
			"observed_resources": len(resources),
			"target_instance":    req.Target.Instance.Name,
			"target_integration": req.Target.Type.Name,
		},
	}, nil
}

func describeActionCatalog() []model.IntegrationActionDefinition {
	return []model.IntegrationActionDefinition{
		{
			Name:          OperationDeclarativeApply,
			Description:   "Apply a desired object set to a Kubernetes cluster using server-side apply.",
			ResourceTypes: []string{"object"},
			Idempotent:    true,
		},
		{
			Name:          OperationApplyManifest,
			Description:   "Apply a single Kubernetes manifest object using server-side apply.",
			ResourceTypes: []string{"object"},
			Idempotent:    true,
		},
		{
			Name:          OperationObserveObjects,
			Description:   "Observe the current state of a desired object set in a Kubernetes cluster.",
			ResourceTypes: []string{"object"},
			Idempotent:    true,
		},
	}
}

func resolveClusterConfig(spec model.IntegrationInstanceManifestSpec) (clusterConfig, error) {
	config := spec.Config
	credentials := spec.Credentials

	cfg := clusterConfig{
		InCluster:        firstBool(credentials, config, []string{"in_cluster"}, false),
		KubeconfigPath:   firstString(credentials, config, []string{"kubeconfig_path"}, ""),
		Kubeconfig:       firstString(credentials, config, []string{"kubeconfig"}, ""),
		Context:          firstString(credentials, config, []string{"context"}, ""),
		DefaultNamespace: firstString(credentials, config, []string{"default_namespace"}, "default"),
		FieldManager:     firstString(credentials, config, []string{"field_manager"}, DefaultFieldManager),
	}

	if cfg.InCluster {
		return cfg, nil
	}
	if cfg.Kubeconfig == "" && cfg.KubeconfigPath == "" {
		return clusterConfig{}, fmt.Errorf("kubernetes integration requires in_cluster, kubeconfig, or kubeconfig_path")
	}

	return cfg, nil
}

func buildExecutor(cfg clusterConfig) (kubernetesExecutor, error) {
	restConfig, err := buildRESTConfig(cfg)
	if err != nil {
		return nil, err
	}

	dyn, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("create dynamic client: %w", err)
	}

	discoveryClient, err := discovery.NewDiscoveryClientForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("create discovery client: %w", err)
	}

	mapper := restmapper.NewDeferredDiscoveryRESTMapper(memcache.NewMemCacheClient(discoveryClient))
	return &dynamicExecutor{
		dynamicClient: dyn,
		mapper:        mapper,
	}, nil
}

func buildRESTConfig(cfg clusterConfig) (*rest.Config, error) {
	if cfg.InCluster {
		restConfig, err := rest.InClusterConfig()
		if err != nil {
			return nil, fmt.Errorf("load in-cluster config: %w", err)
		}
		return restConfig, nil
	}

	var rawConfig []byte
	if cfg.Kubeconfig != "" {
		rawConfig = []byte(cfg.Kubeconfig)
	} else {
		payload, err := os.ReadFile(cfg.KubeconfigPath)
		if err != nil {
			return nil, fmt.Errorf("read kubeconfig_path: %w", err)
		}
		rawConfig = payload
	}

	loaded, err := clientcmd.Load(rawConfig)
	if err != nil {
		return nil, fmt.Errorf("load kubeconfig: %w", err)
	}

	overrides := &clientcmd.ConfigOverrides{}
	if cfg.Context != "" {
		overrides.CurrentContext = cfg.Context
	}

	clientConfig := clientcmd.NewDefaultClientConfig(*loaded, overrides)
	restConfig, err := clientConfig.ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("build rest config: %w", err)
	}
	return restConfig, nil
}

func (e *dynamicExecutor) Apply(ctx context.Context, objects []map[string]any, namespace string, fieldManager string) ([]model.InstallationResourceState, error) {
	resources := make([]model.InstallationResourceState, 0, len(objects))
	for _, raw := range objects {
		resource, err := e.applyOne(ctx, raw, namespace, fieldManager)
		if err != nil {
			return nil, err
		}
		resources = append(resources, resource)
	}
	return resources, nil
}

func (e *dynamicExecutor) Observe(ctx context.Context, objects []map[string]any, namespace string) ([]model.InstallationResourceState, error) {
	resources := make([]model.InstallationResourceState, 0, len(objects))
	for _, raw := range objects {
		resource, err := e.observeOne(ctx, raw, namespace)
		if err != nil {
			return nil, err
		}
		resources = append(resources, resource)
	}
	return resources, nil
}

func (e *dynamicExecutor) applyOne(ctx context.Context, raw map[string]any, namespace, fieldManager string) (model.InstallationResourceState, error) {
	obj, mapping, err := e.prepareObject(raw, namespace)
	if err != nil {
		return model.InstallationResourceState{}, err
	}

	patchData, err := json.Marshal(obj.Object)
	if err != nil {
		return model.InstallationResourceState{}, fmt.Errorf("marshal object %s/%s: %w", obj.GetKind(), obj.GetName(), err)
	}

	resourceClient := e.resourceInterface(mapping, obj.GetNamespace())
	force := true
	applied, err := resourceClient.Patch(ctx, obj.GetName(), types.ApplyPatchType, patchData, metav1.PatchOptions{
		FieldManager: fieldManager,
		Force:        &force,
	})
	if err != nil {
		return model.InstallationResourceState{}, fmt.Errorf("apply %s/%s: %w", obj.GetKind(), obj.GetName(), err)
	}

	return stateFromObject(applied, true, "applied"), nil
}

func (e *dynamicExecutor) observeOne(ctx context.Context, raw map[string]any, namespace string) (model.InstallationResourceState, error) {
	obj, mapping, err := e.prepareObject(raw, namespace)
	if err != nil {
		return model.InstallationResourceState{}, err
	}

	resourceClient := e.resourceInterface(mapping, obj.GetNamespace())
	live, err := resourceClient.Get(ctx, obj.GetName(), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return model.InstallationResourceState{
			Kind:      obj.GetKind(),
			Namespace: obj.GetNamespace(),
			Name:      obj.GetName(),
			Status:    "not_found",
			Observed:  false,
		}, nil
	}
	if err != nil {
		return model.InstallationResourceState{}, fmt.Errorf("observe %s/%s: %w", obj.GetKind(), obj.GetName(), err)
	}

	return stateFromObject(live, true, deriveResourceStatus(live)), nil
}

func (e *dynamicExecutor) prepareObject(raw map[string]any, namespace string) (*unstructured.Unstructured, *meta.RESTMapping, error) {
	obj, err := normalizeObject(raw)
	if err != nil {
		return nil, nil, err
	}

	mapping, err := e.mapper.RESTMapping(obj.GroupVersionKind().GroupKind(), obj.GroupVersionKind().Version)
	if err != nil {
		return nil, nil, fmt.Errorf("map %s: %w", obj.GroupVersionKind().String(), err)
	}

	if mapping.Scope != nil && mapping.Scope.Name() == meta.RESTScopeNameNamespace && obj.GetNamespace() == "" {
		obj.SetNamespace(namespace)
	}

	return obj, mapping, nil
}

func (e *dynamicExecutor) resourceInterface(mapping *meta.RESTMapping, namespace string) dynamic.ResourceInterface {
	if mapping.Scope != nil && mapping.Scope.Name() == meta.RESTScopeNameNamespace {
		return e.dynamicClient.Resource(mapping.Resource).Namespace(namespace)
	}
	return e.dynamicClient.Resource(mapping.Resource)
}

func normalizeObject(raw map[string]any) (*unstructured.Unstructured, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("kubernetes object cannot be empty")
	}
	cloned := runtime.DeepCopyJSON(raw)
	obj := &unstructured.Unstructured{Object: cloned}
	if strings.TrimSpace(obj.GetAPIVersion()) == "" {
		return nil, fmt.Errorf("kubernetes object requires apiVersion")
	}
	if strings.TrimSpace(obj.GetKind()) == "" {
		return nil, fmt.Errorf("kubernetes object requires kind")
	}
	if strings.TrimSpace(obj.GetName()) == "" {
		return nil, fmt.Errorf("kubernetes object requires metadata.name")
	}
	return obj, nil
}

func stateFromObject(obj *unstructured.Unstructured, observed bool, status string) model.InstallationResourceState {
	metadata := map[string]any{
		"api_version":      obj.GetAPIVersion(),
		"kind":             obj.GetKind(),
		"resource_version": obj.GetResourceVersion(),
		"generation":       obj.GetGeneration(),
		"uid":              string(obj.GetUID()),
	}

	return model.InstallationResourceState{
		Kind:      obj.GetKind(),
		Namespace: obj.GetNamespace(),
		Name:      obj.GetName(),
		Status:    status,
		Observed:  observed,
		Metadata:  metadata,
	}
}

func deriveResourceStatus(obj *unstructured.Unstructured) string {
	if obj.GetDeletionTimestamp() != nil {
		return "deleting"
	}

	conditions, found, err := unstructured.NestedSlice(obj.Object, "status", "conditions")
	if err == nil && found {
		for _, item := range conditions {
			condition, ok := item.(map[string]any)
			if !ok {
				continue
			}
			conditionType := strings.ToLower(strings.TrimSpace(stringValue(condition["type"])))
			conditionStatus := strings.ToLower(strings.TrimSpace(stringValue(condition["status"])))
			if conditionStatus != "true" && conditionStatus != "false" {
				continue
			}
			switch conditionType {
			case "ready", "available", "established", "reconciled":
				if conditionStatus == "true" {
					return "ready"
				}
				return "not_ready"
			}
		}
	}

	return "observed"
}

func effectiveNamespace(namespace, fallback string) string {
	namespace = strings.TrimSpace(namespace)
	if namespace != "" {
		return namespace
	}
	fallback = strings.TrimSpace(fallback)
	if fallback != "" {
		return fallback
	}
	return "default"
}

func firstString(primary map[string]any, fallback map[string]any, keys []string, defaultValue string) string {
	for _, source := range []map[string]any{primary, fallback} {
		for _, key := range keys {
			value, ok := source[key]
			if !ok {
				continue
			}
			parsed := strings.TrimSpace(stringValue(value))
			if parsed != "" {
				return parsed
			}
		}
	}
	return defaultValue
}

func firstBool(primary map[string]any, fallback map[string]any, keys []string, defaultValue bool) bool {
	for _, source := range []map[string]any{primary, fallback} {
		for _, key := range keys {
			value, ok := source[key]
			if !ok {
				continue
			}
			if parsed, ok := value.(bool); ok {
				return parsed
			}
		}
	}
	return defaultValue
}

func stringValue(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case nil:
		return ""
	default:
		return fmt.Sprint(value)
	}
}

func boolValue(value any) bool {
	typed, ok := value.(bool)
	return ok && typed
}

func mapValue(value any) map[string]any {
	typed, ok := value.(map[string]any)
	if !ok {
		return map[string]any{}
	}
	return typed
}

func objectSlice(value any) []map[string]any {
	switch typed := value.(type) {
	case []map[string]any:
		return typed
	case []any:
		out := make([]map[string]any, 0, len(typed))
		for _, item := range typed {
			if object, ok := item.(map[string]any); ok {
				out = append(out, object)
			}
		}
		return out
	default:
		return nil
	}
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

// newFakeExecutor is used by tests.
func newFakeExecutor(objects ...*unstructured.Unstructured) kubernetesExecutor {
	mapper := restmapper.NewDiscoveryRESTMapper([]*restmapper.APIGroupResources{
		{
			Group: metav1.APIGroup{
				Name: "",
				Versions: []metav1.GroupVersionForDiscovery{
					{GroupVersion: "v1", Version: "v1"},
				},
				PreferredVersion: metav1.GroupVersionForDiscovery{GroupVersion: "v1", Version: "v1"},
			},
			VersionedResources: map[string][]metav1.APIResource{
				"v1": {
					{Name: "configmaps", SingularName: "configmap", Namespaced: true, Kind: "ConfigMap"},
					{Name: "namespaces", SingularName: "namespace", Namespaced: false, Kind: "Namespace"},
					{Name: "serviceaccounts", SingularName: "serviceaccount", Namespaced: true, Kind: "ServiceAccount"},
					{Name: "secrets", SingularName: "secret", Namespaced: true, Kind: "Secret"},
				},
			},
		},
	})

	store := map[string]*unstructured.Unstructured{}
	for _, obj := range objects {
		cloned := obj.DeepCopy()
		store[fakeObjectKey(cloned)] = cloned
	}

	return &fakeExecutor{
		mapper:  mapper,
		objects: store,
	}
}

func (f *fakeExecutor) Apply(_ context.Context, objects []map[string]any, namespace string, _ string) ([]model.InstallationResourceState, error) {
	resources := make([]model.InstallationResourceState, 0, len(objects))
	for _, raw := range objects {
		obj, err := normalizeObject(raw)
		if err != nil {
			return nil, err
		}

		mapping, err := f.mapper.RESTMapping(obj.GroupVersionKind().GroupKind(), obj.GroupVersionKind().Version)
		if err != nil {
			return nil, fmt.Errorf("map %s: %w", obj.GroupVersionKind().String(), err)
		}
		if mapping.Scope != nil && mapping.Scope.Name() == meta.RESTScopeNameNamespace && obj.GetNamespace() == "" {
			obj.SetNamespace(namespace)
		}

		cloned := obj.DeepCopy()
		f.objects[fakeObjectKey(cloned)] = cloned
		resources = append(resources, stateFromObject(cloned, true, "applied"))
	}
	return resources, nil
}

func (f *fakeExecutor) Observe(_ context.Context, objects []map[string]any, namespace string) ([]model.InstallationResourceState, error) {
	resources := make([]model.InstallationResourceState, 0, len(objects))
	for _, raw := range objects {
		obj, err := normalizeObject(raw)
		if err != nil {
			return nil, err
		}

		mapping, err := f.mapper.RESTMapping(obj.GroupVersionKind().GroupKind(), obj.GroupVersionKind().Version)
		if err != nil {
			return nil, fmt.Errorf("map %s: %w", obj.GroupVersionKind().String(), err)
		}
		if mapping.Scope != nil && mapping.Scope.Name() == meta.RESTScopeNameNamespace && obj.GetNamespace() == "" {
			obj.SetNamespace(namespace)
		}

		live, ok := f.objects[fakeObjectKey(obj)]
		if !ok {
			resources = append(resources, model.InstallationResourceState{
				Kind:      obj.GetKind(),
				Namespace: obj.GetNamespace(),
				Name:      obj.GetName(),
				Status:    "not_found",
				Observed:  false,
			})
			continue
		}
		resources = append(resources, stateFromObject(live.DeepCopy(), true, deriveResourceStatus(live)))
	}
	return resources, nil
}

func fakeObjectKey(obj *unstructured.Unstructured) string {
	gvk := obj.GroupVersionKind()
	return fmt.Sprintf("%s|%s|%s|%s|%s", gvk.Group, gvk.Version, gvk.Kind, obj.GetNamespace(), obj.GetName())
}
