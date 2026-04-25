package adapter

import (
	"context"
	"encoding/base64"
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
	"k8s.io/apimachinery/pkg/runtime/schema"
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
	AdapterVersion            = "1.2.0"
	OperationDeclarativeApply = "declarative_apply"
	// OperationApplyManifest is the single-object variant of
	// declarative_apply. Callers pass exactly one Kubernetes object under
	// with.manifest; the adapter wraps it as a 1-element slice and runs
	// the same server-side apply pipeline. integration_quickstart steps
	// (apply-service-account, apply-adapter-deployment, apply-credentials-secret)
	// compile to this operation — one manifest per step gives adopters
	// explicit, auditable YAML rather than opaque object arrays.
	OperationApplyManifest              = "apply_manifest"
	OperationObserveObjects             = "observe_objects"
	OperationEnsureDockerRegistrySecret = "ensure_docker_registry_secret"
	OperationUpdateContainerImage       = "update_container_image"
	ModeServerSideApply                 = "server_side_apply"
	DefaultFieldManager                 = "yggdrasil"

	QueueDescribe = "yggdrasil.adapter.kubernetes.describe"
	QueueExecute  = "yggdrasil.adapter.kubernetes.execute"
)

var SupportedExecuteOperations = []string{
	OperationDeclarativeApply,
	OperationApplyManifest,
	OperationObserveObjects,
	OperationEnsureDockerRegistrySecret,
	OperationUpdateContainerImage,
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
	List(ctx context.Context, selectors []model.LabelSelector, defaultNamespace string) ([]model.InstallationResourceState, error)
	UpsertDockerRegistrySecret(ctx context.Context, req model.AdapterEnsureDockerRegistrySecretRequest) (model.InstallationResourceState, error)
	UpdateContainerImage(ctx context.Context, req model.AdapterUpdateContainerImageRequest) (model.InstallationResourceState, error)
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
		Provider:     Provider,
		Adapter:      adapterSpec,
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
		Objects:        objects,
		Namespace:      stringValue(req.Input["namespace"]),
		Reconcile:      decodeGenericReconcile(req.Input["reconcile"]),
		ImageOverrides: decodeImageOverrides(req.Input["image_overrides"]),
	}, nil
}

// decodeImageOverrides turns the inbound map[string]any (from RPC JSON
// input) into the map[string]string shape the apply pipeline expects.
// Silently drops any non-string values — the contract says keys and
// values are both strings (old_ref → new_ref).
func decodeImageOverrides(raw any) map[string]string {
	m, ok := raw.(map[string]any)
	if !ok || len(m) == 0 {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		if s, ok := v.(string); ok && strings.TrimSpace(s) != "" {
			out[k] = s
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
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

func decodeEnsureDockerRegistrySecretRequest(req model.AdapterExecuteIntegrationRequest) (model.AdapterEnsureDockerRegistrySecretRequest, error) {
	if req.Integration.Instance.Name == "" || req.Integration.Type.Name == "" {
		return model.AdapterEnsureDockerRegistrySecretRequest{}, fmt.Errorf("ensure_docker_registry_secret requires integration context")
	}
	namespace := strings.TrimSpace(stringValue(req.Input["namespace"]))
	secretName := strings.TrimSpace(stringValue(req.Input["secret_name"]))
	registry := strings.TrimSpace(stringValue(req.Input["registry"]))
	username := strings.TrimSpace(stringValue(req.Input["username"]))
	password := strings.TrimSpace(stringValue(req.Input["password"]))
	if namespace == "" || secretName == "" || registry == "" || username == "" || password == "" {
		return model.AdapterEnsureDockerRegistrySecretRequest{}, fmt.Errorf("ensure_docker_registry_secret requires namespace, secret_name, registry, username, password")
	}
	return model.AdapterEnsureDockerRegistrySecretRequest{
		Operation: OperationEnsureDockerRegistrySecret,
		Context:   decodeGenericInstallationContext(req.Input["context"]),
		Target: model.AdapterTargetIntegrationContext{
			Type:         req.Integration.Type,
			TypeSpec:     req.Integration.TypeSpec,
			Instance:     req.Integration.Instance,
			InstanceSpec: req.Integration.InstanceSpec,
		},
		Namespace:  namespace,
		SecretName: secretName,
		Registry:   registry,
		Username:   username,
		Password:   password,
	}, nil
}

func decodeUpdateContainerImageRequest(req model.AdapterExecuteIntegrationRequest) (model.AdapterUpdateContainerImageRequest, error) {
	if req.Integration.Instance.Name == "" || req.Integration.Type.Name == "" {
		return model.AdapterUpdateContainerImageRequest{}, fmt.Errorf("update_container_image requires integration context")
	}
	ns := strings.TrimSpace(stringValue(req.Input["namespace"]))
	deployment := strings.TrimSpace(stringValue(req.Input["deployment_name"]))
	container := strings.TrimSpace(stringValue(req.Input["container_name"]))
	image := strings.TrimSpace(stringValue(req.Input["image"]))
	pullPolicy := strings.TrimSpace(stringValue(req.Input["image_pull_policy"]))
	if ns == "" || deployment == "" || container == "" || image == "" {
		return model.AdapterUpdateContainerImageRequest{}, fmt.Errorf("update_container_image requires namespace, deployment_name, container_name, image")
	}
	return model.AdapterUpdateContainerImageRequest{
		Operation:       OperationUpdateContainerImage,
		Context:         decodeGenericInstallationContext(req.Input["context"]),
		Target: model.AdapterTargetIntegrationContext{
			Type:         req.Integration.Type,
			TypeSpec:     req.Integration.TypeSpec,
			Instance:     req.Integration.Instance,
			InstanceSpec: req.Integration.InstanceSpec,
		},
		Namespace:       ns,
		DeploymentName:  deployment,
		ContainerName:   container,
		Image:           image,
		ImagePullPolicy: pullPolicy,
	}, nil
}

func decodeGenericObserveObjectsRequest(req model.AdapterExecuteIntegrationRequest) (model.AdapterObserveObjectsRequest, error) {
	if req.Integration.Instance.Name == "" || req.Integration.Type.Name == "" {
		return model.AdapterObserveObjectsRequest{}, fmt.Errorf("generic observe_objects requires integration context")
	}

	objects := objectSlice(req.Input["objects"])
	selectors := decodeLabelSelectors(req.Input["label_selectors"])
	if len(objects) == 0 && len(selectors) == 0 {
		return model.AdapterObserveObjectsRequest{}, fmt.Errorf("generic observe_objects requires at least one object or label_selector")
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
		Objects:        objects,
		Namespace:      stringValue(req.Input["namespace"]),
		LabelSelectors: selectors,
	}, nil
}

func decodeLabelSelectors(raw any) []model.LabelSelector {
	slice, ok := raw.([]any)
	if !ok {
		return nil
	}
	out := make([]model.LabelSelector, 0, len(slice))
	for _, item := range slice {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		matchLabelsRaw, _ := m["match_labels"].(map[string]any)
		ml := make(map[string]string, len(matchLabelsRaw))
		for k, v := range matchLabelsRaw {
			if s, ok := v.(string); ok {
				ml[k] = s
			}
		}
		if len(ml) == 0 {
			continue // skip selectors with empty match_labels — too broad
		}
		out = append(out, model.LabelSelector{
			APIVersion:  strings.TrimSpace(stringValue(m["api_version"])),
			Kind:        strings.TrimSpace(stringValue(m["kind"])),
			Namespace:   strings.TrimSpace(stringValue(m["namespace"])),
			MatchLabels: ml,
		})
	}
	return out
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
	case OperationEnsureDockerRegistrySecret:
		typedReq, err := decodeEnsureDockerRegistrySecretRequest(req)
		if err != nil {
			return model.AdapterExecuteIntegrationResponse{}, err
		}
		response, err := EnsureDockerRegistrySecret(ctx, typedReq)
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
	case OperationUpdateContainerImage:
		typedReq, err := decodeUpdateContainerImageRequest(req)
		if err != nil {
			return model.AdapterExecuteIntegrationResponse{}, err
		}
		response, err := UpdateContainerImage(ctx, typedReq)
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

	objects := req.Objects
	if len(req.ImageOverrides) > 0 {
		objects = applyImageOverrides(objects, req.ImageOverrides)
	}

	resources, err := executor.Apply(ctx, objects, effectiveNamespace(req.Namespace, cfg.DefaultNamespace), fieldManager)
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
			"image_overrides":    len(req.ImageOverrides),
		},
	}, nil
}

// applyImageOverrides rewrites container `image:` fields in-place based on
// the overrides map. Match rules:
//   - Full match: exact current image ref ("dakasa-identities:latest") → new.
//   - Prefix match: the container image's repo component matches any
//     override key up to ":". Typical CD use: the CD sends
//     "153...amazonaws.com/dakasa/identities": "153...amazonaws.com/dakasa/identities:sha-abc",
//     but the desired manifest reads "dakasa-identities:latest". Neither
//     matches by key name, so we ALSO try matching by tag-stripped image.
//
// This walks spec.template.spec.containers and .initContainers of any
// object with a `spec.template.spec` field (Deployment, StatefulSet,
// DaemonSet, Job via their spec.template, etc.).
func applyImageOverrides(objects []map[string]any, overrides map[string]string) []map[string]any {
	if len(overrides) == 0 {
		return objects
	}
	out := make([]map[string]any, len(objects))
	for i, obj := range objects {
		cloned := deepCloneMap(obj)
		rewriteContainerImages(cloned, overrides)
		out[i] = cloned
	}
	return out
}

func rewriteContainerImages(obj map[string]any, overrides map[string]string) {
	podSpec := extractPodSpec(obj)
	if podSpec == nil {
		return
	}
	for _, key := range []string{"containers", "initContainers"} {
		containers, ok := podSpec[key].([]any)
		if !ok {
			continue
		}
		for _, c := range containers {
			cmap, ok := c.(map[string]any)
			if !ok {
				continue
			}
			oldImage, _ := cmap["image"].(string)
			if oldImage == "" {
				continue
			}
			if newImage := matchOverride(oldImage, overrides); newImage != "" {
				cmap["image"] = newImage
			}
		}
	}
}

// extractPodSpec finds the pod spec under common paths:
//   - Deployment/StatefulSet/DaemonSet/Job/CronJob: spec.template.spec
//   - Pod: spec
//   - CronJob: spec.jobTemplate.spec.template.spec
func extractPodSpec(obj map[string]any) map[string]any {
	spec, _ := obj["spec"].(map[string]any)
	if spec == nil {
		return nil
	}
	if template, ok := spec["template"].(map[string]any); ok {
		if ps, ok := template["spec"].(map[string]any); ok {
			return ps
		}
	}
	if jt, ok := spec["jobTemplate"].(map[string]any); ok {
		if tj, ok := jt["spec"].(map[string]any); ok {
			if tmpl, ok := tj["template"].(map[string]any); ok {
				if ps, ok := tmpl["spec"].(map[string]any); ok {
					return ps
				}
			}
		}
	}
	if kind, _ := obj["kind"].(string); kind == "Pod" {
		return spec
	}
	return nil
}

func matchOverride(image string, overrides map[string]string) string {
	if v, ok := overrides[image]; ok {
		return v
	}
	// Prefix match: override key is a repo ref (no tag); image has a tag
	// to strip. Also try reverse — image may be full ref and key a prefix.
	stripped := strings.SplitN(image, ":", 2)[0]
	if v, ok := overrides[stripped]; ok {
		return v
	}
	// If image is a short name like "foo:latest" and override is
	// keyed by a base path or a different ref, fall back to a simple
	// last-path-segment match (e.g. override key "foo" matches image
	// "foo:latest" or "any/registry/foo:tag").
	segments := strings.Split(stripped, "/")
	if len(segments) > 0 {
		last := segments[len(segments)-1]
		if v, ok := overrides[last]; ok {
			return v
		}
	}
	return ""
}

func deepCloneMap(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = deepCloneValue(v)
	}
	return out
}

func deepCloneValue(v any) any {
	switch val := v.(type) {
	case map[string]any:
		return deepCloneMap(val)
	case []any:
		cloned := make([]any, len(val))
		for i, item := range val {
			cloned[i] = deepCloneValue(item)
		}
		return cloned
	default:
		return val
	}
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

	ns := effectiveNamespace(req.Namespace, cfg.DefaultNamespace)
	resources, err := executor.Observe(ctx, req.Objects, ns)
	if err != nil {
		return model.AdapterObserveObjectsResponse{}, err
	}

	if len(req.LabelSelectors) > 0 {
		listed, err := executor.List(ctx, req.LabelSelectors, ns)
		if err != nil {
			return model.AdapterObserveObjectsResponse{}, err
		}
		resources = append(resources, listed...)
	}

	var status string
	var observed bool
	if len(resources) == 0 {
		observed = false
		status = "not_found"
	} else {
		allObserved := true
		allReady := true
		for _, r := range resources {
			if !r.Observed {
				allObserved = false
			}
			if r.Status != "ready" && r.Status != "applied" && r.Status != "observed" {
				allReady = false
			}
		}
		switch {
		case !allObserved:
			status = "partial"
		case !allReady:
			status = "not_ready"
		default:
			status = "observed"
		}
		observed = allObserved
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

// EnsureDockerRegistrySecret idempotently creates or updates a Kubernetes
// Secret of type kubernetes.io/dockerconfigjson for image pull authentication.
func EnsureDockerRegistrySecret(ctx context.Context, req model.AdapterEnsureDockerRegistrySecretRequest) (model.AdapterEnsureDockerRegistrySecretResponse, error) {
	cfg, err := resolveClusterConfig(req.Target.InstanceSpec)
	if err != nil {
		return model.AdapterEnsureDockerRegistrySecretResponse{}, err
	}

	executor, err := newKubernetesExecutor(cfg)
	if err != nil {
		return model.AdapterEnsureDockerRegistrySecretResponse{}, err
	}

	state, err := executor.UpsertDockerRegistrySecret(ctx, req)
	if err != nil {
		return model.AdapterEnsureDockerRegistrySecretResponse{}, err
	}

	return model.AdapterEnsureDockerRegistrySecretResponse{
		Operation: OperationEnsureDockerRegistrySecret,
		Status:    state.Status,
		Resource:  state,
		Metadata: map[string]any{
			"provider":           Provider,
			"secret_name":        req.SecretName,
			"namespace":          req.Namespace,
			"registry":           req.Registry,
			"target_instance":    req.Target.Instance.Name,
			"target_integration": req.Target.Type.Name,
		},
	}, nil
}

// UpdateContainerImage patches a single container's image (and optionally
// imagePullPolicy) in an existing Deployment via strategic-merge patch by
// container name. The pre-check that the container name exists before patching
// eliminates the sparse-SSA "silent duplicate container" footgun.
func UpdateContainerImage(ctx context.Context, req model.AdapterUpdateContainerImageRequest) (model.AdapterUpdateContainerImageResponse, error) {
	cfg, err := resolveClusterConfig(req.Target.InstanceSpec)
	if err != nil {
		return model.AdapterUpdateContainerImageResponse{}, err
	}

	executor, err := newKubernetesExecutor(cfg)
	if err != nil {
		return model.AdapterUpdateContainerImageResponse{}, err
	}

	state, err := executor.UpdateContainerImage(ctx, req)
	if err != nil {
		return model.AdapterUpdateContainerImageResponse{}, err
	}

	return model.AdapterUpdateContainerImageResponse{
		Operation: OperationUpdateContainerImage,
		Status:    state.Status,
		Resource:  state,
		Metadata: map[string]any{
			"provider":        Provider,
			"namespace":       req.Namespace,
			"deployment_name": req.DeploymentName,
			"container_name":  req.ContainerName,
			"image":           req.Image,
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
		{
			Name:          OperationEnsureDockerRegistrySecret,
			Description:   "Create or update a Kubernetes Secret of type kubernetes.io/dockerconfigjson for image pull authentication.",
			ResourceTypes: []string{"object"},
			Idempotent:    true,
		},
		{
			Name:          OperationUpdateContainerImage,
			Description:   "Update the image (and optionally imagePullPolicy) of a named container in an existing Deployment via strategic-merge patch by container name. Replaces fragile sparse-SSA upgrades.",
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

// List returns the set of objects matching each selector. Kinds that
// the RESTMapper does not recognize surface as a single "not_found"
// state so callers can distinguish "selector matched nothing" (empty
// slice appended) from "selector refers to an unknown type" (one
// not_found entry).
func (e *dynamicExecutor) List(ctx context.Context, selectors []model.LabelSelector, defaultNamespace string) ([]model.InstallationResourceState, error) {
	resources := make([]model.InstallationResourceState, 0)
	for _, selector := range selectors {
		ns := selector.Namespace
		if strings.TrimSpace(ns) == "" {
			ns = defaultNamespace
		}
		gv, err := schema.ParseGroupVersion(selector.APIVersion)
		if err != nil {
			return nil, fmt.Errorf("parse api_version %q: %w", selector.APIVersion, err)
		}
		gk := schema.GroupKind{Group: gv.Group, Kind: selector.Kind}
		mapping, err := e.mapper.RESTMapping(gk, gv.Version)
		if err != nil {
			resources = append(resources, model.InstallationResourceState{
				Kind:      selector.Kind,
				Namespace: ns,
				Status:    "not_found",
				Observed:  false,
			})
			continue
		}
		resourceClient := e.resourceInterface(mapping, ns)
		labelSelector := metav1.FormatLabelSelector(&metav1.LabelSelector{MatchLabels: selector.MatchLabels})
		list, err := resourceClient.List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
		if err != nil {
			return nil, fmt.Errorf("list %s/%s: %w", selector.Kind, labelSelector, err)
		}
		for i := range list.Items {
			item := &list.Items[i]
			resources = append(resources, stateFromObject(item, true, deriveResourceStatus(item)))
		}
	}
	return resources, nil
}

func (e *dynamicExecutor) UpsertDockerRegistrySecret(ctx context.Context, req model.AdapterEnsureDockerRegistrySecretRequest) (model.InstallationResourceState, error) {
	payload, err := BuildDockerConfigJSON(req.Registry, req.Username, req.Password)
	if err != nil {
		return model.InstallationResourceState{}, fmt.Errorf("build dockerconfigjson: %w", err)
	}
	obj := map[string]any{
		"apiVersion": "v1",
		"kind":       "Secret",
		"type":       "kubernetes.io/dockerconfigjson",
		"metadata": map[string]any{
			"name":      req.SecretName,
			"namespace": req.Namespace,
		},
		"data": map[string]any{
			".dockerconfigjson": base64.StdEncoding.EncodeToString(payload),
		},
	}
	// Delegate to the same apply pipeline as declarative_apply. The
	// fieldManager here is fixed — users cannot override it for this
	// op, because the Secret contents are fully opaque to them.
	states, err := e.Apply(ctx, []map[string]any{obj}, req.Namespace, DefaultFieldManager)
	if err != nil {
		return model.InstallationResourceState{}, err
	}
	if len(states) == 0 {
		return model.InstallationResourceState{}, fmt.Errorf("apply returned no states")
	}
	return states[0], nil
}

func (e *dynamicExecutor) UpdateContainerImage(ctx context.Context, req model.AdapterUpdateContainerImageRequest) (model.InstallationResourceState, error) {
	// 1. Build the strategic-merge patch body — only the fields we want to update.
	container := map[string]any{
		"name":  req.ContainerName,
		"image": req.Image,
	}
	if req.ImagePullPolicy != "" {
		container["imagePullPolicy"] = req.ImagePullPolicy
	}
	patchBody := map[string]any{
		"spec": map[string]any{
			"template": map[string]any{
				"spec": map[string]any{
					"containers": []any{container},
				},
			},
		},
	}
	patchBytes, err := json.Marshal(patchBody)
	if err != nil {
		return model.InstallationResourceState{}, fmt.Errorf("marshal patch: %w", err)
	}

	// 2. Resolve the Deployment GVR and patch via dynamic client.
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata":   map[string]any{"name": req.DeploymentName, "namespace": req.Namespace},
	}}
	mapping, err := e.mapper.RESTMapping(obj.GroupVersionKind().GroupKind(), obj.GroupVersionKind().Version)
	if err != nil {
		return model.InstallationResourceState{}, fmt.Errorf("resolve Deployment GVR: %w", err)
	}
	resourceClient := e.resourceInterface(mapping, req.Namespace)

	// 3. Pre-check: read the existing Deployment to verify the container name
	//    exists. This is what eliminates the SSA footgun — we explicitly fail
	//    if the container name is wrong, instead of silently appending a new
	//    container to the pod.
	existing, err := resourceClient.Get(ctx, req.DeploymentName, metav1.GetOptions{})
	if err != nil {
		return model.InstallationResourceState{}, fmt.Errorf("get Deployment %s/%s: %w", req.Namespace, req.DeploymentName, err)
	}
	found := false
	if spec, ok := existing.Object["spec"].(map[string]any); ok {
		if tmpl, ok := spec["template"].(map[string]any); ok {
			if podSpec, ok := tmpl["spec"].(map[string]any); ok {
				if containers, ok := podSpec["containers"].([]any); ok {
					for _, raw := range containers {
						if c, ok := raw.(map[string]any); ok && c["name"] == req.ContainerName {
							found = true
							break
						}
					}
				}
			}
		}
	}
	if !found {
		return model.InstallationResourceState{}, fmt.Errorf("container %q not found in Deployment %s/%s — cannot update (strategic-merge patch by container name requires existing container)", req.ContainerName, req.Namespace, req.DeploymentName)
	}

	// 4. Apply the strategic-merge patch.
	patched, err := resourceClient.Patch(ctx, req.DeploymentName, types.StrategicMergePatchType, patchBytes, metav1.PatchOptions{
		FieldManager: DefaultFieldManager,
	})
	if err != nil {
		return model.InstallationResourceState{}, fmt.Errorf("patch Deployment %s/%s: %w", req.Namespace, req.DeploymentName, err)
	}
	return stateFromObject(patched, true, "updated"), nil
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

	// Surface the raw .status block so callers can diagnose why an
	// object is "not_ready" (Pod conditions + container statuses
	// include ImagePullBackOff, CrashLoopBackOff messages). This is
	// the only way to observe pod-level failures when the pod name
	// is only known via its Deployment's ReplicaSet.
	if rawStatus, ok := obj.Object["status"].(map[string]any); ok {
		metadata["status"] = rawStatus
	}

	// Also surface the raw .spec block. Declarative operators often
	// need to compare desired vs. observed at the spec level (e.g.
	// the image tag of a Deployment's container template) without
	// issuing a second API round-trip. Nil-safe: only set the key
	// when .spec is present.
	if rawSpec, ok := obj.Object["spec"].(map[string]any); ok {
		metadata["spec"] = rawSpec
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

func (f *fakeExecutor) List(_ context.Context, selectors []model.LabelSelector, defaultNamespace string) ([]model.InstallationResourceState, error) {
	resources := make([]model.InstallationResourceState, 0)
	for _, selector := range selectors {
		ns := selector.Namespace
		if strings.TrimSpace(ns) == "" {
			ns = defaultNamespace
		}
		for _, obj := range f.objects {
			if obj.GetKind() != selector.Kind {
				continue
			}
			if obj.GetNamespace() != ns {
				continue
			}
			if !labelsMatch(obj.GetLabels(), selector.MatchLabels) {
				continue
			}
			resources = append(resources, stateFromObject(obj, true, deriveResourceStatus(obj)))
		}
	}
	return resources, nil
}

func (f *fakeExecutor) UpsertDockerRegistrySecret(_ context.Context, req model.AdapterEnsureDockerRegistrySecretRequest) (model.InstallationResourceState, error) {
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "Secret",
		"metadata": map[string]any{
			"name":      req.SecretName,
			"namespace": req.Namespace,
		},
		"type": "kubernetes.io/dockerconfigjson",
	}}
	f.objects[fakeObjectKey(obj)] = obj
	return stateFromObject(obj, true, "ready"), nil
}

func (f *fakeExecutor) UpdateContainerImage(_ context.Context, req model.AdapterUpdateContainerImageRequest) (model.InstallationResourceState, error) {
	fakeObj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata":   map[string]any{"name": req.DeploymentName, "namespace": req.Namespace},
	}}
	key := fakeObjectKey(fakeObj)
	existing, ok := f.objects[key]
	if !ok {
		return model.InstallationResourceState{}, fmt.Errorf("Deployment %s/%s not found", req.Namespace, req.DeploymentName)
	}
	spec, _ := existing.Object["spec"].(map[string]any)
	template, _ := spec["template"].(map[string]any)
	podSpec, _ := template["spec"].(map[string]any)
	containers, _ := podSpec["containers"].([]any)
	found := false
	for i, raw := range containers {
		c, _ := raw.(map[string]any)
		if c["name"] == req.ContainerName {
			c["image"] = req.Image
			if req.ImagePullPolicy != "" {
				c["imagePullPolicy"] = req.ImagePullPolicy
			}
			containers[i] = c
			found = true
			break
		}
	}
	if !found {
		return model.InstallationResourceState{}, fmt.Errorf("container %q not found in Deployment %s/%s", req.ContainerName, req.Namespace, req.DeploymentName)
	}
	return stateFromObject(existing, true, "updated"), nil
}

func labelsMatch(have map[string]string, want map[string]string) bool {
	for k, v := range want {
		if have[k] != v {
			return false
		}
	}
	return true
}

func fakeObjectKey(obj *unstructured.Unstructured) string {
	gvk := obj.GroupVersionKind()
	return fmt.Sprintf("%s|%s|%s|%s|%s", gvk.Group, gvk.Version, gvk.Kind, obj.GetNamespace(), obj.GetName())
}
