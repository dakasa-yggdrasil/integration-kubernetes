package adapter

import (
	"context"
	"fmt"
	"strings"
	"time"

	model "github.com/dakasa-yggdrasil/integration-kubernetes/internal/protocol"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

// OperationCreateJobFromCronJob copies a CronJob's jobTemplate into an
// ad-hoc Job. Used by the bootstrap-backup-postgres workflow's smoke
// step — proves the dump pipeline works without waiting until 03:00 UTC.
const OperationCreateJobFromCronJob = "create_job_from_cronjob"

// createJobFromCronJobFn is swappable for tests. The default implementation
// dials the cluster via the same path as buildExecutor so credential
// handling stays consistent.
var createJobFromCronJobFn = defaultCreateJobFromCronJob

var (
	cronJobGVR = schema.GroupVersionResource{Group: "batch", Version: "v1", Resource: "cronjobs"}
	jobGVR     = schema.GroupVersionResource{Group: "batch", Version: "v1", Resource: "jobs"}
)

// CreateJobFromCronJob handles the public capability dispatch: resolve the
// cluster, copy the CronJob's jobTemplate, create a fresh Job, and
// optionally poll until terminal status.
func CreateJobFromCronJob(ctx context.Context, req model.AdapterCreateJobFromCronJobRequest) (model.AdapterCreateJobFromCronJobResponse, error) {
	cfg, err := resolveClusterConfig(req.Target.InstanceSpec)
	if err != nil {
		return model.AdapterCreateJobFromCronJobResponse{}, err
	}
	return createJobFromCronJobFn(ctx, cfg, req)
}

func defaultCreateJobFromCronJob(ctx context.Context, cfg clusterConfig, req model.AdapterCreateJobFromCronJobRequest) (model.AdapterCreateJobFromCronJobResponse, error) {
	if strings.TrimSpace(req.CronJobName) == "" {
		return model.AdapterCreateJobFromCronJobResponse{}, fmt.Errorf("cronjob_name is required")
	}
	ns := effectiveNamespace(req.Namespace, cfg.DefaultNamespace)
	if ns == "" {
		return model.AdapterCreateJobFromCronJobResponse{}, fmt.Errorf("namespace is required (no default on the integration instance)")
	}

	restConfig, err := buildRESTConfig(cfg)
	if err != nil {
		return model.AdapterCreateJobFromCronJobResponse{}, err
	}
	dyn, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		return model.AdapterCreateJobFromCronJobResponse{}, fmt.Errorf("create dynamic client: %w", err)
	}

	cronJob, err := dyn.Resource(cronJobGVR).Namespace(ns).Get(ctx, req.CronJobName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return model.AdapterCreateJobFromCronJobResponse{}, fmt.Errorf("cronjob %s/%s not found", ns, req.CronJobName)
		}
		return model.AdapterCreateJobFromCronJobResponse{}, fmt.Errorf("get cronjob: %w", err)
	}

	jobSpec, found, err := unstructured.NestedMap(cronJob.Object, "spec", "jobTemplate", "spec")
	if err != nil {
		return model.AdapterCreateJobFromCronJobResponse{}, fmt.Errorf("read jobTemplate.spec: %w", err)
	}
	if !found {
		return model.AdapterCreateJobFromCronJobResponse{}, fmt.Errorf("cronjob %s/%s has no jobTemplate.spec", ns, req.CronJobName)
	}

	labels := map[string]any{
		"app.kubernetes.io/managed-by": "yggdrasil",
		"yggdrasil.io/source-cronjob":  req.CronJobName,
	}
	annotations := map[string]any{
		"yggdrasil.io/triggered-by":   "create_job_from_cronjob",
		"yggdrasil.io/source-cronjob": req.CronJobName,
		"yggdrasil.io/triggered-at":   time.Now().UTC().Format(time.RFC3339),
	}
	if req.Reason != "" {
		annotations["yggdrasil.io/trigger-reason"] = req.Reason
	}

	job := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "batch/v1",
		"kind":       "Job",
		"metadata": map[string]any{
			"generateName": req.CronJobName + "-manual-",
			"namespace":    ns,
			"labels":       labels,
			"annotations":  annotations,
		},
		"spec": jobSpec,
	}}

	created, err := dyn.Resource(jobGVR).Namespace(ns).Create(ctx, job, metav1.CreateOptions{})
	if err != nil {
		return model.AdapterCreateJobFromCronJobResponse{}, fmt.Errorf("create job: %w", err)
	}
	jobName := created.GetName()

	resp := model.AdapterCreateJobFromCronJobResponse{
		Operation:   OperationCreateJobFromCronJob,
		Status:      "created",
		JobName:     jobName,
		Namespace:   ns,
		CronJobName: req.CronJobName,
		Metadata: map[string]any{
			"provider":            Provider,
			"target_instance":     req.Target.Instance.Name,
			"target_integration":  req.Target.Type.Name,
			"wait_for_completion": req.WaitForCompletion,
		},
	}

	if !req.WaitForCompletion {
		return resp, nil
	}

	timeout := time.Duration(req.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 10 * time.Minute
	}
	deadline := time.Now().Add(timeout)
	pollInterval := 5 * time.Second

	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return resp, fmt.Errorf("context cancelled while waiting for job: %w", ctx.Err())
		case <-time.After(pollInterval):
		}

		latest, err := dyn.Resource(jobGVR).Namespace(ns).Get(ctx, jobName, metav1.GetOptions{})
		if err != nil {
			return resp, fmt.Errorf("poll job: %w", err)
		}
		succeeded, _, _ := unstructured.NestedInt64(latest.Object, "status", "succeeded")
		failed, _, _ := unstructured.NestedInt64(latest.Object, "status", "failed")

		if succeeded >= 1 {
			resp.Status = "succeeded"
			resp.Succeeded = succeeded
			return resp, nil
		}
		if failed > 0 {
			backoffLimit, _, _ := unstructured.NestedInt64(latest.Object, "spec", "backoffLimit")
			if backoffLimit > 0 && failed > backoffLimit {
				resp.Status = "failed"
				resp.Failed = failed
				return resp, fmt.Errorf("job %s failed (failed=%d > backoffLimit=%d)", jobName, failed, backoffLimit)
			}
		}
	}

	resp.Status = "timeout"
	return resp, fmt.Errorf("job %s did not finish within %s", jobName, timeout)
}

// decodeCreateJobFromCronJobRequest reshapes the generic execute request
// into the typed shape understood by CreateJobFromCronJob. The dispatcher
// in spec.go calls this before forwarding.
func decodeCreateJobFromCronJobRequest(req model.AdapterExecuteIntegrationRequest) (model.AdapterCreateJobFromCronJobRequest, error) {
	in := req.Input
	if in == nil {
		return model.AdapterCreateJobFromCronJobRequest{}, fmt.Errorf("input is required")
	}
	out := model.AdapterCreateJobFromCronJobRequest{
		Operation:         OperationCreateJobFromCronJob,
		CronJobName:       firstStringFromMap(in, "cronjob_name", "name"),
		Namespace:         firstStringFromMap(in, "namespace"),
		WaitForCompletion: firstBoolFromMap(in, "wait_for_completion"),
		TimeoutSeconds:    firstIntFromMap(in, "timeout_seconds"),
		Reason:            firstStringFromMap(in, "reason"),
		Target: model.AdapterTargetIntegrationContext{
			Type:         req.Integration.Type,
			TypeSpec:     req.Integration.TypeSpec,
			Instance:     req.Integration.Instance,
			InstanceSpec: req.Integration.InstanceSpec,
		},
	}
	if out.CronJobName == "" {
		return model.AdapterCreateJobFromCronJobRequest{}, fmt.Errorf("input.cronjob_name is required")
	}
	return out, nil
}

func firstStringFromMap(source map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, ok := source[key]; ok {
			if s, ok := value.(string); ok {
				if trimmed := strings.TrimSpace(s); trimmed != "" {
					return trimmed
				}
			}
		}
	}
	return ""
}

func firstBoolFromMap(source map[string]any, key string) bool {
	if value, ok := source[key]; ok {
		if b, ok := value.(bool); ok {
			return b
		}
	}
	return false
}

func firstIntFromMap(source map[string]any, key string) int {
	value, ok := source[key]
	if !ok {
		return 0
	}
	switch typed := value.(type) {
	case int:
		return typed
	case int64:
		return int(typed)
	case float64:
		return int(typed)
	}
	return 0
}
