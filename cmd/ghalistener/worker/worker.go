package worker

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/actions/actions-runner-controller/apis/actions.github.com/v1alpha1"
	"github.com/actions/actions-runner-controller/cmd/ghalistener/listener"
	"github.com/actions/actions-runner-controller/github/actions"
	"github.com/actions/actions-runner-controller/logging"
	jsonpatch "github.com/evanphx/json-patch"
	"github.com/go-logr/logr"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const workerName = "kubernetesworker"

type Option func(*Worker)

func WithLogger(logger logr.Logger) Option {
	return func(w *Worker) {
		logger = logger.WithName(workerName)
		w.logger = &logger
	}
}

type Config struct {
	EphemeralRunnerSetNamespace string
	EphemeralRunnerSetName      string
	MaxRunners                  int
	MinRunners                  int
}

// The Worker's role is to process the messages it receives from the listener.
// It then initiates Kubernetes API requests to carry out the necessary actions.
type Worker struct {
	clientset *kubernetes.Clientset
	config    Config
	lastPatch int
	patchSeq  int
	logger    *logr.Logger
}

var _ listener.Handler = (*Worker)(nil)

func New(config Config, options ...Option) (*Worker, error) {
	w := &Worker{
		config:    config,
		lastPatch: -1,
		patchSeq:  -1,
	}

	conf, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}

	clientset, err := kubernetes.NewForConfig(conf)
	if err != nil {
		return nil, err
	}

	w.clientset = clientset

	for _, option := range options {
		option(w)
	}

	if err := w.applyDefaults(); err != nil {
		return nil, err
	}

	return w, nil
}

func (w *Worker) applyDefaults() error {
	if w.logger == nil {
		logger, err := logging.NewLogger(logging.LogLevelDebug, logging.LogFormatJSON)
		if err != nil {
			return fmt.Errorf("NewLogger failed: %w", err)
		}
		logger = logger.WithName(workerName)
		w.logger = &logger
	}

	return nil
}

// HandleJobStarted updates the job information for the ephemeral runner when a job is started.
// It takes a context and a jobInfo parameter which contains the details of the started job.
// This update marks the ephemeral runner so that the controller would have more context
// about the ephemeral runner that should not be deleted when scaling down.
// It returns an error if there is any issue with updating the job information.
func (w *Worker) HandleJobStarted(ctx context.Context, jobInfo *actions.JobStarted) error {
	w.logger.Info("🏃 WORKER: HANDLING JOB STARTED",
		"runnerName", jobInfo.RunnerName,
		"runnerRequestId", jobInfo.RunnerRequestId,
		"runnerId", jobInfo.RunnerId,
		"ownerName", jobInfo.OwnerName,
		"repositoryName", jobInfo.RepositoryName,
		"jobWorkflowRef", jobInfo.JobWorkflowRef,
		"workflowRunId", jobInfo.WorkflowRunId,
		"jobDisplayName", jobInfo.JobDisplayName,
		"eventName", jobInfo.EventName,
		"requestLabels", jobInfo.RequestLabels,
		"queueTime", jobInfo.QueueTime,
		"runnerAssignTime", jobInfo.RunnerAssignTime,
		"ephemeralRunnerSetName", w.config.EphemeralRunnerSetName,
		"ephemeralRunnerSetNamespace", w.config.EphemeralRunnerSetNamespace)

	original, err := json.Marshal(&v1alpha1.EphemeralRunner{})
	if err != nil {
		w.logger.Error(err, "❌ WORKER: FAILED TO MARSHAL EMPTY EPHEMERAL RUNNER")
		return fmt.Errorf("failed to marshal empty ephemeral runner: %w", err)
	}

	patch, err := json.Marshal(
		&v1alpha1.EphemeralRunner{
			Status: v1alpha1.EphemeralRunnerStatus{
				JobRequestId:      jobInfo.RunnerRequestId,
				JobRepositoryName: fmt.Sprintf("%s/%s", jobInfo.OwnerName, jobInfo.RepositoryName),
				WorkflowRunId:     jobInfo.WorkflowRunId,
				JobWorkflowRef:    jobInfo.JobWorkflowRef,
				JobDisplayName:    jobInfo.JobDisplayName,
			},
		},
	)
	if err != nil {
		w.logger.Error(err, "❌ WORKER: FAILED TO MARSHAL EPHEMERAL RUNNER PATCH")
		return fmt.Errorf("failed to marshal ephemeral runner patch: %w", err)
	}

	mergePatch, err := jsonpatch.CreateMergePatch(original, patch)
	if err != nil {
		w.logger.Error(err, "❌ WORKER: FAILED TO CREATE MERGE PATCH")
		return fmt.Errorf("failed to create merge patch json for ephemeral runner: %w", err)
	}

	w.logger.Info("🔧 WORKER: UPDATING EPHEMERAL RUNNER STATUS",
		"runnerName", jobInfo.RunnerName,
		"mergePatch", string(mergePatch),
		"jobRequestId", jobInfo.RunnerRequestId,
		"jobRepositoryName", fmt.Sprintf("%s/%s", jobInfo.OwnerName, jobInfo.RepositoryName))

	patchedStatus := &v1alpha1.EphemeralRunner{}
	err = w.clientset.RESTClient().
		Patch(types.MergePatchType).
		Prefix("apis", v1alpha1.GroupVersion.Group, v1alpha1.GroupVersion.Version).
		Namespace(w.config.EphemeralRunnerSetNamespace).
		Resource("EphemeralRunners").
		Name(jobInfo.RunnerName).
		SubResource("status").
		Body(mergePatch).
		Do(ctx).
		Into(patchedStatus)
	if err != nil {
		if kerrors.IsNotFound(err) {
			w.logger.Info("⚠️ WORKER: EPHEMERAL RUNNER NOT FOUND - SKIPPING PATCH",
				"runnerName", jobInfo.RunnerName,
				"reason", "Runner may have been deleted or not yet created")
			return nil
		}
		w.logger.Error(err, "❌ WORKER: EPHEMERAL RUNNER STATUS PATCH FAILED",
			"runnerName", jobInfo.RunnerName,
			"mergePatch", string(mergePatch))
		return fmt.Errorf("could not patch ephemeral runner status, patch JSON: %s, error: %w", string(mergePatch), err)
	}

	w.logger.Info("✅ WORKER: EPHEMERAL RUNNER STATUS UPDATED",
		"runnerName", jobInfo.RunnerName,
		"jobRequestId", jobInfo.RunnerRequestId,
		"patchedJobRequestId", patchedStatus.Status.JobRequestId,
		"patchedJobRepository", patchedStatus.Status.JobRepositoryName)

	return nil
}

// HandleDesiredRunnerCount handles the desired runner count by scaling the ephemeral runner set.
// The function calculates the target runner count based on the minimum and maximum runner count configuration.
// If the target runner count is the same as the last patched count, it skips patching and returns nil.
// Otherwise, it creates a merge patch JSON for updating the ephemeral runner set with the desired count.
// The function then scales the ephemeral runner set by applying the merge patch.
// Finally, it logs the scaled ephemeral runner set details and returns nil if successful.
// If any error occurs during the process, it returns an error with a descriptive message.
func (w *Worker) HandleDesiredRunnerCount(ctx context.Context, count, jobsCompleted int) (int, error) {
	w.logger.Info("🎯 WORKER: CALCULATING DESIRED RUNNER COUNT",
		"assignedJobsCount", count,
		"jobsCompleted", jobsCompleted,
		"minRunners", w.config.MinRunners,
		"maxRunners", w.config.MaxRunners,
		"currentLastPatch", w.lastPatch,
		"currentPatchSeq", w.patchSeq,
		"ephemeralRunnerSetName", w.config.EphemeralRunnerSetName,
		"ephemeralRunnerSetNamespace", w.config.EphemeralRunnerSetNamespace)

	patchID := w.setDesiredWorkerState(count, jobsCompleted)

	w.logger.Info("📊 WORKER: SCALING CALCULATION COMPLETE",
		"assignedJobsCount", count,
		"jobsCompleted", jobsCompleted,
		"calculatedTargetRunners", w.lastPatch,
		"patchID", patchID,
		"minRunners", w.config.MinRunners,
		"maxRunners", w.config.MaxRunners)

	original, err := json.Marshal(
		&v1alpha1.EphemeralRunnerSet{
			Spec: v1alpha1.EphemeralRunnerSetSpec{
				Replicas: -1,
				PatchID:  -1,
			},
		},
	)
	if err != nil {
		w.logger.Error(err, "❌ WORKER: FAILED TO MARSHAL EMPTY EPHEMERAL RUNNER SET")
		return 0, fmt.Errorf("failed to marshal empty ephemeral runner set: %w", err)
	}

	patch, err := json.Marshal(
		&v1alpha1.EphemeralRunnerSet{
			Spec: v1alpha1.EphemeralRunnerSetSpec{
				Replicas: w.lastPatch,
				PatchID:  patchID,
			},
		},
	)
	if err != nil {
		w.logger.Error(err, "❌ WORKER: FAILED TO MARSHAL PATCH EPHEMERAL RUNNER SET")
		return 0, err
	}

	w.logger.Info("🔧 WORKER: CREATING MERGE PATCH",
		"originalReplicas", -1,
		"targetReplicas", w.lastPatch,
		"patchID", patchID,
		"original", string(original),
		"patch", string(patch))

	mergePatch, err := jsonpatch.CreateMergePatch(original, patch)
	if err != nil {
		w.logger.Error(err, "❌ WORKER: FAILED TO CREATE MERGE PATCH")
		return 0, fmt.Errorf("failed to create merge patch json for ephemeral runner set: %w", err)
	}

	w.logger.Info("📤 WORKER: APPLYING EPHEMERAL RUNNER SET PATCH",
		"ephemeralRunnerSetName", w.config.EphemeralRunnerSetName,
		"ephemeralRunnerSetNamespace", w.config.EphemeralRunnerSetNamespace,
		"mergePatch", string(mergePatch),
		"targetReplicas", w.lastPatch,
		"patchID", patchID)

	patchedEphemeralRunnerSet := &v1alpha1.EphemeralRunnerSet{}
	err = w.clientset.RESTClient().
		Patch(types.MergePatchType).
		Prefix("apis", v1alpha1.GroupVersion.Group, v1alpha1.GroupVersion.Version).
		Namespace(w.config.EphemeralRunnerSetNamespace).
		Resource("ephemeralrunnersets").
		Name(w.config.EphemeralRunnerSetName).
		Body([]byte(mergePatch)).
		Do(ctx).
		Into(patchedEphemeralRunnerSet)
	if err != nil {
		w.logger.Error(err, "❌ WORKER: EPHEMERAL RUNNER SET PATCH FAILED",
			"ephemeralRunnerSetName", w.config.EphemeralRunnerSetName,
			"ephemeralRunnerSetNamespace", w.config.EphemeralRunnerSetNamespace,
			"mergePatch", string(mergePatch),
			"targetReplicas", w.lastPatch)
		return 0, fmt.Errorf("could not patch ephemeral runner set , patch JSON: %s, error: %w", string(mergePatch), err)
	}

	w.logger.Info("✅ WORKER: EPHEMERAL RUNNER SET SCALED",
		"ephemeralRunnerSetName", w.config.EphemeralRunnerSetName,
		"ephemeralRunnerSetNamespace", w.config.EphemeralRunnerSetNamespace,
		"actualReplicas", patchedEphemeralRunnerSet.Spec.Replicas,
		"targetReplicas", w.lastPatch,
		"patchID", patchID,
		"actualPatchID", patchedEphemeralRunnerSet.Spec.PatchID,
		"assignedJobsCount", count,
		"jobsCompleted", jobsCompleted)

	return w.lastPatch, nil
}

// calculateDesiredState calculates the desired state of the worker based on the desired count and the the number of jobs completed.
func (w *Worker) setDesiredWorkerState(count, jobsCompleted int) int {
	w.logger.Info("🧮 WORKER: STARTING CALCULATION",
		"assignedJobs", count,
		"jobsCompleted", jobsCompleted,
		"minRunners", w.config.MinRunners,
		"maxRunners", w.config.MaxRunners,
		"currentLastPatch", w.lastPatch,
		"currentPatchSeq", w.patchSeq)

	// Max runners should always be set by the resource builder either to the configured value,
	// or the maximum int32 (resourcebuilder.newAutoScalingListener()).
	targetRunnerCount := min(w.config.MinRunners+count, w.config.MaxRunners)

	w.logger.Info("🎯 WORKER: INITIAL TARGET CALCULATION",
		"minRunners", w.config.MinRunners,
		"assignedJobs", count,
		"sum", w.config.MinRunners+count,
		"maxRunners", w.config.MaxRunners,
		"initialTarget", targetRunnerCount)

	w.patchSeq++
	desiredPatchID := w.patchSeq

	if count == 0 && jobsCompleted == 0 { // empty batch
		w.logger.Info("📭 WORKER: EMPTY BATCH DETECTED",
			"assignedJobs", count,
			"jobsCompleted", jobsCompleted,
			"currentLastPatch", w.lastPatch,
			"initialTarget", targetRunnerCount)

		targetRunnerCount = max(w.lastPatch, targetRunnerCount)

		w.logger.Info("🔄 WORKER: EMPTY BATCH TARGET ADJUSTED",
			"adjustedTarget", targetRunnerCount,
			"previousPatch", w.lastPatch,
			"minRunners", w.config.MinRunners)

		if targetRunnerCount == w.config.MinRunners {
			// We have an empty batch, and the last patch was the min runners.
			// Since this is an empty batch, and we are at the min runners, they should all be idle.
			// If controller created few more pods on accident (during scale down events),
			// this situation allows the controller to scale down to the min runners.
			// However, it is important to keep the patch sequence increasing so we don't ignore one batch.
			desiredPatchID = 0
			w.logger.Info("🔽 WORKER: EMPTY BATCH AT MIN RUNNERS - ALLOWING SCALE DOWN",
				"targetRunnerCount", targetRunnerCount,
				"minRunners", w.config.MinRunners,
				"desiredPatchID", desiredPatchID,
				"reason", "Reset patchID to allow controller scale down cleanup")
		}
	}

	previousLastPatch := w.lastPatch
	w.lastPatch = targetRunnerCount

	w.logger.Info("✅ WORKER: SCALING DECISION FINAL",
		"assignedJobs", count,
		"jobsCompleted", jobsCompleted,
		"finalTargetRunners", targetRunnerCount,
		"previousLastPatch", previousLastPatch,
		"newLastPatch", w.lastPatch,
		"desiredPatchID", desiredPatchID,
		"patchSeq", w.patchSeq,
		"minRunners", w.config.MinRunners,
		"maxRunners", w.config.MaxRunners,
		"wasEmptyBatch", count == 0 && jobsCompleted == 0)

	return desiredPatchID
}
