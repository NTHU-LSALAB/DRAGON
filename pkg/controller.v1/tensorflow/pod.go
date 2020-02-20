// Copyright 2018 The Kubeflow Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package controller provides a Kubernetes controller for a TFJob resource.
package tensorflow

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"

	kubesharev1 "github.com/NTHU-LSALAB/KubeShare/pkg/apis/kubeshare/v1"
	common "github.com/NTHU-LSALAB/DRAGON/pkg/apis/common/v1"
	tfv1 "github.com/NTHU-LSALAB/DRAGON/pkg/apis/tensorflow/v1"
	"github.com/NTHU-LSALAB/DRAGON/pkg/common/jobcontroller"
	"github.com/NTHU-LSALAB/DRAGON/pkg/controller.v1/DRAGON/cluster"
	"github.com/NTHU-LSALAB/DRAGON/pkg/controller.v1/DRAGON/scheduling"
	tflogger "github.com/NTHU-LSALAB/DRAGON/pkg/logger"
	train_util "github.com/NTHU-LSALAB/DRAGON/pkg/util/train"
)

const (
	// tfConfig is the environment variable name of TensorFlow cluster spec.
	tfConfig = "TF_CONFIG"

	// gang scheduler name.
	gangSchedulerName = "kube-batch"

	// podTemplateRestartPolicyReason is the warning reason when the restart
	// policy is set in pod template.
	podTemplateRestartPolicyReason = "SettedPodTemplateRestartPolicy"
	// exitedWithCodeReason is the normal reason when the pod is exited because of the exit code.
	exitedWithCodeReason = "ExitedWithCode"
	// podTemplateSchedulerNameReason is the warning reason when other scheduler name is set
	// in pod templates with gang-scheduling enabled
	podTemplateSchedulerNameReason = "SettedPodTemplateSchedulerName"
)

// reconcilePods checks and updates pods for each given TFReplicaSpec.
// It will requeue the tfjob in case of an error while creating/deleting pods.
func (tc *TFController) reconcilePods(
	tfjob *tfv1.TFJob,
	pods []*kubesharev1.SharePod,
	rtype tfv1.TFReplicaType,
	spec *common.ReplicaSpec,
	rstatus map[string]v1.PodPhase,
	placementPlan *scheduling.JobPlacementPlan,
) (error, []int) {

	// Convert TFReplicaType to lower string.
	rt := strings.ToLower(string(rtype))
	logger := tflogger.LoggerForReplica(tfjob, rt)
	// Get all pods for the type rt.
	pods, err := tc.FilterSharePodsForReplicaType(pods, rt)
	if err != nil {
		return err, nil
	}

	replicas := placementPlan.Count()
	restart := false
	worker0Completed := false

	initializeTFReplicaStatuses(tfjob, rtype)

	podSlices := tc.GetSharePodSlices(pods, int(*tfjob.Spec.MaxInstances), logger)
	createLater := make(scheduling.JobPlacementPlan)
	usedIndex := make([]int, 0, replicas)

	for nodeName, workers := range *placementPlan {
		for workerID, worker := range *workers {
			if val, ok := podSlices[workerID]; !ok {
				if _, ok := createLater[nodeName]; !ok {
					createLater[nodeName] = &scheduling.NodeResPlacePlan{workerID: worker}
				} else {
					(*createLater[nodeName])[workerID] = worker
				}
			} else {
				if len(val) > 1 {
					logger.Warningf("We have too many pods for %s", rt)
					// TODO(gaocegege): Kill some pods.
				} else if len(val) == 1 {
					delete(podSlices, workerID)

					// Check the status of the current pod.
					pod := val[0]

					index, err := strconv.Atoi(pod.Labels[tfReplicaIndexLabel])
					if err != nil {
						logger.Errorf("Pod Label replica index is not a number! %s", pod.Labels[tfReplicaIndexLabel])
					}
					usedIndex = append(usedIndex, index)

					// Get the exit code of the tensorflow container.
					var exitCode int32 = 0xbeef // magic number
					if pod.Status.PodStatus != nil {
						for _, status := range pod.Status.PodStatus.ContainerStatuses {
							state := status.State
							if status.Name == tfv1.DefaultContainerName && state.Terminated != nil {
								exitCode = state.Terminated.ExitCode
								logger.Infof("Pod: %v.%v exited with code %v", pod.Namespace, pod.Name, exitCode)
								tc.Recorder.Eventf(tfjob, v1.EventTypeNormal, exitedWithCodeReason, "Pod: %v.%v exited with code %v", pod.Namespace, pod.Name, exitCode)
							}
						}
					}
					// Check if the pod is retryable.
					if spec.RestartPolicy == common.RestartPolicyExitCode {
						if (pod.Status.PodStatus != nil && pod.Status.PodStatus.Phase == v1.PodFailed) && train_util.IsRetryableExitCode(exitCode) {
							logger.Infof("Need to restart the pod: %v.%v", pod.Namespace, pod.Name)
							if err := tc.PodControl.DeletePod(pod.Namespace, pod.Name, tfjob); err != nil {
								// return err
								logger.Errorf("Error when deleting Pod: %s", err.Error())
							}
							restart = true
						}
					}

					// Check whether worker 0 is exited without error.
					if rtype == tfv1.TFReplicaTypeWorker && index == 0 && exitCode == 0 {
						worker0Completed = true
					}
					updateTFJobReplicaStatuses(tfjob, rtype, pod)
				}
			}
		}
	}

	sort.Ints(usedIndex)
	logger.Infof("usedIndex: %v", usedIndex)

	usedIndexCopy := make([]int, len(usedIndex))
	copy(usedIndexCopy, usedIndex)
	logger.Infof("ERICYEH: %v", usedIndexCopy)

	usedIndexLen := len(usedIndex)
	// usedIndex_i is Index of usedIndex
	usedIndex_i := 0
	// workerIndex is final index set to worker
	workerIndex := 0

	masterRole := false
	for nodeName, workers := range createLater {
		for workerid, worker := range *workers {
			// find next available index
			for ; usedIndex_i < usedIndexLen && workerIndex == usedIndex[usedIndex_i]; usedIndex_i, workerIndex = usedIndex_i+1, workerIndex+1 {
			}
			logger.Infof("Worker Index: %d\n", workerIndex)
			usedIndexCopy = append(usedIndexCopy, workerIndex)

			index := workerIndex
			masterRole = false
			logger.Infof("Need to create new pod: %s", rt)
			// if master pod is present, select the master pod
			// if master is not present, first worker pod is selected as the master.
			if ContainChieforMasterSpec(tfjob) {
				if tfv1.IsChieforMaster(rtype) {
					masterRole = true
				}
			} else {
				if tfv1.IsWorker(rtype) && (index == 0) {
					masterRole = true
				}
			}
			if masterRole {
				worker.Critical = true
			}
			err = tc.createNewPod(tfjob, rt, strconv.Itoa(index), spec, masterRole, nodeName, workerid, worker, rtype)
			if err != nil {
				// return err, nil
				logger.Errorf("Error when creating Pod: %s", err.Error())
			}

			workerIndex++
		}
	}

	for _, worker := range podSlices {
		tc.PodControl.DeletePod(worker[0].Namespace, worker[0].Name, tfjob)
	}

	return tc.updateStatusSingle(tfjob, rtype, replicas, restart, worker0Completed), usedIndexCopy
}

// createNewPod creates a new pod for the given index and type.
func (tc *TFController) createNewPod(tfjob *tfv1.TFJob, rt, index string, spec *common.ReplicaSpec, masterRole bool, nodeName string, workerid string, customDevicesID *scheduling.WorkerResources, rtype tfv1.TFReplicaType) error {
	tfjobKey, err := KeyFunc(tfjob)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for tfjob object %#v: %v", tfjob, err))
		return err
	}
	expectationPodsKey := jobcontroller.GenExpectationPodsKey(tfjobKey, rt)
	err = tc.Expectations.ExpectCreations(expectationPodsKey, 1)
	if err != nil {
		return err
	}
	logger := tflogger.LoggerForReplica(tfjob, rt)
	// Create OwnerReference.
	controllerRef := tc.GenOwnerReference(tfjob)

	// Set type and index for the worker.
	labels := tc.GenLabels(tfjob.Name)
	labels[tfReplicaTypeLabel] = rt
	labels[tfReplicaIndexLabel] = index

	// set unique worker id to label on this worker
	labels["tf-job-workerid"] = workerid

	if masterRole {
		labels[labelTFJobRole] = "master"
	}

	podTemplate := spec.Template.DeepCopy()

	// Set name for the template.
	podTemplate.Name = jobcontroller.GenGeneralName(tfjob.Name, rt, index)

	if podTemplate.Labels == nil {
		podTemplate.Labels = make(map[string]string)
	}

	for key, value := range labels {
		podTemplate.Labels[key] = value
	}

	/****************** Custom Devices Configuration ******************/

	if podTemplate.Annotations == nil {
		podTemplate.Annotations = make(map[string]string)
	}

	if _, ok := (*customDevicesID).Workers[cluster.ResourceLsalabGPU]; ok {
		podTemplate.Annotations[cluster.ResourceLsalabGPUID] = (*customDevicesID).Workers[cluster.ResourceLsalabGPU]
		podTemplate.Annotations[cluster.ResourceLsalabGPUReq] = tfjob.Spec.TFReplicaSpecs[rtype].Template.Annotations[cluster.ResourceLsalabGPUReq]
		podTemplate.Annotations[cluster.ResourceLsalabGPULimit] = tfjob.Spec.TFReplicaSpecs[rtype].Template.Annotations[cluster.ResourceLsalabGPULimit]
		podTemplate.Annotations[cluster.ResourceLsalabGPUMem] = tfjob.Spec.TFReplicaSpecs[rtype].Template.Annotations[cluster.ResourceLsalabGPUMem]
	}

	/****************** Custom Devices Configuration ******************/

	if err := setClusterSpec(podTemplate, tfjob, rt, index); err != nil {
		return err
	}

	// Submit a warning event if the user specifies restart policy for
	// the pod template. We recommend to set it from the replica level.
	if podTemplate.Spec.RestartPolicy != v1.RestartPolicy("") {
		errMsg := "Restart policy in pod template will be overwritten by restart policy in replica spec"
		logger.Warning(errMsg)
		tc.Recorder.Event(tfjob, v1.EventTypeWarning, podTemplateRestartPolicyReason, errMsg)
	}
	setRestartPolicy(podTemplate, spec)

	// if gang-scheduling is enabled:
	// 1. if user has specified other scheduler, we report a warning without overriding any fields.
	// 2. if no SchedulerName is set for pods, then we set the SchedulerName to "kube-batch".
	if tc.Config.EnableGangScheduling {
		if isNonGangSchedulerSet(tfjob) {
			errMsg := "Another scheduler is specified when gang-scheduling is enabled and it will not be overwritten"
			logger.Warning(errMsg)
			tc.Recorder.Event(tfjob, v1.EventTypeWarning, podTemplateSchedulerNameReason, errMsg)
		} else {
			podTemplate.Spec.SchedulerName = gangSchedulerName
		}

		if podTemplate.Annotations == nil {
			podTemplate.Annotations = map[string]string{}
		}
		// we create the podGroup with the same name as the tfjob
		podTemplate.Annotations["scheduling.k8s.io/group-name"] = tfjob.Name
	}

	// err = tc.PodControl.CreatePodsWithControllerRef(tfjob.Namespace, podTemplate, tfjob, controllerRef)
	err = tc.PodControl.CreatePodsOnNode(nodeName, tfjob.Namespace, podTemplate, tfjob, controllerRef)
	if err != nil && errors.IsTimeout(err) {
		// Pod is created but its initialization has timed out.
		// If the initialization is successful eventually, the
		// controller will observe the creation via the informer.
		// If the initialization fails, or if the pod keeps
		// uninitialized for a long time, the informer will not
		// receive any update, and the controller will create a new
		// pod when the expectation expires.
		return nil
	} else if err != nil {
		return err
	}
	return nil
}

func setClusterSpec(podTemplateSpec *v1.PodTemplateSpec, tfjob *tfv1.TFJob, rt, index string) error {
	// Generate TF_CONFIG JSON string.
	tfConfigStr, err := genTFConfigJSONStr(tfjob, rt, index)
	if err != nil {
		return err
	}

	if tfConfigStr == "" {
		return nil
	}
	// Add TF_CONFIG environment variable.
	for i := range podTemplateSpec.Spec.Containers {
		if len(podTemplateSpec.Spec.Containers[i].Env) == 0 {
			podTemplateSpec.Spec.Containers[i].Env = make([]v1.EnvVar, 0)
		}
		podTemplateSpec.Spec.Containers[i].Env = append(podTemplateSpec.Spec.Containers[i].Env, v1.EnvVar{
			Name:  tfConfig,
			Value: tfConfigStr,
		})
	}
	return nil
}

func setRestartPolicy(podTemplateSpec *v1.PodTemplateSpec, spec *common.ReplicaSpec) {
	if spec.RestartPolicy == common.RestartPolicyExitCode {
		podTemplateSpec.Spec.RestartPolicy = v1.RestartPolicyNever
	} else {
		podTemplateSpec.Spec.RestartPolicy = v1.RestartPolicy(spec.RestartPolicy)
	}
}

func isNonGangSchedulerSet(tfjob *tfv1.TFJob) bool {
	for _, spec := range tfjob.Spec.TFReplicaSpecs {
		if spec.Template.Spec.SchedulerName != "" && spec.Template.Spec.SchedulerName != gangSchedulerName {
			return true
		}
	}
	return false
}
