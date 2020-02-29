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
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	kubebatchclient "github.com/kubernetes-sigs/kube-batch/pkg/client/clientset/versioned"
	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	kubeinformers "k8s.io/client-go/informers"
	kubeclientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/cache"

	"github.com/NTHU-LSALAB/DRAGON/cmd/DRAGON/app/options"
	common "github.com/NTHU-LSALAB/DRAGON/pkg/apis/common/v1"
	tfv1 "github.com/NTHU-LSALAB/DRAGON/pkg/apis/tensorflow/v1"
	tfjobclientset "github.com/NTHU-LSALAB/DRAGON/pkg/client/clientset/versioned"
	tfjobscheme "github.com/NTHU-LSALAB/DRAGON/pkg/client/clientset/versioned/scheme"
	tfjobinformers "github.com/NTHU-LSALAB/DRAGON/pkg/client/informers/externalversions"
	tfjobinformersv1 "github.com/NTHU-LSALAB/DRAGON/pkg/client/informers/externalversions/tensorflow/v1"
	tfjoblisters "github.com/NTHU-LSALAB/DRAGON/pkg/client/listers/tensorflow/v1"
	"github.com/NTHU-LSALAB/DRAGON/pkg/common/jobcontroller"
	"github.com/NTHU-LSALAB/DRAGON/pkg/controller.v1/DRAGON/cluster"
	"github.com/NTHU-LSALAB/DRAGON/pkg/controller.v1/DRAGON/scheduling"
	tflogger "github.com/NTHU-LSALAB/DRAGON/pkg/logger"
	"github.com/NTHU-LSALAB/DRAGON/pkg/util/k8sutil"
	kubesharev1 "github.com/NTHU-LSALAB/KubeShare/pkg/apis/kubeshare/v1"
	kubeshareclientset "github.com/NTHU-LSALAB/KubeShare/pkg/client/clientset/versioned"
	kubeshareinformers "github.com/NTHU-LSALAB/KubeShare/pkg/client/informers/externalversions"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	controllerName = "tf-operator"

	// labels for pods and servers.
	tfReplicaTypeLabel  = "tf-replica-type"
	tfReplicaIndexLabel = "tf-replica-index"
	labelGroupName      = "group-name"
	labelTFJobName      = "tf-job-name"
	labelTFJobRole      = "tf-job-role"
)

var (
	// KeyFunc is the short name to DeletionHandlingMetaNamespaceKeyFunc.
	// IndexerInformer uses a delta queue, therefore for deletes we have to use this
	// key function but it should be just fine for non delete events.
	KeyFunc = cache.DeletionHandlingMetaNamespaceKeyFunc

	// DefaultTFControllerConfiguration is the suggested tf-operator configuration for production.
	DefaultTFControllerConfiguration = jobcontroller.JobControllerConfiguration{
		ReconcilerSyncLoopPeriod: metav1.Duration{Duration: 15 * time.Second},
		EnableGangScheduling:     false,
	}

	tfJobsDeletedCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: "tf_operator_jobs_deleted",
		Help: "Counts number of TF jobs deleted",
	})
)

// TFController is the type for TFJob Controller, which manages
// the lifecycle of TFJobs.
type TFController struct {
	jobcontroller.JobController

	// tfJobClientSet is a clientset for CRD TFJob.
	tfJobClientSet tfjobclientset.Interface

	// To allow injection of sync functions for testing.
	syncHandler func(string) (bool, error)

	// To allow injection of updateStatus for testing.
	updateStatusHandler func(tfjob *tfv1.TFJob) (*tfv1.TFJob, error)

	// To allow injection of deleteTFJob for testing.
	deleteTFJobHandler func(tfjob *tfv1.TFJob) error

	// tfJobInformer is a temporary field for unstructured informer support.
	tfJobInformer cache.SharedIndexInformer

	// Listers for TFJob, Pod and Service
	// tfJobLister can list/get tfjobs from the shared informer's store.
	tfJobLister tfjoblisters.TFJobLister

	// tfJobInformerSynced returns true if the tfjob store has been synced at least once.
	tfJobInformerSynced cache.InformerSynced

	/*************** ERICYEH ***************/

	kubeshareClientSet kubeshareclientset.Interface

	WaitingQueue  scheduling.JobQueue
	RunningQueue  scheduling.JobQueue
	FinishedQueue scheduling.JobQueue

	/*************** ERICYEH ***************/
}

// NewTFController returns a new TFJob controller.
func NewTFController(
	// This variable is for unstructured informer.
	tfJobInformer tfjobinformersv1.TFJobInformer,
	kubeClientSet kubeclientset.Interface,
	kubeBatchClientSet kubebatchclient.Interface,
	tfJobClientSet tfjobclientset.Interface,
	kubeInformerFactory kubeinformers.SharedInformerFactory,
	// This field is not used now but we keep it since it will be used
	// after we support CRD validation.
	tfJobInformerFactory tfjobinformers.SharedInformerFactory,
	kubeshareClientSet kubeshareclientset.Interface,
	option *options.ServerOption,
	kubeshareInformerFactory kubeshareinformers.SharedInformerFactory,
) *TFController {

	tfjobscheme.AddToScheme(scheme.Scheme)

	log.Info("Creating TFJob controller")
	// Create new TFController.
	tc := &TFController{
		tfJobClientSet:     tfJobClientSet,
		kubeshareClientSet: kubeshareClientSet,
	}

	// Create base controller
	log.Info("Creating Job controller")
	jc := jobcontroller.NewJobController(tc, metav1.Duration{Duration: 15 * time.Second},
		option.EnableGangScheduling, kubeClientSet, kubeshareClientSet, kubeBatchClientSet, kubeInformerFactory, option, tfv1.Plural)
	tc.JobController = jc
	// Set sync handler.
	tc.syncHandler = tc.syncTFJob
	tc.updateStatusHandler = tc.updateTFJobStatus
	// set delete handler.
	tc.deleteTFJobHandler = tc.deleteTFJob
	// Set up an event handler for when tfjob resources change.
	tfJobInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    tc.addTFJob,
		UpdateFunc: tc.updateTFJob,
		// This will enter the sync loop and no-op,
		// because the tfjob has been deleted from the store.
		DeleteFunc: tc.enqueueTFJob,
	})

	tc.tfJobInformer = tfJobInformer.Informer()
	tc.tfJobLister = tfJobInformer.Lister()
	tc.tfJobInformerSynced = tfJobInformer.Informer().HasSynced

	// Create pod informer.
	podInformer := kubeInformerFactory.Core().V1().Pods()

	// Set up an event handler for when pod resources change
	podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    jc.AddPod,
		UpdateFunc: jc.UpdatePod,
		DeleteFunc: jc.DeletePod,
	})

	tc.PodLister = podInformer.Lister()
	tc.PodInformerSynced = podInformer.Informer().HasSynced

	// Create service informer.
	serviceInformer := kubeInformerFactory.Core().V1().Services()

	// Set up an event handler for when service resources change.
	serviceInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    jc.AddService,
		UpdateFunc: jc.UpdateService,
		DeleteFunc: jc.DeleteService,
	})

	tc.ServiceLister = serviceInformer.Lister()
	tc.ServiceInformerSynced = serviceInformer.Informer().HasSynced

	/*************** ERICYEH ***************/

	scheduling.InitClientSets(kubeClientSet, tfJobClientSet, kubeshareClientSet, jc.Option)
	cluster.InitClientSets(kubeClientSet, kubeshareClientSet, tc.Option)

	if jc.Option.KubeShareSupport {

		kubeshareInformer := kubeshareInformerFactory.Kubeshare().V1().SharePods()

		kubeshareInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc:    jc.AddSharePod,
			UpdateFunc: jc.UpdateSharePod,
			DeleteFunc: jc.DeleteSharePod,
		})

		tc.SharePodLister = kubeshareInformer.Lister()
		tc.SharePodInformerSynced = kubeshareInformer.Informer().HasSynced
	}

	/*************** ERICYEH ***************/

	return tc
}

// Run will set up the event handlers for types we are interested in, as well
// as syncing informer caches and starting workers. It will block until stopCh
// is closed, at which point it will shutdown the workqueue and wait for
// workers to finish processing their current work items.
func (tc *TFController) Run(threadiness int, stopCh <-chan struct{}) error {
	defer utilruntime.HandleCrash()
	defer tc.WorkQueue.ShutDown()

	// Start the informer factories to begin populating the informer caches.
	log.Info("Starting TFJob controller")

	// Wait for the caches to be synced before starting workers.
	log.Info("Waiting for informer caches to sync")

	if tc.Option.KubeShareSupport {
		if ok := cache.WaitForCacheSync(stopCh, tc.tfJobInformerSynced, tc.PodInformerSynced, tc.ServiceInformerSynced, tc.SharePodInformerSynced); !ok {
			return fmt.Errorf("failed to wait for caches to sync")
		}
	} else {
		if ok := cache.WaitForCacheSync(stopCh, tc.tfJobInformerSynced, tc.PodInformerSynced, tc.ServiceInformerSynced); !ok {
			return fmt.Errorf("failed to wait for caches to sync")
		}
	}
	log.Infof("Starting %v workers", threadiness)
	// Launch workers to process TFJob resources.
	for i := 0; i < threadiness; i++ {
		go wait.Until(tc.runWorker, time.Second, stopCh)
	}

	log.Info("Started workers")
	<-stopCh
	log.Info("Shutting down workers")

	return nil
}

// runWorker is a long-running function that will continually call the
// processNextWorkItem function in order to read and process a message on the
// workqueue.
func (tc *TFController) runWorker() {
	for tc.processNextWorkItem() {
	}
}

// processNextWorkItem will read a single work item off the workqueue and
// attempt to process it, by calling the syncHandler.
func (tc *TFController) processNextWorkItem() bool {

	obj, quit := tc.WorkQueue.Get()
	if quit {
		return false
	}
	defer tc.WorkQueue.Done(obj)

	var key string
	var ok bool
	if key, ok = obj.(string); !ok {
		// As the item in the workqueue is actually invalid, we call
		// Forget here else we'd go into a loop of attempting to
		// process a work item that is invalid.
		tc.WorkQueue.Forget(obj)
		utilruntime.HandleError(fmt.Errorf("expected string in workqueue but got %#v", obj))
		return true
	}
	logger := tflogger.LoggerForKey(key)

	if !strings.Contains(key, jobcontroller.SHAREPODNEEDSYNC) {
		tfJob, err := tc.getTFJobFromKey(key)
		if err != nil {
			if err == errNotExists {
				logger.Infof("TFJob has been deleted: %v", key)
				tfJobsDeletedCount.Inc()

				/*************** ERICYEH ***************/

				// tc.RunningQueue.Remove(tfJob)
				namespace, name, err := cache.SplitMetaNamespaceKey(key)
				if err != nil {
					return true
				}
				tc.RunningQueue.Remove(&scheduling.TrainingJob{TFJob: &tfv1.TFJob{ObjectMeta: metav1.ObjectMeta{
					Namespace: namespace,
					Name:      name,
				}}})

				/*************** ERICYEH ***************/

				return true
			}

			// Log the failure to conditions.
			logger.Errorf("Failed to get TFJob from key %s: %v", key, err)
			if err == errFailedMarshal {
				errMsg := fmt.Sprintf("Failed to unmarshal the object to TFJob object: %v", err)
				tflogger.LoggerForJob(tfJob).Warn(errMsg)
				tc.Recorder.Event(tfJob, v1.EventTypeWarning, failedMarshalTFJobReason, errMsg)
			}

			return true
		}

		// Sync TFJob to match the actual state to this desired state.
		forget, err := tc.syncHandler(key)
		if err == nil {
			if forget {
				tc.WorkQueue.Forget(key)
			}
			return true
		}

		utilruntime.HandleError(fmt.Errorf("error syncing tfjob: %v", err))
		tc.WorkQueue.AddRateLimited(key)
	} else {
		err := tc.reconcileTFJobs(nil)
		if err != nil {
			tc.WorkQueue.AddRateLimited(key)
		}
		tc.WorkQueue.Forget(key)
	}

	return true
}

func (tc *TFController) enqueueTFJob(tfjob interface{}) {
	key, err := KeyFunc(tfjob)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for tfjob object %#v: %v", tfjob, err))
		return
	}

	enqueue := false
	if t, ok := tfjob.(metav1.Object); ok {
		if tc.Option.KubeShareSupport {
			enqueue = false
			if val, okk := t.GetAnnotations()["DRAGON_KUBESHARE"]; okk && val == "true" {
				enqueue = true
			}
		} else {
			enqueue = true
			if val, okk := t.GetAnnotations()["DRAGON_KUBESHARE"]; okk && val == "true" {
				enqueue = false
			}
		}
	} else {
		log.Errorf("enqueueTFJob: Cannot interpret argument tfjob as *tfv1.TFJob? Am I wrong? tfjob: %#v", tfjob)
		return
	}

	if enqueue {
		// TODO: we may need add backoff here
		tc.WorkQueue.Add(key)
	}
}

// syncTFJob syncs the tfjob with the given key if it has had its expectations fulfilled, meaning
// it did not expect to see any more of its pods/services created or deleted.
// This function is not meant to be invoked concurrently with the same key.
func (tc *TFController) syncTFJob(key string) (bool, error) {
	startTime := time.Now()
	logger := tflogger.LoggerForKey(key)
	defer func() {
		logger.Infof("Finished syncing tfjob %s (%v)", key, time.Since(startTime))
	}()

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return false, err
	}
	if len(namespace) == 0 || len(name) == 0 {
		return false, fmt.Errorf("invalid tfjob key %q: either namespace or name is missing", key)
	}

	sharedTFJob, err := tc.getTFJobFromName(namespace, name)
	if err != nil {
		if err == errNotExists {
			logger.Infof("TFJob has been deleted: %v", key)
			tfJobsDeletedCount.Inc()
			// jm.expectations.DeleteExpectations(key)
			return true, nil
		}
		return false, err
	}

	tfjob := sharedTFJob.DeepCopy()
	tfjobNeedsSync := tc.satisfiedExpectations(tfjob)

	// Set default for the new tfjob.
	scheme.Scheme.Default(tfjob)

	var reconcileTFJobsErr error
	if tfjobNeedsSync && tfjob.DeletionTimestamp == nil {
		reconcileTFJobsErr = tc.reconcileTFJobs(tfjob)
	}

	if reconcileTFJobsErr != nil {
		return false, reconcileTFJobsErr
	}

	return true, err
}

// reconcileTFJobs checks and updates replicas for each given TFReplicaSpec.
// It will requeue the tfjob in case of an error while creating/deleting pods/services.
func (tc *TFController) reconcileTFJobs(tfjob *tfv1.TFJob) error {

	/*************** Enqueue ***************/

	if tfjob != nil {

		isNewJob := true

		// if we determine tfjob is a new job, it could not be a running job.
		// if tfjob is in running queue, 100% is not a new job.
		// but if tfjob is not in running queue, it is "possible" a new job.
		for _, job := range tc.RunningQueue {
			if tfjob.Namespace == job.Namespace && tfjob.Name == job.Name {
				if tfjob.ObjectMeta.UID == job.ObjectMeta.UID {
					isNewJob = false
					job.TFJob = tfjob
				} else {
					tc.RunningQueue.Remove(job)
				}
				break
			}
		}

		for _, job := range tc.WaitingQueue {
			if tfjob.Namespace == job.Namespace && tfjob.Name == job.Name {
				if tfjob.ObjectMeta.UID == job.ObjectMeta.UID {
					isNewJob = false
					job.TFJob = tfjob
				} else {
					tc.WaitingQueue.Remove(job)
				}
				break
			}
		}

		// if a job's name and namespace equal to the job in finished queue,
		// but the UID is not equal, this represents it is a new job.
		for _, job := range tc.FinishedQueue {
			if tfjob.Namespace == job.Namespace && tfjob.Name == job.Name {
				if tfjob.ObjectMeta.UID == job.ObjectMeta.UID {
					isNewJob = false
				} else {
					tc.FinishedQueue.Remove(job)
				}
				break
			}
		}

		if isNewJob {
			newJob := scheduling.NewTrainingJob(tfjob)
			newJob.Status.EnqueueTime = func(t metav1.Time) *metav1.Time { return &t }(metav1.Now())
			err := newJob.UpdateTFJobTime()
			if err != nil {
				log.Errorf("Error when update tfjob %s/%s time: %s", tfjob.Namespace, tfjob.Name, err)
				return err
			}
			tc.WaitingQueue.Add(newJob)
			log.Infof("New job! %s/%s", newJob.Namespace, newJob.Name)
		}
	}

	/*************** Ensure every jobs in running queue are still exist ***************/
	var disappearJobsKillLater scheduling.JobQueue
	for _, job := range tc.RunningQueue {
		_, err := tc.getTFJobFromName(job.Namespace, job.Name)
		if err != nil && err == errNotExists {
			disappearJobsKillLater.Add(job) // tc.RunningQueue.Remove(job)
		}
	}
	for _, job := range disappearJobsKillLater {
		tc.RunningQueue.Remove(job)
	}
	disappearJobsKillLater = nil

	tc.WaitingQueue.PrintMe("WaitingQueue")
	tc.RunningQueue.PrintMe("RunningQueue")
	tc.FinishedQueue.PrintMe("FinishedQueue")

	/*************** Get cluster resource info ***************/

	// Sync cluster resource
	nodeRes, err := cluster.SyncClusterResource()
	if err != nil {
		log.Errorf("Error when syncing cluster resource: %s", err)
	}
	nodeRes.PrintMe()

	/*************** Apply scheduling algorithm ***************/

	// DRAGON scheduling algorithm
	scheduling.SchedulingAlgorithm(
		&tc.WaitingQueue,
		&tc.RunningQueue,
		tc.HighPrioritySharePodsQueue,
		tc.HighPrioritySharePodsQueueMutex,
		nodeRes,
	)

	log.Infof("Running jobs placement plan after scheduling")
	for _, job := range tc.RunningQueue {
		log.Infof("================== %s/%s ==================", job.Namespace, job.Name)
		log.Infof("PS:")
		job.ReplicasPlacementPlan[tfv1.TFReplicaTypePS].PrintMe()
		log.Infof("Worker:")
		job.ReplicasPlacementPlan[tfv1.TFReplicaTypeWorker].PrintMe()
		log.Infof("==============================================")
	}

	/*************** Sync jobs with its placement plan ***************/

	var finishedJobsKillLater scheduling.JobQueue
	var buf strings.Builder

	for _, runningjob := range tc.RunningQueue {

		tfjob = runningjob.TFJob

		tfjobKey, err := KeyFunc(tfjob)
		if err != nil {
			utilruntime.HandleError(fmt.Errorf("couldn't get key for tfjob object %#v: %v", tfjob, err))
			buf.WriteString(err.Error())
			buf.WriteString("\n")
			continue // return err
		}
		logger := tflogger.LoggerForJob(tfjob)
		logger.Infof("Reconcile TFJobs %s", tfjob.Name)

		oldStatus := tfjob.Status.DeepCopy()

		var pods []*v1.Pod = nil
		var sharepods []*kubesharev1.SharePod = nil

		if tc.Option.KubeShareSupport {
			sps, err := tc.GetSharePodsForJob(tfjob)
			if err != nil {
				logger.Warnf("getPodsForTFJob error %v", err)
				buf.WriteString(err.Error())
				buf.WriteString("\n")
				continue // return err
			}
			sharepods = sps
		} else {
			ps, err := tc.GetPodsForJob(tfjob)
			if err != nil {
				logger.Warnf("getPodsForTFJob error %v", err)
				buf.WriteString(err.Error())
				buf.WriteString("\n")
				continue // return err
			}
			pods = ps
		}

		services, err := tc.GetServicesForJob(tfjob)

		if err != nil {
			logger.Warnf("getServicesForTFJob error %v", err)
			buf.WriteString(err.Error())
			buf.WriteString("\n")
			continue // return err
		}

		// retrieve the previous number of retry
		previousRetry := tc.WorkQueue.NumRequeues(tfjobKey)
		active := int32(-1)
		if tc.Option.KubeShareSupport {
			active = int32(len(k8sutil.FilterActiveSharePods(sharepods)))
		} else {
			active = int32(len(k8sutil.FilterActivePods(pods)))
		}
		failed := int32(-1)
		if tc.Option.KubeShareSupport {
			failed = k8sutil.FilterSharePodCount(sharepods, v1.PodFailed)
		} else {
			failed = k8sutil.FilterPodCount(pods, v1.PodFailed)
		}
		totalReplicas := getTotalReplicas(tfjob)
		prevReplicasFailedNum := getTotalFailedReplicas(tfjob)

		var failureMessage string
		tfJobExceedsLimit := false
		exceedsBackoffLimit := false
		pastBackoffLimit := false

		if tfjob.Spec.BackoffLimit != nil {
			jobHasNewFailure := failed > prevReplicasFailedNum
			// new failures happen when status does not reflect the failures and active
			// is different than parallelism, otherwise the previous controller loop
			// failed updating status so even if we pick up failure it is not a new one
			exceedsBackoffLimit = jobHasNewFailure && (active != totalReplicas) &&
				(int32(previousRetry)+1 > *tfjob.Spec.BackoffLimit)

			if tc.Option.KubeShareSupport {
				pastBackoffLimit, err = tc.pastBackoffLimitSharePods(tfjob, sharepods)
			} else {
				pastBackoffLimit, err = tc.pastBackoffLimitPods(tfjob, pods)
			}
			if err != nil {
				logger.Warnf("pastBackoffLimit error %v", err)
				buf.WriteString(err.Error())
				buf.WriteString("\n")
				continue // return err
			}
		}

		if exceedsBackoffLimit || pastBackoffLimit {
			// check if the number of pod restart exceeds backoff (for restart OnFailure only)
			// OR if the number of failed jobs increased since the last syncJob
			tfJobExceedsLimit = true
			failureMessage = fmt.Sprintf("TFJob %s has failed because it has reached the specified backoff limit", tfjob.Name)
		} else if tc.pastActiveDeadline(tfjob) {
			failureMessage = fmt.Sprintf("TFJob %s has failed because it was active longer than specified deadline", tfjob.Name)
			tfJobExceedsLimit = true
		}

		// If the TFJob is terminated, delete all pods and services.
		if isSucceeded(tfjob.Status) || isFailed(tfjob.Status) || tfJobExceedsLimit {
			if tc.Option.KubeShareSupport {
				if err := tc.deleteSharePodsAndServices(tfjob, sharepods); err != nil {
					logger.Warnf("deleteSharePodsAndServices error %v", err)
					buf.WriteString(err.Error())
					buf.WriteString("\n")
					continue // return err
				}
			} else {
				if err := tc.deletePodsAndServices(tfjob, pods); err != nil {
					logger.Warnf("deletePodsAndServices error %v", err)
					buf.WriteString(err.Error())
					buf.WriteString("\n")
					continue // return err
				}
			}
			finishedJobsKillLater.Add(runningjob) // tc.RunningQueue.Remove(runningjob)
			_now := metav1.Now()
			runningjob.Status.FinishedTime = &_now
			tc.FinishedQueue.Add(runningjob)

			if tfJobExceedsLimit {
				tc.Recorder.Event(tfjob, v1.EventTypeNormal, tfJobFailedReason, failureMessage)
				if tfjob.Status.CompletionTime == nil {
					now := metav1.Now()
					tfjob.Status.CompletionTime = &now
				}
				err := updateTFJobConditions(tfjob, common.JobFailed, tfJobFailedReason, failureMessage)
				if err != nil {
					tflogger.LoggerForJob(tfjob).Infof("Append tfjob condition error: %v", err)
					buf.WriteString(err.Error())
					buf.WriteString("\n")
					continue // return err
				}
			}

			if err := tc.cleanupTFJob(tfjob); err != nil {
				logger.Warnf("cleanupTFJob error %v", err)
				buf.WriteString(err.Error())
				buf.WriteString("\n")
				continue // return err
			}

			if tc.Config.EnableGangScheduling {
				if err := tc.DeletePodGroup(tfjob); err != nil {
					logger.Warnf("DeletePodGroup error %v", err)
					buf.WriteString(err.Error())
					buf.WriteString("\n")
					continue // return err
				}
			}

			// At this point the pods may have been deleted, so if the job succeeded, we need to manually set the replica status.
			// If any replicas are still Active, set their status to succeeded.
			if isSucceeded(tfjob.Status) {
				for rtype := range tfjob.Status.ReplicaStatuses {
					tfjob.Status.ReplicaStatuses[rtype].Succeeded += tfjob.Status.ReplicaStatuses[rtype].Active
					tfjob.Status.ReplicaStatuses[rtype].Active = 0
				}
			}
			// return tc.updateStatusHandler(tfjob)
			if updated, err := tc.updateStatusHandler(tfjob); err != nil {
				logger.Warnf("updateStatusHandler error %v", err)
				buf.WriteString(err.Error())
				buf.WriteString("\n")
				continue
			} else {
				runningjob.TFJob = updated
			}
		}

		if tc.Config.EnableGangScheduling {
			minAvailableReplicas := getTotalReplicas(tfjob)
			_, err := tc.SyncPodGroup(tfjob, minAvailableReplicas)
			if err != nil {
				logger.Warnf("Sync PodGroup %v: %v", tfjob.Name, err)
			}
		}

		// Save the current state of the replicas
		replicasStatus := make(map[string]v1.PodPhase)

		// Diff current active pods/services with replicas.
		for rtype, spec := range tfjob.Spec.TFReplicaSpecs {
			var currentIndex []int
			if tc.Option.KubeShareSupport {
				err, curidx := tc.reconcileSharePods(tfjob, sharepods, rtype, spec, replicasStatus, runningjob.ReplicasPlacementPlan[rtype])
				if err != nil {
					logger.Warnf("reconcilePods error %v", err)
					buf.WriteString(err.Error())
					buf.WriteString("\n")
					continue // return err
				}
				currentIndex = curidx
			} else {
				err, curidx := tc.reconcilePods(tfjob, pods, rtype, spec, replicasStatus, runningjob.ReplicasPlacementPlan[rtype])
				if err != nil {
					logger.Warnf("reconcilePods error %v", err)
					buf.WriteString(err.Error())
					buf.WriteString("\n")
					continue // return err
				}
				currentIndex = curidx
			}

			err = tc.reconcileServices(tfjob, services, rtype, spec, currentIndex, int(*tfjob.Spec.MaxInstances))
			if err != nil {
				logger.Warnf("reconcileServices error %v", err)
				buf.WriteString(err.Error())
				buf.WriteString("\n")
				continue // return err
			}
		}

		// no need to update the tfjob if the status hasn't changed since last time.
		if !reflect.DeepEqual(*oldStatus, tfjob.Status) {
			// return tc.updateStatusHandler(tfjob)
			if updated, err := tc.updateStatusHandler(tfjob); err != nil {
				logger.Warnf("updateStatusHandler error %v", err)
				buf.WriteString(err.Error())
				buf.WriteString("\n")
				continue
			} else {
				runningjob.TFJob = updated
			}
		}
	}
	for _, job := range finishedJobsKillLater {
		tc.RunningQueue.Remove(job)
	}

	if buf.Len() == 0 {
		return nil
	}
	return errors.New(buf.String())
}

// satisfiedExpectations returns true if the required adds/dels for the given tfjob have been observed.
// Add/del counts are established by the controller at sync time, and updated as controllees are observed by the controller
// manager.
func (tc *TFController) satisfiedExpectations(tfjob *tfv1.TFJob) bool {
	satisfied := false
	tfjobKey, err := KeyFunc(tfjob)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for tfjob object %#v: %v", tfjob, err))
		return false
	}

	for rtype := range tfjob.Spec.TFReplicaSpecs {
		// Check the expectations of the pods.
		expectationPodsKey := jobcontroller.GenExpectationPodsKey(tfjobKey, string(rtype))
		satisfied = satisfied || tc.Expectations.SatisfiedExpectations(expectationPodsKey)

		// Check the expectations of the services.
		expectationServicesKey := jobcontroller.GenExpectationServicesKey(tfjobKey, string(rtype))
		satisfied = satisfied || tc.Expectations.SatisfiedExpectations(expectationServicesKey)
	}

	return satisfied
}

// pastBackoffLimit checks if container restartCounts sum exceeds BackoffLimit
// this method applies only to pods with restartPolicy == OnFailure or Always
func (tc *TFController) pastBackoffLimitPods(tfjob *tfv1.TFJob, pods []*v1.Pod) (bool, error) {
	if tfjob.Spec.BackoffLimit == nil {
		return false, nil
	}
	logger := tflogger.LoggerForJob(tfjob)
	result := int32(0)
	for rtype, spec := range tfjob.Spec.TFReplicaSpecs {
		if spec.RestartPolicy != common.RestartPolicyOnFailure && spec.RestartPolicy != common.RestartPolicyAlways {
			logger.Warnf("The restart policy of replica %v of the job %v is not OnFailure or Always. Not counted in backoff limit.", rtype, tfjob.Name)
			continue
		}
		// Convert TFReplicaType to lower string.
		rt := strings.ToLower(string(rtype))
		pods, err := tc.FilterPodsForReplicaType(pods, rt)
		if err != nil {
			return false, err
		}
		for i := range pods {
			po := pods[i]
			if po.Status.Phase == v1.PodRunning || po.Status.Phase == v1.PodPending {
				for j := range po.Status.InitContainerStatuses {
					stat := po.Status.InitContainerStatuses[j]
					result += stat.RestartCount
				}
				for j := range po.Status.ContainerStatuses {
					stat := po.Status.ContainerStatuses[j]
					result += stat.RestartCount
				}
			}
		}
	}

	if *tfjob.Spec.BackoffLimit == 0 {
		return result > 0, nil
	}
	return result >= *tfjob.Spec.BackoffLimit, nil
}

// pastBackoffLimit checks if container restartCounts sum exceeds BackoffLimit
// this method applies only to pods with restartPolicy == OnFailure or Always
func (tc *TFController) pastBackoffLimitSharePods(tfjob *tfv1.TFJob, pods []*kubesharev1.SharePod) (bool, error) {
	if tfjob.Spec.BackoffLimit == nil {
		return false, nil
	}
	logger := tflogger.LoggerForJob(tfjob)
	result := int32(0)
	for rtype, spec := range tfjob.Spec.TFReplicaSpecs {
		if spec.RestartPolicy != common.RestartPolicyOnFailure && spec.RestartPolicy != common.RestartPolicyAlways {
			logger.Warnf("The restart policy of replica %v of the job %v is not OnFailure or Always. Not counted in backoff limit.", rtype, tfjob.Name)
			continue
		}
		// Convert TFReplicaType to lower string.
		rt := strings.ToLower(string(rtype))
		pods, err := tc.FilterSharePodsForReplicaType(pods, rt)
		if err != nil {
			return false, err
		}
		for i := range pods {
			po := pods[i]
			if po.Status.PodStatus != nil && (po.Status.PodStatus.Phase == v1.PodRunning || po.Status.PodStatus.Phase == v1.PodPending) {
				for j := range po.Status.PodStatus.InitContainerStatuses {
					stat := po.Status.PodStatus.InitContainerStatuses[j]
					result += stat.RestartCount
				}
				for j := range po.Status.PodStatus.ContainerStatuses {
					stat := po.Status.PodStatus.ContainerStatuses[j]
					result += stat.RestartCount
				}
			}
		}
	}

	if *tfjob.Spec.BackoffLimit == 0 {
		return result > 0, nil
	}
	return result >= *tfjob.Spec.BackoffLimit, nil
}

// pastActiveDeadline checks if job has ActiveDeadlineSeconds field set and if it is exceeded.
func (tc *TFController) pastActiveDeadline(tfjob *tfv1.TFJob) bool {
	if tfjob.Spec.ActiveDeadlineSeconds == nil || tfjob.Status.StartTime == nil {
		return false
	}
	now := metav1.Now()
	start := tfjob.Status.StartTime.Time
	duration := now.Time.Sub(start)
	allowedDuration := time.Duration(*tfjob.Spec.ActiveDeadlineSeconds) * time.Second
	return duration >= allowedDuration
}

func (tc *TFController) GetJobFromInformerCache(namespace, name string) (metav1.Object, error) {
	return tc.getTFJobFromName(namespace, name)
}

func (tc *TFController) GetJobFromAPIClient(namespace, name string) (metav1.Object, error) {
	return tc.tfJobClientSet.KubeflowV1().TFJobs(namespace).Get(name, metav1.GetOptions{})
}

func (tc *TFController) GetAPIGroupVersionKind() schema.GroupVersionKind {
	return tfv1.SchemeGroupVersionKind
}

func (tc *TFController) GetAPIGroupVersion() schema.GroupVersion {
	return tfv1.SchemeGroupVersion
}

func (tc *TFController) GetGroupNameLabelKey() string {
	return labelGroupName
}

func (tc *TFController) GetJobNameLabelKey() string {
	return labelTFJobName
}

func (tc *TFController) GetGroupNameLabelValue() string {
	return tfv1.GroupName
}

func (tc *TFController) GetReplicaTypeLabelKey() string {
	return tfReplicaTypeLabel
}

func (tc *TFController) GetReplicaIndexLabelKey() string {
	return tfReplicaIndexLabel
}

func (tc *TFController) GetJobRoleKey() string {
	return labelTFJobRole
}

func (tc *TFController) ControllerName() string {
	return controllerName
}
