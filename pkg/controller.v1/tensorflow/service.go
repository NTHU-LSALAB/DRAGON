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
	"strconv"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"

	common "github.com/NTHU-LSALAB/DRAGON/pkg/apis/common/v1"
	tfv1 "github.com/NTHU-LSALAB/DRAGON/pkg/apis/tensorflow/v1"
	"github.com/NTHU-LSALAB/DRAGON/pkg/common/jobcontroller"
	tflogger "github.com/NTHU-LSALAB/DRAGON/pkg/logger"
)

// reconcileServices checks and updates services for each given TFReplicaSpec.
// It will requeue the tfjob in case of an error while creating/deleting services.
func (tc *TFController) reconcileServices(
	tfjob *tfv1.TFJob,
	services []*v1.Service,
	rtype tfv1.TFReplicaType,
	spec *common.ReplicaSpec,
	currentIndex []int,
	replicas int) error {

	// Convert TFReplicaType to lower string.
	rt := strings.ToLower(string(rtype))

	// replicas := int(*spec.Replicas)
	// Get all services for the type rt.
	services, err := tc.FilterServicesForReplicaType(services, rt)
	if err != nil {
		return err
	}

	tflogger.LoggerForReplica(tfjob, rt).Infof("currentIndex: %v", currentIndex)

	serviceSlices := tc.GetServiceSlices(services, replicas, tflogger.LoggerForReplica(tfjob, rt))
	deleteLater := make([]bool, replicas)
	for i := range deleteLater {
		if len(serviceSlices[i]) > 0 {
			deleteLater[i] = true
		}
	}

	for _, index := range currentIndex {
		serviceSlice := serviceSlices[index]
		deleteLater[index] = false
		if len(serviceSlice) > 1 {
			tflogger.LoggerForReplica(tfjob, rt).Warningf("We have too many services for %s %d", rt, index)
			// TODO(gaocegege): Kill some services.
		} else if len(serviceSlice) == 0 {
			tflogger.LoggerForReplica(tfjob, rt).Infof("need to create new service: %s-%d", rt, index)
			err = tc.createNewService(tfjob, rtype, strconv.Itoa(index), spec)
			if err != nil {
				// return err
				tflogger.LoggerForReplica(tfjob, rt).Errorf("Error when creating service: %s", err)
			}
		}
	}

	for index, val := range deleteLater {
		if val {
			n := jobcontroller.GenGeneralName(tfjob.Name, rt, strconv.Itoa(index))
			tflogger.LoggerForReplica(tfjob, rt).Infof("Delete service %s", n)
			tc.ServiceControl.DeleteService(tfjob.Namespace, n, tfjob)
		}
	}

	return nil
}

// createNewService creates a new service for the given index and type.
func (tc *TFController) createNewService(tfjob *tfv1.TFJob, rtype tfv1.TFReplicaType, index string, spec *common.ReplicaSpec) error {
	tfjobKey, err := KeyFunc(tfjob)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for tfjob object %#v: %v", tfjob, err))
		return err
	}

	// Convert TFReplicaType to lower string.
	rt := strings.ToLower(string(rtype))
	expectationServicesKey := jobcontroller.GenExpectationServicesKey(tfjobKey, rt)
	err = tc.Expectations.ExpectCreations(expectationServicesKey, 1)
	if err != nil {
		return err
	}

	// Create OwnerReference.
	controllerRef := tc.GenOwnerReference(tfjob)

	// Append tfReplicaTypeLabel and tfReplicaIndexLabel labels.
	labels := tc.GenLabels(tfjob.Name)
	labels[tfReplicaTypeLabel] = rt
	labels[tfReplicaIndexLabel] = index

	port, err := GetPortFromTFJob(tfjob, rtype)
	if err != nil {
		return err
	}

	service := &v1.Service{
		Spec: v1.ServiceSpec{
			ClusterIP: "None",
			Selector:  labels,
			Ports: []v1.ServicePort{
				{
					Name: tfv1.DefaultPortName,
					Port: port,
				},
			},
		},
	}

	service.Name = jobcontroller.GenGeneralName(tfjob.Name, rt, index)
	service.Labels = labels

	err = tc.ServiceControl.CreateServicesWithControllerRef(tfjob.Namespace, service, tfjob, controllerRef)
	if err != nil && errors.IsTimeout(err) {
		// Service is created but its initialization has timed out.
		// If the initialization is successful eventually, the
		// controller will observe the creation via the informer.
		// If the initialization fails, or if the service keeps
		// uninitialized for a long time, the informer will not
		// receive any update, and the controller will create a new
		// service when the expectation expires.
		return nil
	} else if err != nil {
		return err
	}
	return nil
}
