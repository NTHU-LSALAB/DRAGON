# DRAGON

K8s Custom Resource and Operator for Distributed TensorFlow Training Jobs with Parameter Server and Worker Strategy

## Overview

Fork from [kubeflow/tf-operator](https://github.com/kubeflow/tf-operator).

DRAGON makes it easy to deploy distributed parameter server TensorFlow training jobs with automatic scheduling and scaling strategies.

DRAGON bypass the kube-scheduler, directly assigns the location to every distributed training components. DRAGON is a custom scheduler for distributed TensorFlow training jobs.

## Installation

Installation depends on various use cases:
1. Training with CPU
```
kubectl create -f https://lsalab.cs.nthu.edu.tw/~ericyeh/DRAGON/v0.9/crd.yaml
kubectl create -f https://lsalab.cs.nthu.edu.tw/~ericyeh/DRAGON/v0.9/dragon.yaml
```
2. Training with NVIDIA GPU ([NVIDIA GPU](https://kubernetes.io/docs/tasks/manage-gpus/scheduling-gpus/))
```
# K8s with NVIDIA GPU device plugin installed
kubectl create -f https://lsalab.cs.nthu.edu.tw/~ericyeh/DRAGON/v0.9/crd.yaml
kubectl create -f https://lsalab.cs.nthu.edu.tw/~ericyeh/DRAGON/v0.9/dragon.yaml
```
3. Training with portion NVIDIA GPU allocation ([KubeShare](https://github.com/NTHU-LSALAB/KubeShare))
```
# K8s with KubeShare installed
kubectl create -f https://lsalab.cs.nthu.edu.tw/~ericyeh/DRAGON/v0.9/crd.yaml
kubectl create -f https://lsalab.cs.nthu.edu.tw/~ericyeh/DRAGON/v0.9/dragon-kubeshare.yaml
```

## TFJobs

TFJob is a CRD, which describes the resource usage and how to run the job. DRAGON will deploy and schedule Pods according to the TFJob.

```
apiVersion: kubeflow.org/v1
kind: TFJob
metadata:
  name: job1
spec:
  max-instances: 4
  min-instances: 1
  cleanPodPolicy: "All"
  tfReplicaSpecs:
    PS:
      replicas: 1
      restartPolicy: OnFailure
      template:
        spec:
          terminationGracePeriodSeconds: 0
          containers:
          - name: tensorflow
            image: tensorflow/tensorflow:1.15.0-py3
            command: ["/bin/bash", "-c", "curl -s https://lsalab.cs.nthu.edu.tw/~ericyeh/DRAGON/mnist-df.py | python3 -"]
            ports:
            - containerPort: 2222
              name: tfjob-port
            resources:
              limits:
                cpu: "4"
                memory: "8Gi"
    Worker:
      replicas: 4
      restartPolicy: OnFailure
      template:
        spec:
          terminationGracePeriodSeconds: 0
          containers:
          - name: tensorflow
            image: tensorflow/tensorflow:1.15.0-py3
            command: ["/bin/bash", "-c", "curl -s https://lsalab.cs.nthu.edu.tw/~ericyeh/DRAGON/mnist-df.py | python3 -"]
            env:
            - name: "global_steps"
              value: "100000"
            ports:
            - containerPort: 2222
              name: tfjob-port
            resources:
              limits:
                cpu: "4"
                memory: "8Gi"
```

* spec.max-instances: maximum number of workers to run simultaneously.
* spec.min-instances: minimum number of workers to run simultaneously.
* spec.cleanPodPolicy: set to "All".
* spec.tfReplicaSpecs.PS.replicas: number of distributed tensorflow parameter server.
* spec.tfReplicaSpecs.PS.restartPolicy: restart policy for all replicas of parameter server.
* spec.tfReplicaSpecs.PS.template: parameter server PodSpec.
* spec.tfReplicaSpecs.Worker.replicas: the initial number of distributed tensorflow worker, could be scaled bewteen spec.min-instances and spec.max-instances.
* spec.tfReplicaSpecs.Worker.restartPolicy: restart policy for all replicas of worker.
* spec.tfReplicaSpecs.PSWorker.template: worker PodSpec.
* spec.tfReplicaSpecs.{PS,Worker}.template.spec.containers[N].name: set to "tensorflow", which means that containers[N] is used to determine the job status.

### Using native K8s Nvidia GPUs

```
apiVersion: kubeflow.org/v1
kind: TFJob
metadata:
  name: job1
spec:
  max-instances: 4
  min-instances: 1
  cleanPodPolicy: "All"
  tfReplicaSpecs:
    PS:
      replicas: 1
      restartPolicy: OnFailure
      template:
        spec:
          terminationGracePeriodSeconds: 0
          containers:
          - name: tensorflow
            image: tensorflow/tensorflow:1.15.0-py3
            command: ["/bin/bash", "-c", "curl -s https://lsalab.cs.nthu.edu.tw/~ericyeh/DRAGON/mnist-df.py | python3 -"]
            ports:
            - containerPort: 2222
              name: tfjob-port
            resources:
              limits:
                cpu: "4"
                memory: "8Gi"
    Worker:
      replicas: 4
      restartPolicy: OnFailure
      template:
        spec:
          terminationGracePeriodSeconds: 0
          containers:
          - name: tensorflow
            image: tensorflow/tensorflow:1.15.0-gpu-py3
            command: ["/bin/bash", "-c", "curl -s https://lsalab.cs.nthu.edu.tw/~ericyeh/DRAGON/mnist-df.py | python3 -"]
            env:
            - name: "global_steps"
              value: "100000"
            ports:
            - containerPort: 2222
              name: tfjob-port
            resources:
              limits:
                cpu: "4"
                memory: "8Gi"
                "nvidia.com/gpu": 1
```

### Using portion GPUs ([KubeShare](https://github.com/NTHU-LSALAB/KubeShare))

```
apiVersion: kubeflow.org/v1
kind: TFJob
metadata:
  name: job1
  annotations:
    # required if DRAGON support KubeShare. DO NOT set this flag in non-KubeShare DRAGON mode, otherwise will get the wrong scheduling result.
    "DRAGON_KUBESHARE": "true"
spec:
  max-instances: 4
  min-instances: 1
  cleanPodPolicy: "All"
  tfReplicaSpecs:
    PS:
      replicas: 1
      restartPolicy: OnFailure
      template:
        spec:
          terminationGracePeriodSeconds: 0
          containers:
          - name: tensorflow
            image: tensorflow/tensorflow:1.15.0-py3
            command: ["/bin/bash", "-c", "curl -s https://lsalab.cs.nthu.edu.tw/~ericyeh/DRAGON/mnist-df.py | python3 -"]
            ports:
            - containerPort: 2222
              name: tfjob-port
            resources:
              limits:
                cpu: "4"
                memory: "8Gi"
    Worker:
      replicas: 4
      restartPolicy: OnFailure
      template:
        metadata:
          annotations:
            "kubeshare/gpu_request": "0.5"
            "kubeshare/gpu_limit": "1.0"
            "kubeshare/gpu_mem": "3189243904" # 3G
        spec:
          terminationGracePeriodSeconds: 0
          containers:
          - name: tensorflow
            image: tensorflow/tensorflow:1.15.0-gpu-py3
            command: ["/bin/bash", "-c", "curl -s https://lsalab.cs.nthu.edu.tw/~ericyeh/DRAGON/mnist-df.py | python3 -"]
            env:
            - name: "global_steps"
              value: "100000"
            ports:
            - containerPort: 2222
              name: tfjob-port
            resources:
              limits:
                cpu: "4"
                memory: "8Gi"
```

### Deployment

DRAGON will create several Pods/SharePods and Services (for communication between replicas), naming by {JOB_NAME}-{REPLICA_TYPE}-{ID}. For example: job1-ps-0 and job1-worker-2.

### TF_CONFIG

An environment variable 'TF_CONFIG' in every replicas generated by DRAGON, is used to identify who it is. TF_CONFIG is in json format. For example, a job with one parameter server and four workers in default namespace, and its worker id 0 will be:
```
{"cluster":{"ps":["job1-ps-0.default.svc:2222"],"worker":["job1-worker-0.default.svc:2222","job1-worker-1.default.svc:2222","job1-worker-2.default.svc:2222","job1-worker-3.default.svc:2222"]},"task":{"type":"worker","index":0},"environment":"cloud"}
```

## Scheduling and Scaling Strategies

* DRAGON tries to schedule pending jobs in the FIFO order, and ignoring the job which cannot meet its resource requirements.
* If there exists some jobs which have been waiting for more than 30 seconds, pick the longest one. DRAGON will schedule the job if it can meet its resource requirements after scaling down running jobs. This is for higher system throughput.
* If there exists some idle resources and DRGAON didn't perform any scheuling and scaling actions for more than one minutes, DRAGON tries to scale up running jobs. This is for higher resource utilization.

* DRAGON prefers to schedule all replicas within one node due to communication overhead.
* When performing scaling down, DRAGON prefers to terminates the lonely replicas first. (lonely means that location of the replica is different from parameter server)
* When performing scaling up, DRAGON prefers to schedule the new replicas to where the parameter server located.

## A demo clip

[![asciicast](https://asciinema.org/a/303266.png)](https://asciinema.org/a/303266)

## Build

### Compiling
```
git clone https://github.com/NTHU-LSALAB/DRAGON.git
cd DRAGON
make
```
* bin/DRAGON: DRAGON controller.

### Directories & Files
* cmd/: where main function located of three binaries.
* crd/: CRD specification yaml file.
* docker/: materials of all docker images in yaml files
* pkg/: includes DRAGON core components, SharePod, and API server clientset produced by code-generater.
* code-gen.sh: code-generator script.
* go.mod: DRAGON dependencies.

## Issues

Any issues please open a GitHub issue, thanks.
