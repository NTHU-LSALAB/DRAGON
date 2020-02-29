package cluster

import (
	"math"
	"strconv"
	"strings"

	"github.com/NTHU-LSALAB/DRAGON/cmd/DRAGON/app/options"
	kubesharev1 "github.com/NTHU-LSALAB/KubeShare/pkg/apis/kubeshare/v1"
	kubeshareclientset "github.com/NTHU-LSALAB/KubeShare/pkg/client/clientset/versioned"

	// kubesharecontroller "github.com/NTHU-LSALAB/KubeShare/pkg/devicemanager"
	log "github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeclientset "k8s.io/client-go/kubernetes"
	// "k8s.io/apimachinery/pkg/api/resource"
)

var (
	kubeClientSet      kubeclientset.Interface
	kubeshareClientSet kubeshareclientset.Interface
	option             *options.ServerOption
)

func InitClientSets(it kubeclientset.Interface, mit kubeshareclientset.Interface, op *options.ServerOption) {
	kubeClientSet, kubeshareClientSet, option = it, mit, op
}

const (
	ResourceKubeShareGPU = "kubeshare/gpu"
	ResourceNvidiaGPU    = "nvidia.com/gpu"
)

/* ------------------- struct PodRequest start ------------------- */

type PodRequests []*PodRequest

type PodRequest struct {
	CpuReq    int64
	MemReq    int64
	GpuReq    int64
	GpuMemReq int64
}

/* ------------------- struct PodRequest end ------------------- */

/* ------------------- struct NodeResources start ------------------- */

// NodeResources: Available resources in cluster to schedule Training Jobs
type NodeResources map[string]*NodeResource

func (this *NodeResources) DeepCopy() *NodeResources {
	copy := make(NodeResources, len(*this))
	for k, v := range *this {
		copy[k] = v.DeepCopy()
	}
	return &copy
}

func (this *NodeResources) PrintMe() {
	for name, res := range *this {
		log.Infof("============ Node: %s ============", name)
		log.Infof("CpuTotal: %d", res.CpuTotal)
		log.Infof("MemTotal: %d", res.MemTotal)
		log.Infof("GpuTotal: %d", res.GpuTotal)
		log.Infof("GpuMemTotal: %d", res.GpuMemTotal)
		log.Infof("CpuFree: %d", res.CpuFree)
		log.Infof("MemFree: %d", res.MemFree)
		log.Infof("GpuFree: %d", res.GpuFreeCount)
		log.Infof("GpuId:")
		for id, gpu := range res.GpuFree {
			log.Infof("    %s: %d, %d", id, (*gpu).GPUFreeReq, (*gpu).GPUFreeMem)
		}
	}
	log.Infof("============ Node Info End ============")
}

/* ------------------- struct NodeResources end ------------------- */

/* ------------------- struct NodeResource start ------------------- */

type NodeResource struct {
	CpuTotal int64
	MemTotal int64
	GpuTotal int
	// GpuMemTotal in bytes
	GpuMemTotal int64
	CpuFree     int64
	MemFree     int64
	/* Available GPU calculate */
	// Total GPU count - Pods using nvidia.com/gpu
	GpuFreeCount int
	// GPUs available usage (1.0 - SharePod usage)
	// GPUID to integer index mapping
	GpuFree map[string]*GPUInfo
}

func (this *NodeResource) DeepCopy() *NodeResource {
	gpuFreeCopy := make(map[string]*GPUInfo, len(this.GpuFree))
	for k, v := range this.GpuFree {
		gpuFreeCopy[k] = v.DeepCopy()
	}
	return &NodeResource{
		CpuTotal:     this.CpuTotal,
		MemTotal:     this.MemTotal,
		GpuTotal:     this.GpuTotal,
		GpuMemTotal:  this.GpuMemTotal,
		CpuFree:      this.CpuFree,
		MemFree:      this.MemFree,
		GpuFreeCount: this.GpuFreeCount,
		GpuFree:      gpuFreeCopy,
	}
}

/* ------------------- struct NodeResource end ------------------- */

/* ------------------- struct GPUInfo start ------------------- */

type GPUInfo struct {
	GPUFreeReq int64
	// GPUFreeMem in bytes
	GPUFreeMem int64
}

func (this *GPUInfo) DeepCopy() *GPUInfo {
	return &GPUInfo{
		GPUFreeReq: this.GPUFreeReq,
		GPUFreeMem: this.GPUFreeMem,
	}
}

/* ------------------- struct GPUInfo end ------------------- */

func SyncClusterResource() (nodeResources NodeResources, err error) {

	log.Infof("Start syncing cluster resources...")

	nodeList, err := kubeClientSet.CoreV1().Nodes().List(metav1.ListOptions{})
	if err != nil {
		log.Errorf("Error when listing nodes: %s", err)
		return nil, err
	}

	nodeResources = syncNodeResources(nodeList)

	podList, err := kubeClientSet.CoreV1().Pods("").List(metav1.ListOptions{FieldSelector: "status.phase!=Succeeded,status.phase!=Failed"})
	if err != nil {
		log.Errorf("Error when listing Pod: %s", err)
		return nil, err
	}
	var sharePodList *kubesharev1.SharePodList = nil
	if option.KubeShareSupport {
		spl, err := kubeshareClientSet.KubeshareV1().SharePods("").List(metav1.ListOptions{})
		if err != nil {
			log.Errorf("Error when listing SharePod: %s", err)
			return nil, err
		}
		sharePodList = spl
	}

	syncPodResources(nodeResources, podList, sharePodList)

	return
}

func syncPodResources(nodeRes NodeResources, podList *corev1.PodList, sharePodList *kubesharev1.SharePodList) {
	for _, pod := range podList.Items {
		nodeName := pod.Spec.NodeName
		// 1. If Pod is not scheduled, it don't use resources.
		// 2. If Pod's name contains kubeshare-vgpu is managed by SharePod,
		//    resource usage will be calcuated later.
		if nodeName == "" || strings.Contains(pod.Name, kubesharev1.KubeShareDummyPodName) {
			continue
		}
		ownedBySharePod := false
		for _, owner := range pod.ObjectMeta.OwnerReferences {
			if owner.Kind == "SharePod" {
				ownedBySharePod = true
				break
			}
		}
		if ownedBySharePod {
			continue
		}
		// If a running Pod is on the node we don't want, don't calculate it.
		// ex. on master has NoSchedule Taint.
		if _, ok := nodeRes[nodeName]; !ok {
			continue
		}

		for _, container := range pod.Spec.Containers {
			nodeRes[nodeName].CpuFree -= container.Resources.Requests.Cpu().MilliValue()
			nodeRes[nodeName].MemFree -= container.Resources.Requests.Memory().MilliValue()
			gpu := container.Resources.Limits[kubesharev1.ResourceNVIDIAGPU]
			nodeRes[nodeName].GpuFreeCount -= int(gpu.Value())
		}
	}

	if option.KubeShareSupport {
		for _, SharePod := range sharePodList.Items {
			nodeName := SharePod.Spec.NodeName
			// 1. If Pod is not scheduled, it don't use resources.
			if nodeName == "" {
				continue
			}
			// If a running Pod is on the node we don't want, don't calculate it.
			// ex. on master has NoSchedule Taint.
			if _, ok := nodeRes[nodeName]; !ok {
				continue
			}
			if SharePod.Status.PodStatus != nil && (SharePod.Status.PodStatus.Phase == "Succeeded" || SharePod.Status.PodStatus.Phase == "Failed") {
				continue
			}

			for _, container := range SharePod.Spec.Containers {
				nodeRes[nodeName].CpuFree -= container.Resources.Requests.Cpu().MilliValue()
				nodeRes[nodeName].MemFree -= container.Resources.Requests.Memory().MilliValue()
			}

			if gpuid, gpuidok := SharePod.ObjectMeta.Annotations[kubesharev1.KubeShareResourceGPUID]; gpuidok && gpuid != "" {
				if gpureq, gpureqok := SharePod.ObjectMeta.Annotations[kubesharev1.KubeShareResourceGPURequest]; gpureqok && gpureq != "" {
					if gpumem, gpumemok := SharePod.ObjectMeta.Annotations[kubesharev1.KubeShareResourceGPUMemory]; gpumemok && gpumem != "" {
						gpureqf, err := strconv.ParseFloat(gpureq, 64)
						if err != nil {
							log.Errorf("Cannot parse nvidia gpu request, pod: %s/%s, gpu req: %s", SharePod.Namespace, SharePod.Name, SharePod.ObjectMeta.Annotations[kubesharev1.KubeShareResourceGPURequest])
							break
						}
						gpureqi := int64(math.Ceil(gpureqf * (float64)(1000.0)))

						gpumemi, err := strconv.ParseInt(gpumem, 10, 64)
						if err != nil {
							log.Errorf("Cannot parse nvidia gpu memory, pod: %s/%s, gpu req: %s", SharePod.Namespace, SharePod.Name, SharePod.ObjectMeta.Annotations[kubesharev1.KubeShareResourceGPUMemory])
							break
						}

						if gpuInfo, ok := nodeRes[nodeName].GpuFree[gpuid]; !ok {
							if nodeRes[nodeName].GpuFreeCount > 0 {
								nodeRes[nodeName].GpuFreeCount--
								nodeRes[nodeName].GpuFree[gpuid] = &GPUInfo{
									GPUFreeReq: 1000 - gpureqi,
									GPUFreeMem: nodeRes[nodeName].GpuMemTotal - gpumemi,
								}
							} else {
								log.Errorf("==================================")
								log.Errorf("Bug! The rest number of free GPU is not enough for SharePod! GPUID: %s", gpuid)
								for errID, errGPU := range nodeRes[nodeName].GpuFree {
									log.Errorf("GPUID: %s", errID)
									log.Errorf("    Req: %d", errGPU.GPUFreeReq)
									log.Errorf("    Mem: %d", errGPU.GPUFreeMem)
								}
								log.Errorf("==================================")
							}
						} else {
							gpuInfo.GPUFreeReq -= gpureqi
							gpuInfo.GPUFreeMem -= gpumemi
						}
					}
				}
			}
		}
	}
}

func syncNodeResources(nodeList *corev1.NodeList) (nodeResources NodeResources) {

	nodeResources = make(NodeResources, len(nodeList.Items))

	for _, node := range nodeList.Items {
		// If NoSchedule Taint on the node, don't add to NodeResources!
		cannotScheduled := false
		for _, taint := range node.Spec.Taints {
			if string(taint.Effect) == "NoSchedule" {
				cannotScheduled = true
				log.Info("Node have NoSchedule taint, node name: ", node.ObjectMeta.Name)
				break
			}
		}
		if cannotScheduled {
			continue
		}

		cpu := node.Status.Allocatable.Cpu().MilliValue()
		mem := node.Status.Allocatable.Memory().MilliValue()
		gpuNum := func() int {
			tmp := node.Status.Allocatable[kubesharev1.ResourceNVIDIAGPU]
			return int(tmp.Value())
		}()
		gpuMem := func() int64 {
			if gpuInfo, ok := node.ObjectMeta.Annotations[kubesharev1.KubeShareNodeGPUInfo]; ok {
				gpuInfoArr := strings.Split(gpuInfo, ",")
				if len(gpuInfoArr) >= 1 {
					gpuArr := strings.Split(gpuInfoArr[0], ":")
					if len(gpuArr) != 2 {
						log.Errorf("GPU Info format error: %s", gpuInfo)
						return 0
					}
					gpuMem, err := strconv.ParseInt(gpuArr[1], 10, 64)
					if err != nil {
						log.Errorf("GPU Info format error: %s", gpuInfo)
						return 0
					}
					return gpuMem
				} else {
					return 0
				}
			} else {
				return 0
			}
		}()
		nodeResources[node.ObjectMeta.Name] = &NodeResource{
			CpuTotal:     cpu,
			MemTotal:     mem,
			GpuTotal:     gpuNum,
			GpuMemTotal:  gpuMem * 1024 * 1024, // in bytes
			CpuFree:      cpu,
			MemFree:      mem,
			GpuFreeCount: gpuNum,
			GpuFree:      make(map[string]*GPUInfo, gpuNum),
		}
	}
	return
}
