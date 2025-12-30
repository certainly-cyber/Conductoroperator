package controller

/*
#cgo LDFLAGS: -lglpk
#include <glpk.h>
#include <stdlib.h>
*/
import "C"
import (
	"context"
	"fmt"
	"math"
	"time"
	"unsafe"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	corev1 "k8s.io/api/core/v1"

	// [保留] 你的 Unstructured 引用
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/certainly-cyber/Conductoroperator/api/external"
	appsv1 "github.com/certainly-cyber/Conductoroperator/api/v1"
)

// ---------------------------------------------------------
// 全局类型定义
// ---------------------------------------------------------
type CandidatePG struct {
	Name        string
	Priority    float64
	MemberNum   int     // 组内成员数量
	PodWeight   float64 // 单个成员的重量 (GPU Request)
	OriginalObj *external.PodGroup
}

// fd 计算约束函数
// 带有精度修正，解决浮点数精度导致的装箱误判
func fd(x float64, d float64) float64 {
	val := x / d
	nearest := math.Round(val)
	// [精度修正] 1e-9 容忍度，处理浮点数除法的微小误差
	if math.Abs(val-nearest) < 1e-9 {
		return nearest - 1
	}
	return math.Floor(val)
}

// ConductorReconciler reconciles a Conductor object
type ConductorReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// ---------------------------------------------------------
// RBAC 权限配置 (必须包含 kubeflow.org 用于 PyTorchJob)
// ---------------------------------------------------------
//+kubebuilder:rbac:groups=apps.haojiang.cn,resources=conductors,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=apps.haojiang.cn,resources=conductors/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=apps.haojiang.cn,resources=conductors/finalizers,verbs=update
//+kubebuilder:rbac:groups=scheduling.x-k8s.io,resources=podgroups,verbs=get;list;watch;delete
//+kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch
//+kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
//+kubebuilder:rbac:groups=kubeflow.org,resources=pytorchjobs,verbs=get;list;watch;create;update;patch;delete

func (r *ConductorReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// 1. 获取 Conductor 实例
	var conductor appsv1.Conductor
	if err := r.Get(ctx, req.NamespacedName, &conductor); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	logger.Info("========== 开始选择逻辑 (1分钟/次) ==========")

	// 2. 准备数据: PodGroups
	var pgList external.PodGroupList
	if err := r.List(ctx, &pgList); err != nil {
		logger.Error(err, "无法获取 PodGroup 列表")
		return ctrl.Result{RequeueAfter: 1 * time.Minute}, nil
	}

	var candidates []CandidatePG
	// 使用索引遍历以获取指针，这部分逻辑保留你的写法
	for i := range pgList.Items {
		pg := &pgList.Items[i] // 使用指针引用

		// 获取 GPU 需求
		gpuReq := float64(0)
		if val, ok := pg.Spec.MinResources["nvidia.com/gpu"]; ok {
			gpuReq = float64(val.Value())
		}

		if gpuReq >= 0 {
			totalReq := gpuReq * float64(pg.Spec.MinMember)

			prioFactor := float64(pg.Spec.Priority)
			if prioFactor <= 0 {
				prioFactor = 1.0
			}

			finalReward := totalReq * prioFactor

			candidates = append(candidates, CandidatePG{
				Name:        pg.Name,
				MemberNum:   int(pg.Spec.MinMember),
				PodWeight:   gpuReq,
				Priority:    finalReward,
				OriginalObj: pg,
			})
		}
	}

	if len(candidates) == 0 {
		logger.Info("无待选 PodGroup")
		return ctrl.Result{RequeueAfter: 1 * time.Minute}, nil
	}

	// 3. 准备数据: Nodes
	// [已修复] 这里的 getClusterCapacities 已经更新为过滤掉 0 资源节点的版本
	c, err := r.getClusterCapacities(ctx)
	if err != nil {
		logger.Error(err, "无法获取集群容量")
		return ctrl.Result{RequeueAfter: 1 * time.Minute}, nil
	}

	if len(c) == 0 {
		logger.Info("集群无可用 GPU 资源 (Valid Nodes = 0)")
		return ctrl.Result{RequeueAfter: 1 * time.Minute}, nil
	}

	// 4. 定义 D 集合
	c_max := 0.0
	for _, val := range c {
		if val > c_max {
			c_max = val
		}
	}
	var D []float64
	if c_max > 0 {
		for i := 2; i <= 5; i++ {
			D = append(D, c_max/float64(i))
		}
	} else {
		D = []float64{1.0}
	}

	// 5. 执行 GLPK 选择
	// [已修复] 调用的是带有 1e-6 容忍度和 Go Slice 优化的版本
	logger.Info(">>> 开始执行 GLPK 选择算法", "CandidatesNum", len(candidates), "NodesNum", len(c))
	selectedIndices, totalVal, _ := solveGroupSelectionGLPK(c, candidates, D)

	// --- 以下为你的业务逻辑 (日志统计 & 任务提交) ---

	// 1. 计算集群当前总剩余 GPU
	totalClusterFreeGPU := 0.0
	for _, val := range c {
		totalClusterFreeGPU += val
	}

	// 2. 统计所有未被选中的 PodGroup 及其需求
	selectedMap := make(map[int]bool)
	for _, idx := range selectedIndices {
		selectedMap[idx] = true
	}

	var unselectedNames []string
	var unselectedDetails []string // 详细信息：Name(Req)

	for i, cand := range candidates {
		if !selectedMap[i] {
			// 该组未被选中
			totalReq := cand.PodWeight * float64(cand.MemberNum)
			unselectedNames = append(unselectedNames, cand.Name)
			unselectedDetails = append(unselectedDetails, fmt.Sprintf("%s(Need:%.0f)", cand.Name, totalReq))
		}
	}

	// 3. 统一输出日志
	logger.Info("资源概览",
		"ClusterTotalFreeGPU", totalClusterFreeGPU,
		"UnselectedPodGroupsCount", len(unselectedNames),
		"UnselectedDetails", unselectedDetails,
	)

	if len(selectedIndices) > 0 {
		logger.Info("GLPK 求解成功", "TotalReward", totalVal, "SelectedCount", len(selectedIndices))

		// -------------------------------------------------------------
		// 提交 PyTorchJob 并删除 PodGroup
		// -------------------------------------------------------------
		for _, idx := range selectedIndices {
			targetPG := candidates[idx]
			logger.Info("正在提交任务...", "PodGroupName", targetPG.Name)

			// 1. 构建 PyTorchJob 对象 (Unstructured)
			job := constructPyTorchJob(targetPG.OriginalObj)

			// 2. 提交到 K8s
			if err := r.Create(ctx, job); err != nil {
				logger.Error(err, "创建 PyTorchJob 失败", "JobName", job.GetName())
				continue
			}
			logger.Info("PyTorchJob 创建成功", "JobName", job.GetName())

			// 3. 删除旧的 PodGroup
			if err := r.Delete(ctx, targetPG.OriginalObj); err != nil {
				logger.Error(err, "删除 PodGroup 失败 (任务已提交)", "PodGroup", targetPG.Name)
			} else {
				logger.Info("PodGroup 资源已清除", "PodGroup", targetPG.Name)
			}
		}

	} else {
		logger.Info("未能选中任何 PodGroup")
	}

	logger.Info("========== 本次选择结束 ==========")
	return ctrl.Result{RequeueAfter: 1 * time.Minute}, nil
}

// ---------------------------------------------------------
// 辅助函数: 使用 Unstructured 构建 PyTorchJob
// [完全保留你的逻辑]
// ---------------------------------------------------------
func constructPyTorchJob(pg *external.PodGroup) *unstructured.Unstructured {
	// 1. 计算副本数
	// Master 永远是 1
	masterReplicas := int64(1)
	// Worker = MinMember - 1 (Master 占了一个)
	workerReplicas := int64(pg.Spec.MinMember) - 1
	if workerReplicas < 0 {
		workerReplicas = 0
	}

	// 2. 获取资源需求
	gpuReq := "0"
	if val, ok := pg.Spec.MinResources["nvidia.com/gpu"]; ok {
		gpuReq = val.String()
	}

	// 3. 获取镜像
	image := pg.Spec.Image
	if image == "" {
		image = "docker.io/library/opendilococode:latest"
	}

	// 4. 构造 Unstructured 对象 (严格匹配你的 YAML 模板)
	job := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "kubeflow.org/v1",
			"kind":       "PyTorchJob",
			"metadata": map[string]interface{}{
				"name":      pg.Name,
				"namespace": pg.Namespace,
			},
			"spec": map[string]interface{}{
				"pytorchReplicaSpecs": map[string]interface{}{
					"Master": map[string]interface{}{
						"replicas":      masterReplicas,
						"restartPolicy": "OnFailure",
						"template": map[string]interface{}{
							"metadata": map[string]interface{}{
								"annotations": map[string]interface{}{
									"sidecar.istio.io/inject": "false",
								},
								"labels": map[string]interface{}{
									"app":                      "diloco",
									"Podbandwitdh":             "100gbit",
									"appgroup.diktyo.x-k8s.io": "ml",
									"Priority":                 "high",
								},
							},
							"spec": map[string]interface{}{
								"hostNetwork": true,
								"tolerations": []interface{}{
									map[string]interface{}{
										"effect":   "NoSchedule",
										"operator": "Exists",
									},
								},
								"containers": []interface{}{
									map[string]interface{}{
										"name":            "pytorch",
										"image":           image, // 动态镜像
										"imagePullPolicy": "Never",
										"env":             getTemplateEnvVars(),
										"command": []string{
											"torchrun",
											"--nproc_per_node=1",
											"--nnodes=$(WORLD_SIZE)",
											"--node_rank=$(RANK)",
											"--master_addr=$(MASTER_ADDR)",
											"--master_port=$(MASTER_PORT)",
											"train_diloco_torch.py",
										},
										"resources": map[string]interface{}{
											"requests": map[string]interface{}{
												"cpu":            "10",
												"memory":         "10G",
												"nvidia.com/gpu": gpuReq, // 动态 GPU
											},
											"limits": map[string]interface{}{
												"cpu":            "10",
												"memory":         "10G",
												"nvidia.com/gpu": gpuReq, // 动态 GPU
											},
										},
									},
								},
							},
						},
					},
					// 只有当副本数 > 0 时才添加 Worker
				},
			},
		},
	}

	// 动态添加 Worker Spec
	if workerReplicas > 0 {
		// 通过断言获取 spec 引用
		specs := job.Object["spec"].(map[string]interface{})["pytorchReplicaSpecs"].(map[string]interface{})
		specs["Worker"] = map[string]interface{}{
			"replicas":      workerReplicas,
			"restartPolicy": "OnFailure",
			"template": map[string]interface{}{
				"metadata": map[string]interface{}{
					"annotations": map[string]interface{}{
						"sidecar.istio.io/inject": "false",
					},
					"labels": map[string]interface{}{
						"app":                      "diloco",
						"Podbandwitdh":             "100gbit",
						"appgroup.diktyo.x-k8s.io": "ml",
						"Priority":                 "high",
					},
				},
				"spec": map[string]interface{}{
					"hostNetwork": true,
					"tolerations": []interface{}{
						map[string]interface{}{
							"effect":   "NoSchedule",
							"operator": "Exists",
						},
					},
					"containers": []interface{}{
						map[string]interface{}{
							"name":            "pytorch",
							"image":           image, // 动态镜像
							"imagePullPolicy": "Never",
							"env":             getTemplateEnvVars(),
							"command": []string{
								"torchrun",
								"--nproc_per_node=1",
								"--nnodes=$(WORLD_SIZE)",
								"--node_rank=$(RANK)",
								"--master_addr=$(MASTER_ADDR)",
								"--master_port=$(MASTER_PORT)",
								"train_diloco_torch.py",
							},
							"resources": map[string]interface{}{
								"requests": map[string]interface{}{
									"cpu":            "10",
									"memory":         "10G",
									"nvidia.com/gpu": gpuReq, // 动态 GPU
								},
								"limits": map[string]interface{}{
									"cpu":            "10",
									"memory":         "10G",
									"nvidia.com/gpu": gpuReq, // 动态 GPU
								},
							},
						},
					},
				},
			},
		}
	}

	return job
}

// 辅助函数：返回环境变量模板
func getTemplateEnvVars() []interface{} {
	return []interface{}{
		map[string]interface{}{"name": "KUBERNETES_SERVICE_PORT_HTTPS", "value": "6443"},
		map[string]interface{}{"name": "KUBERNETES_SERVICE_PORT", "value": "6443"},
		map[string]interface{}{"name": "KUBERNETES_PORT_6443_TCP", "value": "tcp://k8smaster01:6443"},
		map[string]interface{}{"name": "KUBERNETES_PORT_6443_TCP_ADDR", "value": "k8smaster01"},
		map[string]interface{}{"name": "KUBERNETES_SERVICE_HOST", "value": "k8smaster01"},
		map[string]interface{}{"name": "KUBERNETES_PORT", "value": "tcp://k8smaster01:6443"},
		map[string]interface{}{"name": "KUBERNETES_PORT_6443_TCP_PORT", "value": "6443"},
		map[string]interface{}{"name": "NCCL_SOCKET_IFNAME", "value": "ens2f0np0"},
	}
}

// -------------------------------------------------------------------------
// [核心修复] getClusterCapacities
// 修复点：增加 free > 1e-9 判断，剔除空闲资源为 0 的节点，避免 fd(0) 惩罚
// -------------------------------------------------------------------------
func (r *ConductorReconciler) getClusterCapacities(ctx context.Context) ([]float64, error) {
	logger := log.FromContext(ctx)
	gpuResourceName := corev1.ResourceName("nvidia.com/gpu")

	var nodeList corev1.NodeList
	if err := r.List(ctx, &nodeList); err != nil {
		return nil, err
	}

	var podList corev1.PodList
	if err := r.List(ctx, &podList); err != nil {
		return nil, err
	}

	nodeUsedMap := make(map[string]float64)
	for _, pod := range podList.Items {
		if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		if pod.Spec.NodeName != "" {
			for _, container := range pod.Spec.Containers {
				if val, ok := container.Resources.Limits[gpuResourceName]; ok {
					nodeUsedMap[pod.Spec.NodeName] += float64(val.Value())
				}
			}
		}
	}

	var c []float64
	for _, node := range nodeList.Items {
		allocatable := float64(0)
		if val, ok := node.Status.Allocatable[gpuResourceName]; ok {
			allocatable = float64(val.Value())
		}
		used := nodeUsedMap[node.Name]
		free := allocatable - used
		if free < 0 {
			free = 0
		}

		// [关键修复]：只添加真正有空闲资源的节点
		if free > 1e-9 {
			logger.Info("节点加入计算池", "Node", node.Name, "Free", free)
			c = append(c, free)
		}
	}
	return c, nil
}

// -------------------------------------------------------------------------
// [核心修复] solveGroupSelectionGLPK
// 修复点：
// 1. 使用 Go Slice 传递矩阵，移除手动 malloc/free
// 2. 增加约束容忍度 (1e-6)
// 3. 设置求解器整数公差 (tol_int)
// -------------------------------------------------------------------------
func solveGroupSelectionGLPK(c []float64, candidates []CandidatePG, D []float64) ([]int, float64, float64) {
	k := len(candidates)
	numConstraints := len(D)

	// --- 数据转换 ---
	var w []float64
	var p []float64
	var groups [][]int

	currentIndex := 0
	for _, cand := range candidates {
		p = append(p, cand.Priority)
		var groupIndices []int
		for i := 0; i < cand.MemberNum; i++ {
			w = append(w, cand.PodWeight)
			groupIndices = append(groupIndices, currentIndex)
			currentIndex++
		}
		groups = append(groups, groupIndices)
	}

	// 1. 初始化
	lp := C.glp_create_prob()
	defer C.glp_delete_prob(lp)
	C.glp_set_prob_name(lp, C.CString("mKP_Fixed_Solver"))
	C.glp_set_obj_dir(lp, C.GLP_MAX)

	// 2. 定义行 (约束)
	numRows := 1 + numConstraints
	C.glp_add_rows(lp, C.int(numRows))

	// [约束1] 总容量 (加 1e-6 容忍度)
	totalCapacity := 0.0
	for _, val := range c {
		totalCapacity += val
	}
	constraintVal1 := totalCapacity + 1e-6
	C.glp_set_row_name(lp, 1, C.CString("Weight_Limit"))
	C.glp_set_row_bnds(lp, 1, C.GLP_UP, 0.0, C.double(constraintVal1))

	// [约束2...N] D集合约束 (加 1e-6 容忍度)
	for i, d := range D {
		rowIdx := C.int(i + 2)
		fTotal := 0.0
		for _, capVal := range c {
			fTotal += fd(capVal, d)
		}
		constraintValD := fTotal + 1e-6

		name := C.CString(fmt.Sprintf("D_Constraint_%d", i))
		C.glp_set_row_name(lp, rowIdx, name)
		C.glp_set_row_bnds(lp, rowIdx, C.GLP_UP, 0.0, C.double(constraintValD))
		C.free(unsafe.Pointer(name))
	}

	// 3. 定义列 (决策变量)
	C.glp_add_cols(lp, C.int(k))
	for i := 0; i < k; i++ {
		colIdx := C.int(i + 1)
		name := C.CString(fmt.Sprintf("z_%d", i))
		C.glp_set_col_name(lp, colIdx, name)
		C.glp_set_col_kind(lp, colIdx, C.GLP_BV) // Binary Variable
		C.glp_set_obj_coef(lp, colIdx, C.double(p[i]))
		C.free(unsafe.Pointer(name))
	}

	// 4. 构建稀疏矩阵 (使用 Go Slice)
	ia := []int32{0}
	ja := []int32{0}
	ar := []float64{0.0}

	// Row 1: 重量
	for i := 0; i < k; i++ {
		gw := 0.0
		for _, itemIdx := range groups[i] {
			gw += w[itemIdx]
		}
		if gw != 0 {
			ia = append(ia, 1)
			ja = append(ja, int32(i+1))
			ar = append(ar, gw)
		}
	}

	// Row 2...N: D约束
	for dIdx, d := range D {
		rowIdx := int32(dIdx + 2)
		for i := 0; i < k; i++ {
			val := 0.0
			for _, itemIdx := range groups[i] {
				val += fd(w[itemIdx], d)
			}
			// 优化：仅非零值入矩阵
			if math.Abs(val) > 1e-9 {
				ia = append(ia, rowIdx)
				ja = append(ja, int32(i+1))
				ar = append(ar, val)
			}
		}
	}

	// 加载矩阵
	ne := C.int(len(ia) - 1)
	C.glp_load_matrix(lp, ne,
		(*C.int)(unsafe.Pointer(&ia[0])),
		(*C.int)(unsafe.Pointer(&ja[0])),
		(*C.double)(unsafe.Pointer(&ar[0])))

	// 5. 求解参数设置 (关键修正)
	var iocp C.glp_iocp
	C.glp_init_iocp(&iocp)
	iocp.presolve = C.GLP_ON
	iocp.msg_lev = C.GLP_MSG_OFF
	iocp.tol_int = 1e-6 // 整数公差
	iocp.tol_obj = 1e-6 // 目标函数公差

	err := C.glp_intopt(lp, &iocp)
	if err != 0 {
		return nil, 0, 0
	}

	// 6. 结果提取
	if C.glp_mip_status(lp) == C.GLP_OPT {
		totalVal := float64(C.glp_mip_obj_val(lp))
		totalWeight := 0.0
		var selectedIndices []int

		for i := 0; i < k; i++ {
			if C.glp_mip_col_val(lp, C.int(i+1)) > 0.5 {
				selectedIndices = append(selectedIndices, i)
				for _, itemIdx := range groups[i] {
					totalWeight += w[itemIdx]
				}
			}
		}
		return selectedIndices, totalVal, totalWeight
	}
	return nil, 0, 0
}

func (r *ConductorReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&appsv1.Conductor{}).
		Complete(r)
}
