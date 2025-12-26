package controller

/*
#cgo LDFLAGS: -lglpk
#include <glpk.h>
#include <stdlib.h>

// 辅助函数：封装 GLPK 矩阵加载
void set_matrix(glp_prob *lp, int size, int *ia, int *ja, double *ar) {
    glp_load_matrix(lp, size, ia, ja, ar);
}
*/
import "C"
import (
	"context"
	"fmt"
	"time"
	"unsafe"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	corev1 "k8s.io/api/core/v1"

	// 引入你定义的 API 包 (Conductor)
	appsv1 "github.com/certainly-cyber/Conductoroperator/api/v1"
	// 引入外部 API 包 (PodGroup)
	"github.com/certainly-cyber/Conductoroperator/api/external"
)

// ---------------------------------------------------------
// 1. 全局定义结构体 (解决类型不兼容报错的关键)
// ---------------------------------------------------------
type CandidatePG struct {
	Name      string
	GPUDemand int64
	Priority  float64
}

// ConductorReconciler reconciles a Conductor object
type ConductorReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// ---------------------------------------------------------
// RBAC 权限配置
// ---------------------------------------------------------
//+kubebuilder:rbac:groups=apps.haojiang.cn,resources=conductors,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=apps.haojiang.cn,resources=conductors/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=apps.haojiang.cn,resources=conductors/finalizers,verbs=update
//+kubebuilder:rbac:groups=scheduling.x-k8s.io,resources=podgroups,verbs=get;list;watch
//+kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch
//+kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch

// Reconcile 是核心循环逻辑
func (r *ConductorReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// 1. 获取 Conductor 实例
	var conductor appsv1.Conductor
	if err := r.Get(ctx, req.NamespacedName, &conductor); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	logger.Info("========== 开始优化调度周期 (1分钟/次) ==========")

	// 2. 获取所有 Pending 状态的 PodGroup
	var pgList external.PodGroupList
	if err := r.List(ctx, &pgList); err != nil {
		logger.Error(err, "无法获取 PodGroup 列表")
		return ctrl.Result{RequeueAfter: 1 * time.Minute}, nil
	}

	// 使用全局定义的 CandidatePG 类型
	var candidates []CandidatePG

	for _, pg := range pgList.Items {
		// 这里假设 minResources 定义的是 *单个 Pod* 的需求
		// 总需求 = 单个需求 * MinMember
		gpuReq := int64(0)
		if val, ok := pg.Spec.MinResources["nvidia.com/gpu"]; ok {
			gpuReq = val.Value()
		}

		totalGPU := gpuReq * int64(pg.Spec.MinMember)

		if totalGPU > 0 {
			candidates = append(candidates, CandidatePG{
				Name:      pg.Name,
				GPUDemand: totalGPU,
				Priority:  float64(totalGPU), // 贪心策略：价值 = GPU需求量
			})
		}
	}

	if len(candidates) == 0 {
		logger.Info("没有待调度的 GPU PodGroup")
		return ctrl.Result{RequeueAfter: 1 * time.Minute}, nil
	}

	// 3. 对节点进行调度优化
	// 此时传递 candidates 不会再报错，因为类型匹配
	if err := r.optimizeScheduling(ctx, candidates); err != nil {
		logger.Error(err, "调度优化失败")
	}

	logger.Info("========== 本次优化结束 ==========")
	// 保持每 1 分钟执行一次
	return ctrl.Result{RequeueAfter: 1 * time.Minute}, nil
}

// optimizeScheduling 对每个节点运行 GLPK 求解
// 参数 candidates 使用全局类型 []CandidatePG
func (r *ConductorReconciler) optimizeScheduling(ctx context.Context, candidates []CandidatePG) error {
	logger := log.FromContext(ctx)
	gpuResourceName := corev1.ResourceName("nvidia.com/gpu")

	// 1. 获取节点列表
	var nodeList corev1.NodeList
	if err := r.List(ctx, &nodeList); err != nil {
		return err
	}

	// 2. 获取 Pod 列表以计算已用资源
	var podList corev1.PodList
	if err := r.List(ctx, &podList); err != nil {
		return err
	}

	nodeUsedMap := make(map[string]int64)
	for _, pod := range podList.Items {
		if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		if pod.Spec.NodeName != "" {
			for _, container := range pod.Spec.Containers {
				// 优先取 Limits，如果没有取 Requests
				if val, ok := container.Resources.Limits[gpuResourceName]; ok {
					nodeUsedMap[pod.Spec.NodeName] += val.Value()
				} else if val, ok := container.Resources.Requests[gpuResourceName]; ok {
					nodeUsedMap[pod.Spec.NodeName] += val.Value()
				}
			}
		}
	}

	// 3. 针对每个节点运行 ILP 求解
	foundGPU := false
	for _, node := range nodeList.Items {
		allocatable := node.Status.Allocatable
		totalGPU := int64(0)
		if val, ok := allocatable[gpuResourceName]; ok {
			totalGPU = val.Value()
		}

		if totalGPU == 0 {
			continue
		}
		foundGPU = true

		freeGPU := totalGPU - nodeUsedMap[node.Name]
		if freeGPU <= 0 {
			logger.Info("节点无剩余 GPU", "Node", node.Name)
			continue
		}

		logger.Info(">>> 开始计算节点优化", "Node", node.Name, "FreeGPU", freeGPU)

		// 调用求解函数
		selectedIndices, totalVal := solveKnapsackGLPK(candidates, float64(freeGPU))

		if len(selectedIndices) > 0 {
			var selectedNames []string
			for _, idx := range selectedIndices {
				selectedNames = append(selectedNames, candidates[idx].Name)
			}
			logger.Info("GLPK 求解成功",
				"Node", node.Name,
				"SelectedPodGroups", selectedNames,
				"TotalReward", totalVal,
			)
			// TODO: 在这里添加实际的绑定逻辑 (例如 Patch PodGroup)
		} else {
			logger.Info("GLPK 计算完成，无合适组合", "Node", node.Name)
		}
	}

	if !foundGPU {
		logger.Info("集群中未检测到 GPU 节点")
	}

	return nil
}

// solveKnapsackGLPK 使用 CGO 调用 GLPK 解决 0/1 背包问题
// 参数 candidates 使用全局类型 []CandidatePG
func solveKnapsackGLPK(candidates []CandidatePG, capacity float64) ([]int, float64) {
	numItems := len(candidates)
	if numItems == 0 {
		return nil, 0
	}

	// GLPK 初始化
	lp := C.glp_create_prob()
	defer C.glp_delete_prob(lp)

	C.glp_set_prob_name(lp, C.CString("Knapsack_Solver"))
	C.glp_set_obj_dir(lp, C.GLP_MAX) // 最大化价值

	// 定义约束: 总重量 <= 容量
	C.glp_add_rows(lp, 1)
	C.glp_set_row_name(lp, 1, C.CString("Capacity_Constraint"))
	C.glp_set_row_bnds(lp, 1, C.GLP_UP, 0.0, C.double(capacity))

	// 定义变量 (0/1选择)
	C.glp_add_cols(lp, C.int(numItems))
	for i := 0; i < numItems; i++ {
		colIdx := C.int(i + 1)
		C.glp_set_col_name(lp, colIdx, C.CString(fmt.Sprintf("x_%d", i)))
		C.glp_set_col_kind(lp, colIdx, C.GLP_BV) // Binary Variable
		C.glp_set_obj_coef(lp, colIdx, C.double(candidates[i].Priority))
	}

	// 构建矩阵
	// 数组长度 = 元素个数 + 1 (因为 C 数组索引从 1 开始)
	// 这里最坏情况是所有物品都在约束中，所以分配 numItems + 1
	maxElements := numItems + 1

	c_ia := (*C.int)(C.malloc(C.size_t(maxElements * 4)))
	c_ja := (*C.int)(C.malloc(C.size_t(maxElements * 4)))
	c_ar := (*C.double)(C.malloc(C.size_t(maxElements * 8)))
	defer C.free(unsafe.Pointer(c_ia))
	defer C.free(unsafe.Pointer(c_ja))
	defer C.free(unsafe.Pointer(c_ar))

	ia := (*[1 << 30]C.int)(unsafe.Pointer(c_ia))[:maxElements:maxElements]
	ja := (*[1 << 30]C.int)(unsafe.Pointer(c_ja))[:maxElements:maxElements]
	ar := (*[1 << 30]C.double)(unsafe.Pointer(c_ar))[:maxElements:maxElements]

	idx := 1
	for i := 0; i < numItems; i++ {
		weight := float64(candidates[i].GPUDemand)
		if weight > 0 {
			ia[idx] = 1            // 第 1 行
			ja[idx] = C.int(i + 1) // 第 i+1 列
			ar[idx] = C.double(weight)
			idx++
		}
	}

	// 加载矩阵
	C.set_matrix(lp, C.int(idx-1), c_ia, c_ja, c_ar)

	// 求解
	var iocp C.glp_iocp
	C.glp_init_iocp(&iocp)
	iocp.presolve = C.GLP_ON
	iocp.msg_lev = C.GLP_MSG_OFF // 关闭 GLPK 控制台输出

	err := C.glp_intopt(lp, &iocp)
	if err != 0 {
		fmt.Printf("GLPK 求解出错: %d\n", err)
		return nil, 0
	}

	// 获取结果
	status := C.glp_mip_status(lp)
	if status == C.GLP_OPT {
		totalVal := float64(C.glp_mip_obj_val(lp))
		var selectedIndices []int

		for i := 0; i < numItems; i++ {
			val := C.glp_mip_col_val(lp, C.int(i+1))
			if val > 0.5 {
				selectedIndices = append(selectedIndices, i)
			}
		}
		return selectedIndices, totalVal
	}

	return nil, 0
}

// SetupWithManager sets up the controller with the Manager.
func (r *ConductorReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&appsv1.Conductor{}).
		Complete(r)
}
