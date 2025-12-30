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

	"github.com/certainly-cyber/Conductoroperator/api/external"
	appsv1 "github.com/certainly-cyber/Conductoroperator/api/v1"
)

// ---------------------------------------------------------
// 全局类型定义
// ---------------------------------------------------------
type CandidatePG struct {
	Name      string
	Priority  float64
	MemberNum int     // 组内成员数量
	PodWeight float64 // 单个成员的重量 (GPU Request)
}

// fd 计算约束函数 (带有精度修正)
// 修正逻辑：当 x/d 极其接近整数时，视为整除，执行 (x/d) - 1
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

//+kubebuilder:rbac:groups=apps.haojiang.cn,resources=conductors,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=apps.haojiang.cn,resources=conductors/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=apps.haojiang.cn,resources=conductors/finalizers,verbs=update
//+kubebuilder:rbac:groups=scheduling.x-k8s.io,resources=podgroups,verbs=get;list;watch
//+kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch
//+kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch

func (r *ConductorReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// 1. 获取 Conductor 实例
	var conductor appsv1.Conductor
	if err := r.Get(ctx, req.NamespacedName, &conductor); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	logger.Info("========== 开始选择逻辑 (1分钟/次) ==========")

	// 2. 准备数据: PodGroups (作为 Groups)
	var pgList external.PodGroupList
	if err := r.List(ctx, &pgList); err != nil {
		logger.Error(err, "无法获取 PodGroup 列表")
		return ctrl.Result{RequeueAfter: 1 * time.Minute}, nil
	}

	var candidates []CandidatePG
	for _, pg := range pgList.Items {
		// 获取单个 Pod 的 GPU 需求
		gpuReq := float64(0)
		if val, ok := pg.Spec.MinResources["nvidia.com/gpu"]; ok {
			gpuReq = float64(val.Value())
		}

		// 只要有 GPU 需求就加入计算
		if gpuReq >= 0 {
			totalReq := gpuReq * float64(pg.Spec.MinMember)

			// 计算 Reward = 总需求 * 优先级
			prioFactor := float64(pg.Spec.Priority)
			if prioFactor <= 0 {
				prioFactor = 1.0
			}

			finalReward := totalReq * prioFactor

			logger.Info("检测到 PodGroup 需求",
				"Name", pg.Name,
				"SinglePodGPU", gpuReq,
				"MemberNum", pg.Spec.MinMember,
				"TotalGPU", totalReq,
				"UserPriority", prioFactor,
				"FinalCalculatedReward", finalReward,
			)

			candidates = append(candidates, CandidatePG{
				Name:      pg.Name,
				MemberNum: int(pg.Spec.MinMember),
				PodWeight: gpuReq,
				Priority:  finalReward,
			})
		}
	}

	if len(candidates) == 0 {
		logger.Info("无待选 PodGroup")
		return ctrl.Result{RequeueAfter: 1 * time.Minute}, nil
	}

	// 3. 准备数据: Nodes (作为 c 数组)
	// [重要] 这里现在会过滤掉 free=0 的节点，防止 fd(0) = -1 的惩罚
	c, err := r.getClusterCapacities(ctx)
	if err != nil {
		logger.Error(err, "无法获取集群容量")
		return ctrl.Result{RequeueAfter: 1 * time.Minute}, nil
	}

	if len(c) == 0 {
		logger.Info("集群无可用 GPU 资源")
		return ctrl.Result{RequeueAfter: 1 * time.Minute}, nil
	}

	// 4. 定义 D 集合
	// c_max: 集群中最大的单节点空闲 GPU 数量
	c_max := 0.0
	for _, val := range c {
		if val > c_max {
			c_max = val
		}
	}

	var D []float64
	if c_max > 0 {
		for i := 2; i <= 5; i++ {
			val := c_max / float64(i)
			D = append(D, val)
		}
	} else {
		D = []float64{1.0}
	}

	logger.Info("约束参数 D 计算完成", "c_max", c_max, "D", D)

	// 5. 执行核心选择算法
	logger.Info(">>> 开始执行 GLPK 选择算法", "CandidatesNum", len(candidates), "NodesNum", len(c))

	selectedIndices, totalVal, totalWeight := solveGroupSelectionGLPK(c, candidates, D)

	if len(selectedIndices) > 0 {
		var selectedNames []string
		for _, idx := range selectedIndices {
			selectedNames = append(selectedNames, candidates[idx].Name)
		}

		logger.Info("GLPK 选择结果",
			"TotalReward", totalVal,
			"TotalWeight", totalWeight,
			"SelectedGroups", selectedNames,
		)
		// TODO: Bind 操作
	} else {
		logger.Info("未能选中任何 PodGroup (可能资源不足或约束限制)")
	}

	logger.Info("========== 本次选择结束 ==========")
	return ctrl.Result{RequeueAfter: 1 * time.Minute}, nil
}

// getClusterCapacities 获取集群容量
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

		// [关键逻辑修正]
		// 必须剔除空闲资源为 0 的节点。
		// 因为 fd(0, d) = -1，会导致该节点在约束公式中产生 -1 的惩罚，
		// 从而减少总容量的计算值，导致本来能放下的 Group 被拒绝。
		// 使用 1e-9 过滤掉浮点数误差产生的 "假0"。
		if free > 1e-9 {
			logger.Info("节点加入计算池",
				"NodeName", node.Name,
				"Free", free,
			)
			c = append(c, free)
		}
	}
	return c, nil
}

// -------------------------------------------------------------------------
// 核心逻辑：solveGroupSelectionGLPK
// 1. 使用 Go Slice 传递矩阵，避免手动 malloc
// 2. 增加 1e-6 的约束容忍度 (Constraint Tolerance)
// 3. 设置 GLPK 整数求解参数
// -------------------------------------------------------------------------
func solveGroupSelectionGLPK(c []float64, candidates []CandidatePG, D []float64) ([]int, float64, float64) {

	k := len(candidates) // 组的数量
	numConstraints := len(D)

	// --- 数据转换 ---
	var w []float64    // items 重量
	var p []float64    // 组价值
	var groups [][]int // 组成员索引

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

	// [约束1] 总容量约束 + 1e-6 容忍度
	totalCapacity := 0.0
	for _, val := range c {
		totalCapacity += val
	}
	constraintVal1 := totalCapacity + 1e-6

	C.glp_set_row_name(lp, 1, C.CString("Weight_Limit"))
	C.glp_set_row_bnds(lp, 1, C.GLP_UP, 0.0, C.double(constraintVal1))

	// [约束2...N] D集合约束 + 1e-6 容忍度
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

	// 4. 构建稀疏矩阵
	// GLPK 使用 1-based 索引，idx 0 为占位
	ia := []int32{0}
	ja := []int32{0}
	ar := []float64{0.0}

	// Row 1: 重量约束系数
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

	// Row 2...N: D约束系数
	for dIdx, d := range D {
		rowIdx := int32(dIdx + 2)
		for i := 0; i < k; i++ {
			val := 0.0
			for _, itemIdx := range groups[i] {
				val += fd(w[itemIdx], d) // 使用修正后的 fd
			}
			// 优化：只有当 val 不为 0 时才加入矩阵
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

	// 5. 求解参数设置
	var iocp C.glp_iocp
	C.glp_init_iocp(&iocp)
	iocp.presolve = C.GLP_ON
	iocp.msg_lev = C.GLP_MSG_OFF
	// [关键修正] 设置整数求解器的公差
	iocp.tol_int = 1e-6
	iocp.tol_obj = 1e-6

	err := C.glp_intopt(lp, &iocp)
	if err != 0 {
		fmt.Printf("GLPK 求解错误代码: %d\n", err)
		return nil, 0, 0
	}

	// 6. 结果提取
	status := C.glp_mip_status(lp)
	if status == C.GLP_OPT {
		totalVal := float64(C.glp_mip_obj_val(lp))
		totalWeight := 0.0
		var selectedIndices []int

		for i := 0; i < k; i++ {
			val := C.glp_mip_col_val(lp, C.int(i+1))
			if val > 0.5 {
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

// SetupWithManager sets up the controller with the Manager.
func (r *ConductorReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&appsv1.Conductor{}).
		Complete(r)
}
