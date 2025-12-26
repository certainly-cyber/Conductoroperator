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

// fd 计算约束函数 (移植自你的代码)
func fd(x float64, d float64) float64 {
	mod := math.Mod(x, d)
	if mod == 0 {
		return (x / d) - 1
	}
	return math.Floor(x / d)
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

			// [新增日志] 输出每个 PodGroup 的需求详情
			logger.Info("检测到 PodGroup 需求",
				"Name", pg.Name,
				"SinglePodGPU", gpuReq,
				"MemberNum", pg.Spec.MinMember,
				"TotalGPURequired", totalReq,
			)

			candidates = append(candidates, CandidatePG{
				Name:      pg.Name,
				MemberNum: int(pg.Spec.MinMember),
				PodWeight: gpuReq,
				// 价值 P = 总 GPU 需求 (可根据需求修改)
				Priority: totalReq,
			})
		}
	}

	if len(candidates) == 0 {
		logger.Info("无待选 PodGroup")
		return ctrl.Result{RequeueAfter: 1 * time.Minute}, nil
	}

	// 3. 准备数据: Nodes (作为 c 数组)
	c, err := r.getClusterCapacities(ctx)
	if err != nil {
		logger.Error(err, "无法获取集群容量")
		return ctrl.Result{RequeueAfter: 1 * time.Minute}, nil
	}

	// 4. 定义 D 集合 (动态计算)
	// 对应 MATLAB: D = c_max ./ (2:5);
	// ---------------------------------------------------------

	// 第一步：找出 c_max (集群中最大的单节点空闲 GPU 数量)
	c_max := 0.0
	for _, val := range c {
		if val > c_max {
			c_max = val
		}
	}

	// 第二步：生成 D 数组 [c_max/2, c_max/3, c_max/4, c_max/5]
	var D []float64
	if c_max > 0 {
		// MATLAB 的 2:5 意味着整数序列 2, 3, 4, 5
		for i := 2; i <= 5; i++ {
			val := c_max / float64(i)
			D = append(D, val)
		}
	} else {
		// 如果集群完全没有空闲资源 (c_max=0)，为了防止算法出错，给一个默认值
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
		// TODO: 这里仅做选择，不进行 Bind 操作
	} else {
		logger.Info("未能选中任何 PodGroup (可能资源不足或约束限制)")
	}

	logger.Info("========== 本次选择结束 ==========")
	return ctrl.Result{RequeueAfter: 1 * time.Minute}, nil
}

// getClusterCapacities 获取集群中每个节点的空闲 GPU 数量，对应算法中的 c[]
func (r *ConductorReconciler) getClusterCapacities(ctx context.Context) ([]float64, error) {
	logger := log.FromContext(ctx) // 获取 logger
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

		// 计算剩余量
		used := nodeUsedMap[node.Name]
		free := allocatable - used
		if free < 0 {
			free = 0
		}

		// 只要节点有 GPU 能力 (allocatable > 0)，就记录下来，哪怕 free 是 0
		if allocatable > 0 {
			// [新增日志] 输出每个节点的资源详情
			logger.Info("节点 GPU 状态",
				"NodeName", node.Name,
				"Total(Allocatable)", allocatable,
				"Used", used,
				"Free(c_val)", free,
			)
			c = append(c, free)
		}
	}
	return c, nil
}

// solveGroupSelectionGLPK 完全复刻你提供的 GLPK 逻辑
// c: 容器容量数组
// candidates: 包含了 groups, w, p 的信息
// D: 约束参数数组
func solveGroupSelectionGLPK(c []float64, candidates []CandidatePG, D []float64) ([]int, float64, float64) {

	k := len(candidates) // 组的数量
	numConstraints := len(D)

	// --- 数据结构转换 ---
	var w []float64    // 所有 items 的重量
	var p []float64    // 组价值
	var groups [][]int // 组 -> w 中的索引列表

	currentIndex := 0
	for _, cand := range candidates {
		p = append(p, cand.Priority)

		var groupIndices []int
		// 展开 PodGroup 成员
		for i := 0; i < cand.MemberNum; i++ {
			w = append(w, cand.PodWeight)
			groupIndices = append(groupIndices, currentIndex)
			currentIndex++
		}
		groups = append(groups, groupIndices)
	}

	// ---------------------------------------------------------
	// 1. 初始化 GLPK 问题
	// ---------------------------------------------------------
	lp := C.glp_create_prob()
	defer C.glp_delete_prob(lp)

	C.glp_set_prob_name(lp, C.CString("mKP_D_Solver"))
	C.glp_set_obj_dir(lp, C.GLP_MAX)

	// ---------------------------------------------------------
	// 2. 定义行 (约束条件)
	// ---------------------------------------------------------
	numRows := 1 + numConstraints
	C.glp_add_rows(lp, C.int(numRows))

	// [约束1] 重量约束 (Total Capacity)
	totalCapacity := 0.0
	for _, val := range c {
		totalCapacity += val
	}

	C.glp_set_row_name(lp, 1, C.CString("Weight_Limit"))
	C.glp_set_row_bnds(lp, 1, C.GLP_UP, 0.0, C.double(totalCapacity))

	// [约束2...N] D集合约束
	for i, d := range D {
		rowIdx := C.int(i + 2)
		fTotal := 0.0
		// 计算 sum(fd(c_j, d))
		for _, capVal := range c {
			fTotal += fd(capVal, d)
		}

		name := C.CString(fmt.Sprintf("D_Constraint_%d", i))
		C.glp_set_row_name(lp, rowIdx, name)
		C.glp_set_row_bnds(lp, rowIdx, C.GLP_UP, 0.0, C.double(fTotal))
	}

	// ---------------------------------------------------------
	// 3. 定义列 (决策变量 z_i)
	// ---------------------------------------------------------
	C.glp_add_cols(lp, C.int(k))
	for i := 0; i < k; i++ {
		colIdx := C.int(i + 1)
		name := C.CString(fmt.Sprintf("z_%d", i))

		C.glp_set_col_name(lp, colIdx, name)
		C.glp_set_col_kind(lp, colIdx, C.GLP_BV) // Binary
		C.glp_set_obj_coef(lp, colIdx, C.double(p[i]))
	}

	// ---------------------------------------------------------
	// 4. 构建稀疏矩阵 (Matrix)
	// ---------------------------------------------------------
	// 计算矩阵最大元素个数
	maxElements := k*numRows + 1

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

	// 填充矩阵：重量约束行 (Row 1)
	for i := 0; i < k; i++ {
		gw := 0.0
		for _, itemIdx := range groups[i] {
			gw += w[itemIdx]
		}
		if gw != 0 {
			ia[idx] = 1
			ja[idx] = C.int(i + 1)
			ar[idx] = C.double(gw)
			idx++
		}
	}

	// 填充矩阵：D集合约束行 (Row 2...N)
	for dIdx, d := range D {
		rowIdx := C.int(dIdx + 2)
		for i := 0; i < k; i++ {
			val := 0.0
			for _, itemIdx := range groups[i] {
				// 这里的 w[itemIdx] 是单个 Pod 的重量
				// 你的逻辑核心：对每个 item 应用 fd 然后求和
				val += fd(w[itemIdx], d)
			}

			// 即使 val 是 0 也添加，保持逻辑结构一致（稀疏矩阵其实可以优化跳过）
			ia[idx] = rowIdx
			ja[idx] = C.int(i + 1)
			ar[idx] = C.double(val)
			idx++
		}
	}

	// 加载矩阵
	C.set_matrix(lp, C.int(idx-1), c_ia, c_ja, c_ar)

	// ---------------------------------------------------------
	// 5. 求解
	// ---------------------------------------------------------
	var iocp C.glp_iocp
	C.glp_init_iocp(&iocp)
	iocp.presolve = C.GLP_ON
	iocp.msg_lev = C.GLP_MSG_OFF // 关闭日志

	err := C.glp_intopt(lp, &iocp)
	if err != 0 {
		fmt.Printf("求解出错: %d\n", err)
		return nil, 0, 0
	}

	// ---------------------------------------------------------
	// 6. 输出结果
	// ---------------------------------------------------------
	status := C.glp_mip_status(lp)
	if status == C.GLP_OPT {
		totalVal := float64(C.glp_mip_obj_val(lp))
		totalWeight := 0.0
		var selectedIndices []int

		for i := 0; i < k; i++ {
			val := C.glp_mip_col_val(lp, C.int(i+1))
			if val > 0.5 {
				selectedIndices = append(selectedIndices, i)
				// 计算实际重量
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
