package external

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// 定义 GroupVersion (与 crd.yaml 中的 scheduling.x-k8s.io/v1alpha1 一致)
var (
	GroupVersion  = schema.GroupVersion{Group: "scheduling.x-k8s.io", Version: "v1alpha1"}
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	AddToScheme   = SchemeBuilder.AddToScheme
)

// ---------------------------------------------------------
// 结构体定义 (严格匹配 crd.yaml)
// ---------------------------------------------------------

type PodGroup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PodGroupSpec   `json:"spec,omitempty"`
	Status PodGroupStatus `json:"status,omitempty"`
}

type PodGroupSpec struct {
	// MinMember defines the minimal number of members/tasks to run the pod group
	MinMember int32 `json:"minMember,omitempty"`

	// MinResources defines the minimal resource of members/tasks to run the pod group
	// 对应 CRD 中的 minResources，使用 corev1.ResourceList (即 map[string]Quantity)
	MinResources corev1.ResourceList `json:"minResources,omitempty"`

	// ScheduleTimeoutSeconds defines the maximal time of members/tasks to wait
	ScheduleTimeoutSeconds *int32 `json:"scheduleTimeoutSeconds,omitempty"`
}

type PodGroupStatus struct {
	// Current phase of PodGroup (e.g., Pending, Running, Scheduling)
	Phase string `json:"phase,omitempty"`

	// The number of actively running pods
	Running int32 `json:"running,omitempty"`

	// The number of pods which reached phase Succeeded
	Succeeded int32 `json:"succeeded,omitempty"`

	// The number of pods which reached phase Failed
	Failed int32 `json:"failed,omitempty"`

	// OccupiedBy marks the workload UID that occupy the podgroup
	OccupiedBy string `json:"occupiedBy,omitempty"`

	// ScheduleStartTime of the group
	ScheduleStartTime *metav1.Time `json:"scheduleStartTime,omitempty"`
}

type PodGroupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PodGroup `json:"items"`
}

func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(GroupVersion,
		&PodGroup{},
		&PodGroupList{},
	)
	metav1.AddToGroupVersion(scheme, GroupVersion)
	return nil
}

// ---------------------------------------------------------
// 手动 DeepCopy 实现 (已更新以支持 Map 和 Pointer)
// ---------------------------------------------------------

// DeepCopyInto 将 in 的内容拷贝到 out
func (in *PodGroup) DeepCopyInto(out *PodGroup) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)     // 调用 Spec 的拷贝
	in.Status.DeepCopyInto(&out.Status) // 调用 Status 的拷贝
}

func (in *PodGroup) DeepCopy() *PodGroup {
	if in == nil {
		return nil
	}
	out := new(PodGroup)
	in.DeepCopyInto(out)
	return out
}

func (in *PodGroup) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto Spec (需要特殊处理 Map 和 Pointer)
func (in *PodGroupSpec) DeepCopyInto(out *PodGroupSpec) {
	*out = *in
	// 处理 Map: MinResources
	if in.MinResources != nil {
		out.MinResources = make(corev1.ResourceList, len(in.MinResources))
		for key, val := range in.MinResources {
			out.MinResources[key] = val.DeepCopy()
		}
	}
	// 处理 Pointer: ScheduleTimeoutSeconds
	if in.ScheduleTimeoutSeconds != nil {
		in, out := &in.ScheduleTimeoutSeconds, &out.ScheduleTimeoutSeconds
		*out = new(int32)
		**out = **in
	}
}

// DeepCopyInto Status (需要特殊处理 Pointer)
func (in *PodGroupStatus) DeepCopyInto(out *PodGroupStatus) {
	*out = *in
	// 处理 Pointer: ScheduleStartTime
	if in.ScheduleStartTime != nil {
		in, out := &in.ScheduleStartTime, &out.ScheduleStartTime
		*out = (*in).DeepCopy()
	}
}

func (in *PodGroupList) DeepCopyInto(out *PodGroupList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		in, out := &in.Items, &out.Items
		*out = make([]PodGroup, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

func (in *PodGroupList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

func (in *PodGroupList) DeepCopy() *PodGroupList {
	if in == nil {
		return nil
	}
	out := new(PodGroupList)
	in.DeepCopyInto(out)
	return out
}
