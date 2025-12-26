package external

import (
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// 定义 GroupVersion
var (
	GroupVersion  = schema.GroupVersion{Group: "scheduling.x-k8s.io", Version: "v1alpha1"}
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	AddToScheme   = SchemeBuilder.AddToScheme
)

// ---------------------------------------------------------
// 结构体定义
// ---------------------------------------------------------

type PodGroup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PodGroupSpec   `json:"spec,omitempty"`
	Status PodGroupStatus `json:"status,omitempty"`
}

type PodGroupSpec struct {
	MinMember int32 `json:"minMember,omitempty"`

	// 注意：这里定义的是 map[string]resource.Quantity
	MinResources map[string]resource.Quantity `json:"minResources,omitempty"`

	ScheduleTimeoutSeconds int32 `json:"scheduleTimeoutSeconds,omitempty"`

	// 新增字段
	Image    string `json:"image,omitempty"`
	Priority int32  `json:"priority,omitempty"`
}

type PodGroupStatus struct {
	Phase             string       `json:"phase,omitempty"`
	Running           int32        `json:"running,omitempty"`
	Succeeded         int32        `json:"succeeded,omitempty"`
	Failed            int32        `json:"failed,omitempty"`
	OccupiedBy        string       `json:"occupiedBy,omitempty"`
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
// DeepCopy 实现
// ---------------------------------------------------------

func (in *PodGroup) DeepCopyInto(out *PodGroup) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
	in.Status.DeepCopyInto(&out.Status)
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

// DeepCopyInto Spec (修正了这里的 map 初始化类型)
func (in *PodGroupSpec) DeepCopyInto(out *PodGroupSpec) {
	*out = *in
	// 处理 Map: MinResources
	if in.MinResources != nil {
		// [修正点] 使用 map[string]resource.Quantity 而不是 corev1.ResourceList
		out.MinResources = make(map[string]resource.Quantity, len(in.MinResources))
		for key, val := range in.MinResources {
			out.MinResources[key] = val.DeepCopy()
		}
	}
	// int32 和 string 是值类型，直接赋值即可，无需处理
}

// DeepCopyInto Status
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
