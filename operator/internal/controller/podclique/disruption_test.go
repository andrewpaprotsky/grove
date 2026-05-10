// /*
// Copyright 2026 The Grove Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// */

package podclique

import (
	"context"
	"testing"

	apicommon "github.com/ai-dynamo/grove/operator/api/common"
	apiconstants "github.com/ai-dynamo/grove/operator/api/common/constants"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	ctrlcommon "github.com/ai-dynamo/grove/operator/internal/controller/common"
	componentutils "github.com/ai-dynamo/grove/operator/internal/controller/common/component/utils"
	testutils "github.com/ai-dynamo/grove/operator/test/utils"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestProcessDisruptionPolicyDeletesManagedPodClique(t *testing.T) {
	for _, ownerKind := range []string{apiconstants.KindPodCliqueSet, apiconstants.KindPodCliqueScalingGroup} {
		t.Run(ownerKind, func(t *testing.T) {
			pclq, pod := disruptedPodClique(ownerKind)
			var deleteOptions *client.DeleteOptions
			reconciler := &Reconciler{client: fakeClientForDisruption(t, &deleteOptions, pclq, pod)}

			result := reconciler.processDisruptionPolicy(componentutils.WithPCLQPodsCache(context.Background()), logr.Discard(), pclq)

			assert.True(t, ctrlcommon.ShortCircuitReconcileFlow(result))
			require.NotNil(t, deleteOptions)
			require.NotNil(t, deleteOptions.PropagationPolicy)
			assert.Equal(t, metav1.DeletePropagationForeground, *deleteOptions.PropagationPolicy)
		})
	}
}

func TestPodMatchesDisruptionPolicy(t *testing.T) {
	tests := []struct {
		name string
		pod  *corev1.Pod
		want bool
	}{
		{name: "matching terminating pod", pod: disruptedPod(), want: true},
		{name: "omitted pattern status defaults to true", pod: disruptedPod(), want: true},
		{name: "ignores pod before termination", pod: podWithDisruptionCondition(corev1.ConditionTrue, grovecorev1alpha1.PodDisruptionReasonDeletionByTaintManager)},
		{name: "ignores other reason", pod: terminatingPodWithCondition(corev1.ConditionTrue, "PreemptionByScheduler")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			policy := testutils.NewPodCliqueDisruptionPolicy()
			if tt.name == "omitted pattern status defaults to true" {
				policy.Rules[0].OnPodConditions[0].Status = ""
			}
			assert.Equal(t, tt.want, podMatchesDisruptionPolicy(policy, tt.pod))
		})
	}
}

func disruptedPodClique(ownerKind string) (*grovecorev1alpha1.PodClique, *corev1.Pod) {
	ownerGVK := grovecorev1alpha1.SchemeGroupVersion.WithKind(ownerKind)
	pclq := testutils.NewPodCliqueBuilder("inference", uuid.NewUUID(), "worker", "default", 0).Build()
	pclq.Spec.Disruption = testutils.NewPodCliqueDisruptionPolicy()
	pclq.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: ownerGVK.GroupVersion().String(),
		Kind:       ownerKind,
		Name:       "inference",
		UID:        uuid.NewUUID(),
		Controller: ptr.To(true),
	}}
	pod := disruptedPod()
	pod.Name = pclq.Name + "-0"
	pod.Namespace = pclq.Namespace
	pod.Labels = map[string]string{apicommon.LabelPodClique: pclq.Name}
	pod.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: grovecorev1alpha1.SchemeGroupVersion.String(),
		Kind:       apiconstants.KindPodClique,
		Name:       pclq.Name,
		UID:        pclq.UID,
		Controller: ptr.To(true),
	}}
	return pclq, pod
}

func disruptedPod() *corev1.Pod {
	return terminatingPodWithCondition(corev1.ConditionTrue, grovecorev1alpha1.PodDisruptionReasonDeletionByTaintManager)
}

func terminatingPodWithCondition(status corev1.ConditionStatus, reason string) *corev1.Pod {
	pod := podWithDisruptionCondition(status, reason)
	pod.DeletionTimestamp = ptr.To(metav1.Now())
	pod.Finalizers = []string{"test.grove.io/finalizer"}
	return pod
}

func podWithDisruptionCondition(status corev1.ConditionStatus, reason string) *corev1.Pod {
	return &corev1.Pod{Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.DisruptionTarget, Status: status, Reason: reason}}}}
}

func fakeClientForDisruption(t *testing.T, deleteOptions **client.DeleteOptions, objects ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, grovecorev1alpha1.AddToScheme(scheme))
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).WithInterceptorFuncs(interceptor.Funcs{
		Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			*deleteOptions = (&client.DeleteOptions{}).ApplyOptions(opts)
			return cl.Delete(ctx, obj, opts...)
		},
	}).Build()
}
