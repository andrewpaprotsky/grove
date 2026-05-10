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

package validation

import (
	"context"
	"testing"

	apicommon "github.com/ai-dynamo/grove/operator/api/common"
	apiconstants "github.com/ai-dynamo/grove/operator/api/common/constants"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	testutils "github.com/ai-dynamo/grove/operator/test/utils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	admissionv1 "k8s.io/api/admission/v1"
	authenticationv1 "k8s.io/api/authentication/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

func TestValidatePodCliqueDisruptionPolicy(t *testing.T) {
	tests := []struct {
		name     string
		mutate   func(*grovecorev1alpha1.PodCliqueDisruptionPolicy)
		wantErrs int
	}{
		{name: "valid"},
		{name: "omitted status", mutate: func(policy *grovecorev1alpha1.PodCliqueDisruptionPolicy) {
			policy.Rules[0].OnPodConditions[0].Status = ""
		}},
		{name: "unsupported action", mutate: func(policy *grovecorev1alpha1.PodCliqueDisruptionPolicy) { policy.Rules[0].Action = "DeletePods" }, wantErrs: 1},
		{name: "unsupported condition", mutate: func(policy *grovecorev1alpha1.PodCliqueDisruptionPolicy) {
			policy.Rules[0].OnPodConditions[0] = grovecorev1alpha1.PodConditionPattern{Type: corev1.PodReady, Status: corev1.ConditionFalse, Reason: "OtherReason"}
		}, wantErrs: 3},
		{name: "multiple rules", mutate: func(policy *grovecorev1alpha1.PodCliqueDisruptionPolicy) {
			policy.Rules = append(policy.Rules, policy.Rules[0])
		}, wantErrs: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			policy := testutils.NewPodCliqueDisruptionPolicy()
			if tt.mutate != nil {
				tt.mutate(policy)
			}
			assert.Len(t, validatePodCliqueDisruptionPolicy(policy, field.NewPath("spec").Child("disruption")), tt.wantErrs)
		})
	}
}

func TestValidatePodCliqueDisruptionOwner(t *testing.T) {
	pcsUID, pcsgUID := uuid.NewUUID(), uuid.NewUUID()
	pcs := validationPCS("inference", pcsUID)
	pcsgPCS := validationPCSWithPCSG("pcsg-inference", uuid.NewUUID())
	pcsg := validationPCSG(pcsgPCS, "workers", pcsgUID)
	reconcilerUser := "system:serviceaccount:grove-system:grove-operator"
	tests := []struct {
		name     string
		pclq     *grovecorev1alpha1.PodClique
		objects  []client.Object
		wantErrs int
	}{
		{name: "allows unset policy", pclq: &grovecorev1alpha1.PodClique{}},
		{name: "rejects direct PodClique", pclq: &grovecorev1alpha1.PodClique{Spec: grovecorev1alpha1.PodCliqueSpec{Disruption: testutils.NewPodCliqueDisruptionPolicy()}}, wantErrs: 1},
		{name: "allows PodCliqueSet owner", pclq: validationPCSOwnedPCLQ(pcs, "worker", 0, pcsUID), objects: []client.Object{pcs}},
		{name: "allows PodCliqueScalingGroup owner", pclq: validationPCSGOwnedPCLQ(pcsgPCS, pcsg, "worker", 0, pcsgUID), objects: []client.Object{pcsgPCS, pcsg}},
		{name: "rejects fake owner UID", pclq: validationPCSOwnedPCLQ(pcs, "worker", 0, uuid.NewUUID()), objects: []client.Object{pcs}, wantErrs: 1},
		{name: "rejects non-generated name", pclq: func() *grovecorev1alpha1.PodClique {
			pclq := validationPCSOwnedPCLQ(pcs, "worker", 0, pcsUID)
			pclq.Name = "worker"
			return pclq
		}(), objects: []client.Object{pcs}, wantErrs: 1},
		{name: "rejects PCSG clique outside membership", pclq: validationPCSGOwnedPCLQ(pcsgPCS, pcsg, "other", 0, pcsgUID), objects: []client.Object{pcsgPCS, pcsg}, wantErrs: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := &PodCliqueHandler{client: validationFakeClient(t, tt.objects...), reconcilerServiceAccountUserName: reconcilerUser}
			_, err := handler.validate(admissionContext(reconcilerUser), nil, tt.pclq)
			assert.Equal(t, tt.wantErrs > 0, err != nil)
		})
	}
}

func TestValidatePodCliqueDisruptionMutationAuthorization(t *testing.T) {
	pcs := validationPCS("inference", uuid.NewUUID())
	oldPCLQ := validationPCSOwnedPCLQ(pcs, "worker", 0, pcs.UID)
	reconcilerUser := "system:serviceaccount:grove-system:grove-operator"
	exemptUser := "system:serviceaccount:default:breakglass"
	handler := &PodCliqueHandler{
		client:                           validationFakeClient(t, pcs),
		reconcilerServiceAccountUserName: reconcilerUser,
		exemptServiceAccountUserNames:    []string{exemptUser},
	}

	tests := []struct {
		name    string
		user    string
		oldObj  *grovecorev1alpha1.PodClique
		newObj  *grovecorev1alpha1.PodClique
		wantErr bool
	}{
		{name: "allows create by reconciler", user: reconcilerUser, newObj: oldPCLQ.DeepCopy()},
		{name: "rejects create by ordinary user", user: "alice", newObj: oldPCLQ.DeepCopy(), wantErr: true},
		{name: "rejects change by ordinary user", user: "alice", oldObj: oldPCLQ.DeepCopy(), newObj: withChangedDisruption(oldPCLQ), wantErr: true},
		{name: "rejects removal by ordinary user", user: "alice", oldObj: oldPCLQ.DeepCopy(), newObj: withoutDisruption(oldPCLQ), wantErr: true},
		{name: "allows unchanged disruption by ordinary user", user: "alice", oldObj: oldPCLQ.DeepCopy(), newObj: oldPCLQ.DeepCopy()},
		{name: "allows change by exempt user", user: exemptUser, oldObj: oldPCLQ.DeepCopy(), newObj: withChangedDisruption(oldPCLQ)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var err error
			ctx := admissionContext(tt.user)
			if tt.oldObj == nil {
				_, err = handler.ValidateCreate(ctx, tt.newObj)
			} else {
				_, err = handler.ValidateUpdate(ctx, tt.oldObj, tt.newObj)
			}
			assert.Equal(t, tt.wantErr, err != nil)
		})
	}
}

func withChangedDisruption(pclq *grovecorev1alpha1.PodClique) *grovecorev1alpha1.PodClique {
	pclq = pclq.DeepCopy()
	pclq.Spec.Disruption.Rules[0].OnPodConditions[0].Status = ""
	return pclq
}

func withoutDisruption(pclq *grovecorev1alpha1.PodClique) *grovecorev1alpha1.PodClique {
	pclq = pclq.DeepCopy()
	pclq.Spec.Disruption = nil
	return pclq
}

func TestValidatePodCliqueSpecRejectsDisruptionWithPodCliqueScaleConfig(t *testing.T) {
	minReplicas := int32(1)
	validator := &pcsValidator{pcs: validationPCS("inference", uuid.NewUUID())}
	_, errs := validator.validatePodCliqueSpec("worker", grovecorev1alpha1.PodCliqueSpec{
		Replicas:     1,
		MinAvailable: ptr.To[int32](1),
		ScaleConfig: &grovecorev1alpha1.AutoScalingConfig{
			MinReplicas: &minReplicas,
			MaxReplicas: 1,
			Metrics:     []autoscalingv2.MetricSpec{},
		},
		Disruption: testutils.NewPodCliqueDisruptionPolicy(),
	}, field.NewPath("spec"))

	assert.NotEmpty(t, errs)
	assert.Contains(t, errs.ToAggregate().Error(), "cannot be used with podClique autoScalingConfig")
}

func TestValidatePodCliqueUpdateAllowsTerminatingObject(t *testing.T) {
	oldPCLQ := validationPCSOwnedPCLQ(validationPCS("missing", uuid.NewUUID()), "worker", 0, uuid.NewUUID())
	oldPCLQ.DeletionTimestamp = ptr.To(metav1.Now())
	oldPCLQ.Finalizers = []string{apiconstants.FinalizerPodClique}
	newPCLQ := oldPCLQ.DeepCopy()
	newPCLQ.Finalizers = nil

	_, err := (&PodCliqueHandler{client: validationFakeClient(t)}).ValidateUpdate(admissionContext("alice"), oldPCLQ, newPCLQ)

	assert.NoError(t, err)
}

func validationPCS(name string, uid types.UID) *grovecorev1alpha1.PodCliqueSet {
	return testutils.NewPodCliqueSetBuilder(name, "default", uid).
		WithPodCliqueTemplateSpec(testutils.NewPodCliqueTemplateSpecBuilder("worker").WithMinAvailable(1).Build()).
		Build()
}

func validationPCSWithPCSG(name string, uid types.UID) *grovecorev1alpha1.PodCliqueSet {
	return testutils.NewPodCliqueSetBuilder(name, "default", uid).
		WithScalingGroupConfig("workers", []string{"worker"}, 1, 1).
		Build()
}

func validationPCSG(pcs *grovecorev1alpha1.PodCliqueSet, name string, uid types.UID) *grovecorev1alpha1.PodCliqueScalingGroup {
	pcsgName := apicommon.GeneratePodCliqueScalingGroupName(apicommon.ResourceNameReplica{Name: pcs.Name, Replica: 0}, name)
	return &grovecorev1alpha1.PodCliqueScalingGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pcsgName,
			Namespace: pcs.Namespace,
			UID:       uid,
			Labels: map[string]string{
				apicommon.LabelPartOfKey:                pcs.Name,
				apicommon.LabelPodCliqueSetReplicaIndex: "0",
			},
			OwnerReferences: []metav1.OwnerReference{controllerRef(apiconstants.KindPodCliqueSet, pcs.Name, pcs.UID)},
		},
		Spec: grovecorev1alpha1.PodCliqueScalingGroupSpec{
			Replicas:     1,
			MinAvailable: ptr.To[int32](1),
			CliqueNames:  []string{"worker"},
		},
	}
}

func validationPCSOwnedPCLQ(pcs *grovecorev1alpha1.PodCliqueSet, cliqueName string, replicaIndex int, ownerUID types.UID) *grovecorev1alpha1.PodClique {
	pclq := testutils.NewPodCliqueBuilder(pcs.Name, ownerUID, cliqueName, pcs.Namespace, int32(replicaIndex)).Build()
	pclq.Spec.Disruption = testutils.NewPodCliqueDisruptionPolicy()
	return pclq
}

func validationPCSGOwnedPCLQ(pcs *grovecorev1alpha1.PodCliqueSet, pcsg *grovecorev1alpha1.PodCliqueScalingGroup, cliqueName string, replicaIndex int, ownerUID types.UID) *grovecorev1alpha1.PodClique {
	pclqName := apicommon.GeneratePodCliqueName(apicommon.ResourceNameReplica{Name: pcsg.Name, Replica: replicaIndex}, cliqueName)
	pclq := testutils.NewPCSGPodCliqueBuilder(pclqName, pcsg.Namespace, pcs.Name, pcsg.Name, 0, replicaIndex).Build()
	pclq.OwnerReferences = []metav1.OwnerReference{controllerRef(apiconstants.KindPodCliqueScalingGroup, pcsg.Name, ownerUID)}
	pclq.Spec.Disruption = testutils.NewPodCliqueDisruptionPolicy()
	return pclq
}

func controllerRef(ownerKind, ownerName string, ownerUID types.UID) metav1.OwnerReference {
	ownerGVK := grovecorev1alpha1.SchemeGroupVersion.WithKind(ownerKind)
	return metav1.OwnerReference{
		APIVersion: ownerGVK.GroupVersion().String(),
		Kind:       ownerKind,
		Name:       ownerName,
		UID:        ownerUID,
		Controller: ptr.To(true),
	}
}

func admissionContext(username string) context.Context {
	return admission.NewContextWithRequest(context.Background(), admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		UserInfo: authenticationv1.UserInfo{Username: username},
	}})
}

func validationFakeClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, grovecorev1alpha1.AddToScheme(scheme))
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
}
