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
	"fmt"
	"slices"
	"strconv"

	apicommon "github.com/ai-dynamo/grove/operator/api/common"
	apiconstants "github.com/ai-dynamo/grove/operator/api/common/constants"
	configv1alpha1 "github.com/ai-dynamo/grove/operator/api/config/v1alpha1"
	"github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	componentutils "github.com/ai-dynamo/grove/operator/internal/controller/common/component/utils"
	ctrlutils "github.com/ai-dynamo/grove/operator/internal/controller/utils"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

const (
	// PodCliqueName is the validating webhook handler name for PodCliques.
	PodCliqueName        = "podclique-validating-webhook"
	podCliqueWebhookPath = "/webhooks/validate-podclique"
)

// PodCliqueHandler validates PodClique resources.
type PodCliqueHandler struct {
	client                           client.Reader
	reconcilerServiceAccountUserName string
	exemptServiceAccountUserNames    []string
}

// NewPodCliqueHandler creates a PodClique validating webhook handler.
func NewPodCliqueHandler(mgr manager.Manager, authorizerCfg configv1alpha1.AuthorizerConfig, reconcilerServiceAccountUserName string) *PodCliqueHandler {
	return &PodCliqueHandler{
		client:                           mgr.GetAPIReader(),
		reconcilerServiceAccountUserName: reconcilerServiceAccountUserName,
		exemptServiceAccountUserNames:    authorizerCfg.ExemptServiceAccountUserNames,
	}
}

// RegisterWithManager registers the PodClique validating webhook.
func (h *PodCliqueHandler) RegisterWithManager(mgr manager.Manager) error {
	webhook := admission.WithCustomValidator(mgr.GetScheme(), &v1alpha1.PodClique{}, h).WithRecoverPanic(true)
	mgr.GetWebhookServer().Register(podCliqueWebhookPath, webhook)
	return nil
}

func (h *PodCliqueHandler) ValidateCreate(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	pclq, err := castToPodClique(obj)
	if err != nil {
		return nil, err
	}
	return h.validate(ctx, nil, pclq)
}

func (h *PodCliqueHandler) ValidateUpdate(ctx context.Context, oldObj, newObj runtime.Object) (admission.Warnings, error) {
	oldPCLQ, err := castToPodClique(oldObj)
	if err != nil {
		return nil, err
	}
	newPCLQ, err := castToPodClique(newObj)
	if err != nil {
		return nil, err
	}
	if isTerminatingFinalizerOnlyUpdate(oldPCLQ, newPCLQ) {
		return nil, nil
	}
	return h.validate(ctx, oldPCLQ, newPCLQ)
}

func (h *PodCliqueHandler) ValidateDelete(context.Context, runtime.Object) (admission.Warnings, error) {
	return nil, nil
}

func (h *PodCliqueHandler) validate(ctx context.Context, oldPCLQ, newPCLQ *v1alpha1.PodClique) (admission.Warnings, error) {
	allErrs := validatePodCliqueDisruptionPolicy(newPCLQ.Spec.Disruption, field.NewPath("spec").Child("disruption"))
	if hasDisruptionField(oldPCLQ, newPCLQ) {
		allErrs = append(allErrs, h.validateDisruptionMutationAllowed(ctx, oldPCLQ, newPCLQ)...)
		allErrs = append(allErrs, h.validateDisruptionOwner(ctx, newPCLQ)...)
	}
	return nil, allErrs.ToAggregate()
}

func (h *PodCliqueHandler) validateDisruptionOwner(ctx context.Context, pclq *v1alpha1.PodClique) field.ErrorList {
	owner := metav1.GetControllerOf(pclq)
	labels := pclq.GetLabels()
	fldPath := field.NewPath("metadata")
	if owner == nil || labels[apicommon.LabelPartOfKey] == "" || !ctrlutils.IsManagedPodClique(pclq, apiconstants.KindPodCliqueSet, apiconstants.KindPodCliqueScalingGroup) {
		return field.ErrorList{field.Invalid(fldPath, pclq.Name, "spec.disruption is supported only on Grove-managed PodCliques owned by a PodCliqueSet or PodCliqueScalingGroup")}
	}

	switch owner.Kind {
	case apiconstants.KindPodCliqueSet:
		pcs := &v1alpha1.PodCliqueSet{}
		if errs := h.getLiveOwner(ctx, pclq, owner, pcs, fldPath); len(errs) > 0 {
			return errs
		}
		if owner.Name != labels[apicommon.LabelPartOfKey] {
			return field.ErrorList{field.Invalid(fldPath.Child("labels").Key(apicommon.LabelPartOfKey), labels[apicommon.LabelPartOfKey], "must match the PodCliqueSet owner")}
		}
		replicaIndex, errs := replicaIndex(labels, apicommon.LabelPodCliqueSetReplicaIndex, int(pcs.Spec.Replicas), fldPath)
		if len(errs) > 0 {
			return errs
		}
		return validateGeneratedPodClique(pclq, componentutils.GetPodCliqueFQNsForPCSReplicaNotInPCSG(pcs, replicaIndex), apicommon.LabelComponentNamePodCliqueSetPodClique, fldPath)

	case apiconstants.KindPodCliqueScalingGroup:
		return h.validatePodCliqueScalingGroupOwner(ctx, pclq, owner, fldPath)
	default:
		return field.ErrorList{field.NotSupported(fldPath.Child("ownerReferences"), owner.Kind, []string{apiconstants.KindPodCliqueSet, apiconstants.KindPodCliqueScalingGroup})}
	}
}

func (h *PodCliqueHandler) getLiveOwner(ctx context.Context, pclq *v1alpha1.PodClique, owner *metav1.OwnerReference, ownerObject client.Object, fldPath *field.Path) field.ErrorList {
	if err := h.client.Get(ctx, client.ObjectKey{Namespace: pclq.Namespace, Name: owner.Name}, ownerObject); err != nil {
		if apierrors.IsNotFound(err) {
			return field.ErrorList{field.NotFound(fldPath.Child("ownerReferences"), owner.Name)}
		}
		return field.ErrorList{field.InternalError(fldPath.Child("ownerReferences"), err)}
	}
	if ownerObject.GetUID() != owner.UID {
		return field.ErrorList{field.Invalid(fldPath.Child("ownerReferences"), owner.UID, fmt.Sprintf("%s owner UID does not match", owner.Kind))}
	}
	return nil
}

func (h *PodCliqueHandler) validatePodCliqueScalingGroupOwner(ctx context.Context, pclq *v1alpha1.PodClique, owner *metav1.OwnerReference, fldPath *field.Path) field.ErrorList {
	pcsg := &v1alpha1.PodCliqueScalingGroup{}
	if errs := h.getLiveOwner(ctx, pclq, owner, pcsg, fldPath); len(errs) > 0 {
		return errs
	}
	labels := pclq.Labels
	if pcsg.Labels[apicommon.LabelPartOfKey] == "" || pcsg.Labels[apicommon.LabelPartOfKey] != labels[apicommon.LabelPartOfKey] {
		return field.ErrorList{field.Invalid(fldPath.Child("ownerReferences"), pcsg.Name, "PodCliqueScalingGroup owner belongs to a different PodCliqueSet")}
	}
	if labels[apicommon.LabelPodCliqueScalingGroup] != pcsg.Name {
		return field.ErrorList{field.Invalid(fldPath.Child("labels").Key(apicommon.LabelPodCliqueScalingGroup), pclq.Labels[apicommon.LabelPodCliqueScalingGroup], "must match the PodCliqueScalingGroup owner")}
	}

	pcsName := pcsg.Labels[apicommon.LabelPartOfKey]
	pcs := &v1alpha1.PodCliqueSet{}
	if err := h.client.Get(ctx, client.ObjectKey{Namespace: pcsg.Namespace, Name: pcsName}, pcs); err != nil {
		if apierrors.IsNotFound(err) {
			return field.ErrorList{field.NotFound(fldPath.Child("ownerReferences"), pcsName)}
		}
		return field.ErrorList{field.InternalError(fldPath.Child("ownerReferences"), err)}
	}
	pcsgOwner := metav1.GetControllerOf(pcsg)
	if pcsgOwner == nil || pcsgOwner.Kind != apiconstants.KindPodCliqueSet || pcsgOwner.Name != pcs.Name || pcsgOwner.UID != pcs.UID {
		return field.ErrorList{field.Invalid(fldPath.Child("ownerReferences"), pcsg.Name, "PodCliqueScalingGroup owner must be generated by the parent PodCliqueSet")}
	}

	pcsReplicaIndex, errs := replicaIndex(labels, apicommon.LabelPodCliqueSetReplicaIndex, int(pcs.Spec.Replicas), fldPath)
	if len(errs) > 0 {
		return errs
	}
	pcsgPCSReplicaIndex, errs := replicaIndex(pcsg.Labels, apicommon.LabelPodCliqueSetReplicaIndex, int(pcs.Spec.Replicas), fldPath)
	if len(errs) > 0 || pcsgPCSReplicaIndex != pcsReplicaIndex {
		return field.ErrorList{field.Invalid(fldPath.Child("labels").Key(apicommon.LabelPodCliqueSetReplicaIndex), pclq.Labels[apicommon.LabelPodCliqueSetReplicaIndex], "must match the PodCliqueScalingGroup PodCliqueSet replica label")}
	}
	pcsgConfig := matchingPCSGConfig(pcs, pcsg, pcsReplicaIndex)
	if pcsgConfig == nil {
		return field.ErrorList{field.Invalid(fldPath.Child("ownerReferences"), pcsg.Name, "PodCliqueScalingGroup name is not generated from the parent PodCliqueSet template")}
	}

	pcsgReplicaIndex, errs := replicaIndex(labels, apicommon.LabelPodCliqueScalingGroupReplicaIndex, int(pcsg.Spec.Replicas), fldPath)
	if len(errs) > 0 {
		return errs
	}
	expectedFromSpec := generatedPodCliqueNames(pcsg.Name, pcsgReplicaIndex, pcsg.Spec.CliqueNames)
	expectedFromTemplate := generatedPodCliqueNames(pcsg.Name, pcsgReplicaIndex, pcsgConfig.CliqueNames)
	if !slices.Contains(expectedFromSpec, pclq.Name) || !slices.Contains(expectedFromTemplate, pclq.Name) {
		return field.ErrorList{field.Invalid(fldPath.Child("name"), pclq.Name, "clique is not a member of the owning PodCliqueScalingGroup")}
	}
	return validateGeneratedPodClique(pclq, expectedFromSpec, apicommon.LabelComponentNamePodCliqueScalingGroupPodClique, fldPath)
}

func (h *PodCliqueHandler) validateDisruptionMutationAllowed(ctx context.Context, oldPCLQ, newPCLQ *v1alpha1.PodClique) field.ErrorList {
	if !hasDisruptionMutation(oldPCLQ, newPCLQ) {
		return nil
	}
	req, err := admission.RequestFromContext(ctx)
	if err != nil {
		return field.ErrorList{field.InternalError(field.NewPath("spec").Child("disruption"), err)}
	}
	if (h.reconcilerServiceAccountUserName != "" && req.UserInfo.Username == h.reconcilerServiceAccountUserName) || slices.Contains(h.exemptServiceAccountUserNames, req.UserInfo.Username) {
		return nil
	}
	return field.ErrorList{field.Forbidden(field.NewPath("spec").Child("disruption"), fmt.Sprintf("may only be changed by the Grove reconciler service account or configured exempt users; got user %q", req.UserInfo.Username))}
}

func replicaIndex(labels map[string]string, label string, replicas int, fldPath *field.Path) (int, field.ErrorList) {
	index, err := strconv.Atoi(labels[label])
	if err != nil || index < 0 || index >= replicas {
		return 0, field.ErrorList{field.Invalid(fldPath.Child("labels").Key(label), labels[label], "must be a valid replica index")}
	}
	return index, nil
}

func validateGeneratedPodClique(pclq *v1alpha1.PodClique, expectedNames []string, component string, fldPath *field.Path) field.ErrorList {
	if !slices.Contains(expectedNames, pclq.Name) {
		return field.ErrorList{field.Invalid(fldPath.Child("name"), pclq.Name, "must be a generated PodClique name for the owner replica")}
	}
	if pclq.Labels[apicommon.LabelComponentKey] != component {
		return field.ErrorList{field.Invalid(fldPath.Child("labels").Key(apicommon.LabelComponentKey), pclq.Labels[apicommon.LabelComponentKey], "must identify the generated PodClique owner type")}
	}
	return nil
}

func generatedPodCliqueNames(ownerName string, replicaIndex int, cliqueNames []string) []string {
	names := make([]string, 0, len(cliqueNames))
	for _, name := range cliqueNames {
		names = append(names, apicommon.GeneratePodCliqueName(apicommon.ResourceNameReplica{Name: ownerName, Replica: replicaIndex}, name))
	}
	return names
}

func matchingPCSGConfig(pcs *v1alpha1.PodCliqueSet, pcsg *v1alpha1.PodCliqueScalingGroup, pcsReplicaIndex int) *v1alpha1.PodCliqueScalingGroupConfig {
	for i, config := range pcs.Spec.Template.PodCliqueScalingGroupConfigs {
		pcsgName := apicommon.GeneratePodCliqueScalingGroupName(apicommon.ResourceNameReplica{Name: pcs.Name, Replica: pcsReplicaIndex}, config.Name)
		if pcsgName == pcsg.Name {
			return &pcs.Spec.Template.PodCliqueScalingGroupConfigs[i]
		}
	}
	return nil
}

func hasDisruptionField(oldPCLQ, newPCLQ *v1alpha1.PodClique) bool {
	return newPCLQ.Spec.Disruption != nil || oldPCLQ != nil && oldPCLQ.Spec.Disruption != nil
}

func hasDisruptionMutation(oldPCLQ, newPCLQ *v1alpha1.PodClique) bool {
	if oldPCLQ == nil {
		return newPCLQ.Spec.Disruption != nil
	}
	return !apiequality.Semantic.DeepEqual(oldPCLQ.Spec.Disruption, newPCLQ.Spec.Disruption)
}

func isTerminatingFinalizerOnlyUpdate(oldPCLQ, newPCLQ *v1alpha1.PodClique) bool {
	if newPCLQ.DeletionTimestamp.IsZero() {
		return false
	}
	oldCopy := oldPCLQ.DeepCopy()
	newCopy := newPCLQ.DeepCopy()
	oldCopy.Finalizers = newCopy.Finalizers
	oldCopy.ResourceVersion = newCopy.ResourceVersion
	oldCopy.ManagedFields = newCopy.ManagedFields
	oldCopy.DeletionTimestamp = newCopy.DeletionTimestamp
	oldCopy.DeletionGracePeriodSeconds = ptr.To(ptr.Deref(newCopy.DeletionGracePeriodSeconds, 0))
	newCopy.DeletionGracePeriodSeconds = ptr.To(ptr.Deref(newCopy.DeletionGracePeriodSeconds, 0))
	return apiequality.Semantic.DeepEqual(oldCopy, newCopy)
}

func castToPodClique(obj runtime.Object) (*v1alpha1.PodClique, error) {
	pclq, ok := obj.(*v1alpha1.PodClique)
	if !ok {
		return nil, fmt.Errorf("expected a PodClique object but got %T", obj)
	}
	return pclq, nil
}

var _ admission.CustomValidator = &PodCliqueHandler{}
