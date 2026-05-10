// /*
// Copyright 2024 The Grove Authors.
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

package webhook

import (
	"os"
	"path/filepath"
	"testing"

	configv1alpha1 "github.com/ai-dynamo/grove/operator/api/config/v1alpha1"
	"github.com/ai-dynamo/grove/operator/internal/constants"
	testutils "github.com/ai-dynamo/grove/operator/test/utils"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
)

// TestGenerateReconcilerServiceAccountUsername tests the generation of service account usernames
// in the Kubernetes format.
func TestGenerateReconcilerServiceAccountUsername(t *testing.T) {
	tests := []struct {
		// name identifies this test case
		name string
		// namespace is the Kubernetes namespace of the service account
		namespace string
		// serviceAccountName is the name of the service account
		serviceAccountName string
		// expected is the expected formatted username string
		expected string
	}{
		{
			name:               "standard namespace and service account",
			namespace:          "default",
			serviceAccountName: "grove-operator",
			expected:           "system:serviceaccount:default:grove-operator",
		},
		{
			name:               "custom namespace and service account",
			namespace:          "grove-system",
			serviceAccountName: "operator-sa",
			expected:           "system:serviceaccount:grove-system:operator-sa",
		},
		{
			name:               "hyphenated names",
			namespace:          "my-custom-namespace",
			serviceAccountName: "my-service-account",
			expected:           "system:serviceaccount:my-custom-namespace:my-service-account",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := generateReconcilerServiceAccountUsername(tt.namespace, tt.serviceAccountName)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestRegisterWebhooks_WithoutAuthorizer tests webhook registration when authorizer is disabled.
func TestRegisterWebhooks_WithoutAuthorizer(t *testing.T) {
	cl := testutils.NewTestClientBuilder().Build()
	mgr := &testutils.FakeManager{
		Client: cl,
		Scheme: cl.Scheme(),
		Logger: logr.Discard(),
	}

	// Create a real webhook server
	server := webhook.NewServer(webhook.Options{
		Port: 9443,
	})
	mgr.WebhookServer = server

	t.Setenv(constants.EnvVarServiceAccountName, "test-sa")
	setTestNamespaceFile(t, "test-namespace")

	// Authorizer disabled
	authorizerConfig := configv1alpha1.AuthorizerConfig{
		Enabled: false,
	}

	operatorCfg := configv1alpha1.OperatorConfiguration{
		Authorizer:              authorizerConfig,
		TopologyAwareScheduling: configv1alpha1.TopologyAwareSchedulingConfiguration{},
		Network:                 configv1alpha1.NetworkAcceleration{},
		Scheduler:               configv1alpha1.SchedulerConfiguration{Profiles: []configv1alpha1.SchedulerProfile{{Name: configv1alpha1.SchedulerNameKube}}, DefaultProfileName: string(configv1alpha1.SchedulerNameKube)},
	}
	err := Register(mgr, &operatorCfg)
	require.NoError(t, err)
}

// TestRegisterWebhooks_WithoutAuthorizerMissingEnvVar tests that registration fails
// when the reconciler service account environment variable is missing.
func TestRegisterWebhooks_WithoutAuthorizerMissingEnvVar(t *testing.T) {
	cl := testutils.NewTestClientBuilder().Build()
	mgr := &testutils.FakeManager{
		Client: cl,
		Scheme: cl.Scheme(),
		Logger: logr.Discard(),
	}

	// Create a real webhook server
	server := webhook.NewServer(webhook.Options{
		Port: 9443,
	})
	mgr.WebhookServer = server

	// Ensure env var is not set
	err := os.Unsetenv(constants.EnvVarServiceAccountName)
	require.NoError(t, err)

	// Authorizer disabled, but PodClique validation still needs the reconciler identity.
	authorizerConfig := configv1alpha1.AuthorizerConfig{
		Enabled: false,
	}

	operatorCfg := configv1alpha1.OperatorConfiguration{
		Authorizer:              authorizerConfig,
		TopologyAwareScheduling: configv1alpha1.TopologyAwareSchedulingConfiguration{},
		Network:                 configv1alpha1.NetworkAcceleration{},
		Scheduler:               configv1alpha1.SchedulerConfiguration{Profiles: []configv1alpha1.SchedulerProfile{{Name: configv1alpha1.SchedulerNameKube}}, DefaultProfileName: string(configv1alpha1.SchedulerNameKube)},
	}
	err = Register(mgr, &operatorCfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), constants.EnvVarServiceAccountName)
}

// TestRegisterWebhooks_WithAuthorizerMissingNamespaceFile tests that registration fails
// when the reconciler service account namespace file is missing.
func TestRegisterWebhooks_WithAuthorizerMissingNamespaceFile(t *testing.T) {
	cl := testutils.NewTestClientBuilder().Build()
	mgr := &testutils.FakeManager{
		Client: cl,
		Scheme: cl.Scheme(),
		Logger: logr.Discard(),
	}

	// Create a real webhook server
	server := webhook.NewServer(webhook.Options{
		Port: 9443,
	})
	mgr.WebhookServer = server

	// Set env var
	t.Setenv(constants.EnvVarServiceAccountName, "test-sa")
	setMissingNamespaceFile(t)

	// Authorizer enabled - will fail on reading non-existent namespace file
	authorizerConfig := configv1alpha1.AuthorizerConfig{
		Enabled: true,
	}

	operatorCfg := configv1alpha1.OperatorConfiguration{
		Authorizer:              authorizerConfig,
		TopologyAwareScheduling: configv1alpha1.TopologyAwareSchedulingConfiguration{},
		Network:                 configv1alpha1.NetworkAcceleration{},
		Scheduler:               configv1alpha1.SchedulerConfiguration{Profiles: []configv1alpha1.SchedulerProfile{{Name: configv1alpha1.SchedulerNameKube}}, DefaultProfileName: string(configv1alpha1.SchedulerNameKube)},
	}
	err := Register(mgr, &operatorCfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "error reading namespace file")
}

// TestRegisterWebhooks_WithAuthorizerSuccess tests successful webhook registration
// when authorizer is enabled and all reconciler identity requirements are met.
func TestRegisterWebhooks_WithAuthorizerSuccess(t *testing.T) {
	cl := testutils.NewTestClientBuilder().Build()
	mgr := &testutils.FakeManager{
		Client: cl,
		Scheme: cl.Scheme(),
		Logger: logr.Discard(),
	}

	// Create a real webhook server
	server := webhook.NewServer(webhook.Options{
		Port: 9443,
	})
	mgr.WebhookServer = server

	// Set env var
	t.Setenv(constants.EnvVarServiceAccountName, "test-sa")
	setTestNamespaceFile(t, "test-namespace")

	authorizerConfig := configv1alpha1.AuthorizerConfig{
		Enabled: true,
	}

	operatorCfg := configv1alpha1.OperatorConfiguration{
		Authorizer:              authorizerConfig,
		TopologyAwareScheduling: configv1alpha1.TopologyAwareSchedulingConfiguration{},
		Network:                 configv1alpha1.NetworkAcceleration{},
		Scheduler:               configv1alpha1.SchedulerConfiguration{Profiles: []configv1alpha1.SchedulerProfile{{Name: configv1alpha1.SchedulerNameKube}}, DefaultProfileName: string(configv1alpha1.SchedulerNameKube)},
	}
	err := Register(mgr, &operatorCfg)
	require.NoError(t, err)
}

func setTestNamespaceFile(t *testing.T, namespace string) {
	t.Helper()
	namespaceFile := filepath.Join(t.TempDir(), "namespace")
	require.NoError(t, os.WriteFile(namespaceFile, []byte(namespace), 0600))
	setOperatorNamespaceFile(t, namespaceFile)
}

func setMissingNamespaceFile(t *testing.T) {
	t.Helper()
	setOperatorNamespaceFile(t, filepath.Join(t.TempDir(), "namespace"))
}

func setOperatorNamespaceFile(t *testing.T, namespaceFile string) {
	t.Helper()
	originalNamespaceFile := operatorNamespaceFile
	operatorNamespaceFile = namespaceFile
	t.Cleanup(func() {
		operatorNamespaceFile = originalNamespaceFile
	})
}
