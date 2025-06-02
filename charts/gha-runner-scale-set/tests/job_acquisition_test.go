package tests

import (
	"path/filepath"
	"strings"
	"testing"

	v1alpha1 "github.com/actions/actions-runner-controller/apis/actions.github.com/v1alpha1"
	"github.com/gruntwork-io/terratest/modules/helm"
	"github.com/gruntwork-io/terratest/modules/k8s"
	"github.com/gruntwork-io/terratest/modules/logger"
	"github.com/gruntwork-io/terratest/modules/random"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTemplateRenderedAutoScalingRunnerSet_JobAcquisitionSettings(t *testing.T) {
	t.Parallel()

	// Path to the helm chart we will test
	helmChartPath, err := filepath.Abs("../../gha-runner-scale-set")
	require.NoError(t, err)

	releaseName := "test-runners"
	namespaceName := "test-" + strings.ToLower(random.UniqueId())

	options := &helm.Options{
		Logger: logger.Discard,
		SetValues: map[string]string{
			"githubConfigUrl":                    "https://github.com/actions",
			"githubConfigSecret.github_token":    "gh_token12345",
			"controllerServiceAccount.name":      "arc",
			"controllerServiceAccount.namespace": "arc-system",
			"maxJobsPercentage":                  "17",
			"maxJobsPerAcquisition":              "50",
		},
		KubectlOptions: k8s.NewKubectlOptions("", "", namespaceName),
	}

	output := helm.RenderTemplate(t, options, helmChartPath, releaseName, []string{"templates/autoscalingrunnerset.yaml"})

	var ars v1alpha1.AutoscalingRunnerSet
	helm.UnmarshalK8SYaml(t, output, &ars)

	assert.Equal(t, namespaceName, ars.Namespace)
	assert.Equal(t, releaseName, ars.Name)
	assert.Equal(t, 17, ars.Spec.MaxJobsPercentage, "MaxJobsPercentage should be 17")
	assert.Equal(t, 50, ars.Spec.MaxJobsPerAcquisition, "MaxJobsPerAcquisition should be 50")
}

func TestTemplateRenderedAutoScalingRunnerSet_JobAcquisitionDefaults(t *testing.T) {
	t.Parallel()

	// Path to the helm chart we will test
	helmChartPath, err := filepath.Abs("../../gha-runner-scale-set")
	require.NoError(t, err)

	releaseName := "test-runners"
	namespaceName := "test-" + strings.ToLower(random.UniqueId())

	options := &helm.Options{
		Logger: logger.Discard,
		SetValues: map[string]string{
			"githubConfigUrl":                    "https://github.com/actions",
			"githubConfigSecret.github_token":    "gh_token12345",
			"controllerServiceAccount.name":      "arc",
			"controllerServiceAccount.namespace": "arc-system",
		},
		KubectlOptions: k8s.NewKubectlOptions("", "", namespaceName),
	}

	output := helm.RenderTemplate(t, options, helmChartPath, releaseName, []string{"templates/autoscalingrunnerset.yaml"})

	var ars v1alpha1.AutoscalingRunnerSet
	helm.UnmarshalK8SYaml(t, output, &ars)

	// With template defaults, these fields are now always rendered with their default values
	// (100 and -1) even when not specified in values.yaml
	assert.Equal(t, 100, ars.Spec.MaxJobsPercentage, "MaxJobsPercentage should be 100 (template default)")
	assert.Equal(t, -1, ars.Spec.MaxJobsPerAcquisition, "MaxJobsPerAcquisition should be -1 (template default)")
}

func TestTemplateRenderedAutoScalingRunnerSet_MaxJobsPercentageValidationError(t *testing.T) {
	t.Parallel()

	// Path to the helm chart we will test
	helmChartPath, err := filepath.Abs("../../gha-runner-scale-set")
	require.NoError(t, err)

	releaseName := "test-runners"
	namespaceName := "test-" + strings.ToLower(random.UniqueId())

	testCases := []struct {
		name       string
		percentage string
		errorMsg   string
	}{
		{
			name:       "Negative percentage",
			percentage: "-1",
			errorMsg:   "maxJobsPercentage must be between 0 and 100",
		},
		{
			name:       "Over 100 percentage",
			percentage: "101",
			errorMsg:   "maxJobsPercentage must be between 0 and 100",
		},
		{
			name:       "Large invalid percentage",
			percentage: "500",
			errorMsg:   "maxJobsPercentage must be between 0 and 100",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			options := &helm.Options{
				Logger: logger.Discard,
				SetValues: map[string]string{
					"githubConfigUrl":                    "https://github.com/actions",
					"githubConfigSecret.github_token":    "gh_token12345",
					"controllerServiceAccount.name":      "arc",
					"controllerServiceAccount.namespace": "arc-system",
					"maxJobsPercentage":                  tc.percentage,
				},
				KubectlOptions: k8s.NewKubectlOptions("", "", namespaceName),
			}

			_, err := helm.RenderTemplateE(t, options, helmChartPath, releaseName, []string{"templates/autoscalingrunnerset.yaml"})
			require.Error(t, err)
			assert.ErrorContains(t, err, tc.errorMsg)
		})
	}
}

func TestTemplateRenderedAutoScalingRunnerSet_MaxJobsPerAcquisitionValidationError(t *testing.T) {
	t.Parallel()

	// Path to the helm chart we will test
	helmChartPath, err := filepath.Abs("../../gha-runner-scale-set")
	require.NoError(t, err)

	releaseName := "test-runners"
	namespaceName := "test-" + strings.ToLower(random.UniqueId())

	options := &helm.Options{
		Logger: logger.Discard,
		SetValues: map[string]string{
			"githubConfigUrl":                    "https://github.com/actions",
			"githubConfigSecret.github_token":    "gh_token12345",
			"controllerServiceAccount.name":      "arc",
			"controllerServiceAccount.namespace": "arc-system",
			"maxJobsPerAcquisition":              "-10",
		},
		KubectlOptions: k8s.NewKubectlOptions("", "", namespaceName),
	}

	_, err = helm.RenderTemplateE(t, options, helmChartPath, releaseName, []string{"templates/autoscalingrunnerset.yaml"})
	require.Error(t, err)
	assert.ErrorContains(t, err, "maxJobsPerAcquisition has to be greater or equal to -1")
}

func TestTemplateRenderedAutoScalingRunnerSet_JobAcquisitionEdgeCases(t *testing.T) {
	t.Parallel()

	// Path to the helm chart we will test
	helmChartPath, err := filepath.Abs("../../gha-runner-scale-set")
	require.NoError(t, err)

	releaseName := "test-runners"
	namespaceName := "test-" + strings.ToLower(random.UniqueId())

	testCases := []struct {
		name                  string
		maxJobsPercentage     string
		maxJobsPerAcquisition string
		expectedPercentage    int
		expectedAcquisition   int
	}{
		{
			name:                "Zero percentage (valid)",
			maxJobsPercentage:   "0",
			expectedPercentage:  0,
			expectedAcquisition: -1, // Default value when not set
		},
		{
			name:                  "Zero acquisition (valid)",
			maxJobsPerAcquisition: "0",
			expectedPercentage:    100, // Default value when not set
			expectedAcquisition:   0,
		},
		{
			name:                "Maximum percentage",
			maxJobsPercentage:   "100",
			expectedPercentage:  100,
			expectedAcquisition: -1, // Default value when not set
		},
		{
			name:                  "Both set to valid values",
			maxJobsPercentage:     "25",
			maxJobsPerAcquisition: "200",
			expectedPercentage:    25,
			expectedAcquisition:   200,
		},
		{
			name:                  "No limit acquisition (-1)",
			maxJobsPerAcquisition: "-1",
			expectedPercentage:    100, // Default value when not set
			expectedAcquisition:   -1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			setValues := map[string]string{
				"githubConfigUrl":                    "https://github.com/actions",
				"githubConfigSecret.github_token":    "gh_token12345",
				"controllerServiceAccount.name":      "arc",
				"controllerServiceAccount.namespace": "arc-system",
			}

			if tc.maxJobsPercentage != "" {
				setValues["maxJobsPercentage"] = tc.maxJobsPercentage
			}
			if tc.maxJobsPerAcquisition != "" {
				setValues["maxJobsPerAcquisition"] = tc.maxJobsPerAcquisition
			}

			options := &helm.Options{
				Logger:         logger.Discard,
				SetValues:      setValues,
				KubectlOptions: k8s.NewKubectlOptions("", "", namespaceName),
			}

			output := helm.RenderTemplate(t, options, helmChartPath, releaseName, []string{"templates/autoscalingrunnerset.yaml"})

			var ars v1alpha1.AutoscalingRunnerSet
			helm.UnmarshalK8SYaml(t, output, &ars)

			assert.Equal(t, tc.expectedPercentage, ars.Spec.MaxJobsPercentage, "MaxJobsPercentage should match expected value")
			assert.Equal(t, tc.expectedAcquisition, ars.Spec.MaxJobsPerAcquisition, "MaxJobsPerAcquisition should match expected value")
		})
	}
}

func TestTemplateRenderedAutoScalingRunnerSet_RealWorldJobAcquisitionScenarios(t *testing.T) {
	t.Parallel()

	// Path to the helm chart we will test
	helmChartPath, err := filepath.Abs("../../gha-runner-scale-set")
	require.NoError(t, err)

	releaseName := "test-runners"
	namespaceName := "test-" + strings.ToLower(random.UniqueId())

	testCases := []struct {
		name                string
		percentage          string
		acquisition         string
		description         string
		expectedPercentage  int
		expectedAcquisition int
	}{
		{
			name:                "Six cluster deployment - 1/6th each",
			percentage:          "17",
			description:         "Each of 6 clusters gets ~17% (1/6) of available jobs",
			expectedPercentage:  17,
			expectedAcquisition: -1, // Default when not set
		},
		{
			name:                "Conservative limit - 20% with 100 job cap",
			percentage:          "20",
			acquisition:         "100",
			description:         "Take 20% of jobs but never more than 100 at once",
			expectedPercentage:  20,
			expectedAcquisition: 100,
		},
		{
			name:                "Large deployment - 5% with high cap",
			percentage:          "5",
			acquisition:         "500",
			description:         "Large deployment with many clusters, each takes 5%",
			expectedPercentage:  5,
			expectedAcquisition: 500,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			setValues := map[string]string{
				"githubConfigUrl":                    "https://github.com/actions",
				"githubConfigSecret.github_token":    "gh_token12345",
				"controllerServiceAccount.name":      "arc",
				"controllerServiceAccount.namespace": "arc-system",
				"maxJobsPercentage":                  tc.percentage,
			}

			if tc.acquisition != "" {
				setValues["maxJobsPerAcquisition"] = tc.acquisition
			}

			options := &helm.Options{
				Logger:         logger.Discard,
				SetValues:      setValues,
				KubectlOptions: k8s.NewKubectlOptions("", "", namespaceName),
			}

			output := helm.RenderTemplate(t, options, helmChartPath, releaseName, []string{"templates/autoscalingrunnerset.yaml"})

			var ars v1alpha1.AutoscalingRunnerSet
			helm.UnmarshalK8SYaml(t, output, &ars)

			assert.Equal(t, tc.expectedPercentage, ars.Spec.MaxJobsPercentage, tc.description)
			assert.Equal(t, tc.expectedAcquisition, ars.Spec.MaxJobsPerAcquisition, tc.description)
		})
	}
}

// Helper function to create int pointers for test comparisons
func intPtr(i int) *int {
	return &i
}
