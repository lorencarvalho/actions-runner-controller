package config

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfigValidationMinMax(t *testing.T) {
	config := &Config{
		ConfigureUrl:                "github.com/some_org/some_repo",
		EphemeralRunnerSetNamespace: "namespace",
		EphemeralRunnerSetName:      "deployment",
		RunnerScaleSetId:            1,
		MinRunners:                  5,
		MaxRunners:                  2,
		Token:                       "token",
	}
	err := config.Validate()
	assert.ErrorContains(t, err, "MinRunners '5' cannot be greater than MaxRunners '2", "Expected error about MinRunners > MaxRunners")
}

func TestConfigValidationMissingToken(t *testing.T) {
	config := &Config{
		ConfigureUrl:                "github.com/some_org/some_repo",
		EphemeralRunnerSetNamespace: "namespace",
		EphemeralRunnerSetName:      "deployment",
		RunnerScaleSetId:            1,
	}
	err := config.Validate()
	expectedError := fmt.Sprintf("GitHub auth credential is missing, token length: '%d', appId: '%d', installationId: '%d', private key length: '%d", len(config.Token), config.AppID, config.AppInstallationID, len(config.AppPrivateKey))
	assert.ErrorContains(t, err, expectedError, "Expected error about missing auth")
}

func TestConfigValidationAppKey(t *testing.T) {
	config := &Config{
		AppID:                       1,
		AppInstallationID:           10,
		ConfigureUrl:                "github.com/some_org/some_repo",
		EphemeralRunnerSetNamespace: "namespace",
		EphemeralRunnerSetName:      "deployment",
		RunnerScaleSetId:            1,
	}
	err := config.Validate()
	expectedError := fmt.Sprintf("GitHub auth credential is missing, token length: '%d', appId: '%d', installationId: '%d', private key length: '%d", len(config.Token), config.AppID, config.AppInstallationID, len(config.AppPrivateKey))
	assert.ErrorContains(t, err, expectedError, "Expected error about missing auth")
}

func TestConfigValidationOnlyOneTypeOfCredentials(t *testing.T) {
	config := &Config{
		AppID:                       1,
		AppInstallationID:           10,
		AppPrivateKey:               "asdf",
		Token:                       "asdf",
		ConfigureUrl:                "github.com/some_org/some_repo",
		EphemeralRunnerSetNamespace: "namespace",
		EphemeralRunnerSetName:      "deployment",
		RunnerScaleSetId:            1,
	}
	err := config.Validate()
	expectedError := fmt.Sprintf("only one GitHub auth method supported at a time. Have both PAT and App auth: token length: '%d', appId: '%d', installationId: '%d', private key length: '%d", len(config.Token), config.AppID, config.AppInstallationID, len(config.AppPrivateKey))
	assert.ErrorContains(t, err, expectedError, "Expected error about missing auth")
}

func TestConfigValidation(t *testing.T) {
	t.Parallel()

	t.Run("Valid config with job acquisition settings", func(t *testing.T) {
		t.Parallel()

		config := Config{
			ConfigureUrl:                "https://github.com/test/repo",
			EphemeralRunnerSetNamespace: "test-namespace",
			EphemeralRunnerSetName:      "test-runner-set",
			RunnerScaleSetId:            123,
			MaxRunners:                  10,
			MinRunners:                  2,
			MaxJobsPercentage:           50,
			MaxJobsPerAcquisition:       100,
		}

		err := config.Validate()
		assert.NoError(t, err, "Valid config should not produce validation error")
	})

	t.Run("MaxJobsPercentage validation", func(t *testing.T) {
		t.Parallel()

		baseConfig := Config{
			ConfigureUrl:                "https://github.com/test/repo",
			EphemeralRunnerSetNamespace: "test-namespace",
			EphemeralRunnerSetName:      "test-runner-set",
			RunnerScaleSetId:            123,
			MaxRunners:                  10,
			MinRunners:                  2,
			MaxJobsPerAcquisition:       50,
		}

		testCases := []struct {
			name          string
			percentage    int
			shouldError   bool
			errorContains string
		}{
			{"Valid 0%", 0, false, ""},
			{"Valid 1%", 1, false, ""},
			{"Valid 50%", 50, false, ""},
			{"Valid 100%", 100, false, ""},
			{"Invalid negative", -1, true, "MaxJobsPercentage must be between 0 and 100"},
			{"Invalid over 100", 101, true, "MaxJobsPercentage must be between 0 and 100"},
			{"Invalid large number", 500, true, "MaxJobsPercentage must be between 0 and 100"},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				config := baseConfig
				config.MaxJobsPercentage = tc.percentage

				err := config.Validate()
				if tc.shouldError {
					assert.Error(t, err, "Should produce validation error for percentage %d", tc.percentage)
					assert.Contains(t, err.Error(), tc.errorContains, "Error message should contain expected text")
				} else {
					assert.NoError(t, err, "Should not produce validation error for percentage %d", tc.percentage)
				}
			})
		}
	})

	t.Run("MaxJobsPerAcquisition validation", func(t *testing.T) {
		t.Parallel()

		baseConfig := Config{
			ConfigureUrl:                "https://github.com/test/repo",
			EphemeralRunnerSetNamespace: "test-namespace",
			EphemeralRunnerSetName:      "test-runner-set",
			RunnerScaleSetId:            123,
			MaxRunners:                  10,
			MinRunners:                  2,
			MaxJobsPercentage:           50,
		}

		testCases := []struct {
			name          string
			acquisition   int
			shouldError   bool
			errorContains string
		}{
			{"Valid 0 (no limit)", 0, false, ""},
			{"Valid 1", 1, false, ""},
			{"Valid 50", 50, false, ""},
			{"Valid 1000", 1000, false, ""},
			{"Invalid negative", -1, true, "MaxJobsPerAcquisition must be greater than or equal to 0"},
			{"Invalid -100", -100, true, "MaxJobsPerAcquisition must be greater than or equal to 0"},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				config := baseConfig
				config.MaxJobsPerAcquisition = tc.acquisition

				err := config.Validate()
				if tc.shouldError {
					assert.Error(t, err, "Should produce validation error for acquisition limit %d", tc.acquisition)
					assert.Contains(t, err.Error(), tc.errorContains, "Error message should contain expected text")
				} else {
					assert.NoError(t, err, "Should not produce validation error for acquisition limit %d", tc.acquisition)
				}
			})
		}
	})

	t.Run("Combined job acquisition settings", func(t *testing.T) {
		t.Parallel()

		baseConfig := Config{
			ConfigureUrl:                "https://github.com/test/repo",
			EphemeralRunnerSetNamespace: "test-namespace",
			EphemeralRunnerSetName:      "test-runner-set",
			RunnerScaleSetId:            123,
			MaxRunners:                  10,
			MinRunners:                  2,
		}

		testCases := []struct {
			name        string
			percentage  int
			acquisition int
			shouldError bool
		}{
			{"Both valid", 50, 100, false},
			{"Percentage valid, acquisition 0", 25, 0, false},
			{"Percentage 0, acquisition valid", 0, 50, false},
			{"Both at minimum valid", 0, 0, false},
			{"Percentage invalid, acquisition valid", 150, 50, true},
			{"Percentage valid, acquisition invalid", 50, -10, true},
			{"Both invalid", -10, -20, true},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				config := baseConfig
				config.MaxJobsPercentage = tc.percentage
				config.MaxJobsPerAcquisition = tc.acquisition

				err := config.Validate()
				if tc.shouldError {
					assert.Error(t, err, "Should produce validation error for combination %d%%, %d limit", tc.percentage, tc.acquisition)
				} else {
					assert.NoError(t, err, "Should not produce validation error for combination %d%%, %d limit", tc.percentage, tc.acquisition)
				}
			})
		}
	})

	t.Run("Other validation still works", func(t *testing.T) {
		t.Parallel()

		// Test that existing validation logic still works
		config := Config{
			// Missing required fields should still cause validation errors
			MaxJobsPercentage:     50,
			MaxJobsPerAcquisition: 100,
		}

		err := config.Validate()
		assert.Error(t, err, "Should still validate other required fields")
		assert.Contains(t, err.Error(), "GitHubConfigUrl", "Should validate ConfigureUrl")
	})
}

func TestConfigValidationConfigUrl(t *testing.T) {
	config := &Config{
		EphemeralRunnerSetNamespace: "namespace",
		EphemeralRunnerSetName:      "deployment",
		RunnerScaleSetId:            1,
	}

	err := config.Validate()

	assert.ErrorContains(t, err, "GitHubConfigUrl is not provided", "Expected error about missing ConfigureUrl")
}
