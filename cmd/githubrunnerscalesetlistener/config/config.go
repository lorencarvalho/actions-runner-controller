package config

import (
	"encoding/json"
	"fmt"
	"os"
)

type Config struct {
	ConfigureUrl                string `json:"configureUrl"`
	AppID                       int64  `json:"appID"`
	AppInstallationID           int64  `json:"appInstallationID"`
	AppPrivateKey               string `json:"appPrivateKey"`
	Token                       string `json:"token"`
	EphemeralRunnerSetNamespace string `json:"ephemeralRunnerSetNamespace"`
	EphemeralRunnerSetName      string `json:"ephemeralRunnerSetName"`
	MaxRunners                  int    `json:"maxRunners"`
	MinRunners                  int    `json:"minRunners"`
	MaxJobsPercentage           int    `json:"maxJobsPercentage"`
	MaxJobsPerAcquisition       int    `json:"maxJobsPerAcquisition"`
	RunnerScaleSetId            int    `json:"runnerScaleSetId"`
	RunnerScaleSetName          string `json:"runnerScaleSetName"`
	ServerRootCA                string `json:"serverRootCA"`
	LogLevel                    string `json:"logLevel"`
	LogFormat                   string `json:"logFormat"`
	MetricsAddr                 string `json:"metricsAddr"`
	MetricsEndpoint             string `json:"metricsEndpoint"`
}

func Read(path string) (Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return Config{}, err
	}
	defer f.Close()

	var config Config
	if err := json.NewDecoder(f).Decode(&config); err != nil {
		return Config{}, fmt.Errorf("failed to decode config: %w", err)
	}

	if err := config.Validate(); err != nil {
		return Config{}, fmt.Errorf("failed to validate config: %w", err)
	}

	return config, nil
}

func (c *Config) Validate() error {
	if len(c.ConfigureUrl) == 0 {
		return fmt.Errorf("GitHubConfigUrl is not provided")
	}

	if len(c.EphemeralRunnerSetNamespace) == 0 || len(c.EphemeralRunnerSetName) == 0 {
		return fmt.Errorf("EphemeralRunnerSetNamespace '%s' or EphemeralRunnerSetName '%s' is missing", c.EphemeralRunnerSetNamespace, c.EphemeralRunnerSetName)
	}

	if c.RunnerScaleSetId == 0 {
		return fmt.Errorf("RunnerScaleSetId '%d' is missing", c.RunnerScaleSetId)
	}

	if c.MaxRunners < c.MinRunners {
		return fmt.Errorf("MinRunners '%d' cannot be greater than MaxRunners '%d'", c.MinRunners, c.MaxRunners)
	}

	if c.MaxJobsPercentage < 0 || c.MaxJobsPercentage > 100 {
		return fmt.Errorf("MaxJobsPercentage must be between 0 and 100")
	}

	if c.MaxJobsPerAcquisition < -1 {
		return fmt.Errorf("MaxJobsPerAcquisition must be greater than or equal to -1 (-1 means no limit)")
	}

	hasToken := len(c.Token) > 0
	hasPrivateKeyConfig := c.AppID > 0 && c.AppPrivateKey != ""

	if !hasToken && !hasPrivateKeyConfig {
		return fmt.Errorf("GitHub auth credential is missing, token length: '%d', appId: '%d', installationId: '%d', private key length: '%d", len(c.Token), c.AppID, c.AppInstallationID, len(c.AppPrivateKey))
	}

	if hasToken && hasPrivateKeyConfig {
		return fmt.Errorf("only one GitHub auth method supported at a time. Have both PAT and App auth: token length: '%d', appId: '%d', installationId: '%d', private key length: '%d", len(c.Token), c.AppID, c.AppInstallationID, len(c.AppPrivateKey))
	}

	return nil
}
