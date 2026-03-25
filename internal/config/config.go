package config

import (
	"os"
	"path/filepath"
	"strings"
)

const (
	defaultAppID           = "ph.lgu.beneficiary"
	defaultWindowTitle     = "Offline Beneficiary Tool (Wave 1 Scaffold)"
	defaultPSGCCSVFileName = "lib_geo_map_2025_202603251312.csv"
	psgcCSVEnvKey          = "BENEFICIARY_APP_PSGC_CSV"
)

// Config contains process-level runtime settings.
type Config struct {
	AppID       string
	WindowTitle string
	DBPath      string
	PSGCCSVPath string
}

// Load reads configuration from environment with safe defaults.
func Load() (Config, error) {
	userConfigDir, err := os.UserConfigDir()
	if err != nil {
		return Config{}, err
	}

	currentDir, err := os.Getwd()
	if err != nil {
		currentDir = ""
	}

	executablePath, err := os.Executable()
	if err != nil {
		executablePath = ""
	}
	executableDir := ""
	if executablePath != "" {
		executableDir = filepath.Dir(executablePath)
	}

	defaultDBPath := filepath.Join(userConfigDir, "beneficiary-app", "beneficiary.db")
	psgcCSVPath := resolvePSGCCSVPath(getEnvOrDefault(psgcCSVEnvKey, defaultPSGCCSVFileName), currentDir, executableDir)

	cfg := Config{
		AppID:       getEnvOrDefault("BENEFICIARY_APP_ID", defaultAppID),
		WindowTitle: getEnvOrDefault("BENEFICIARY_APP_TITLE", defaultWindowTitle),
		DBPath:      getEnvOrDefault("BENEFICIARY_APP_DB_PATH", defaultDBPath),
		PSGCCSVPath: psgcCSVPath,
	}

	return cfg, nil
}

func resolvePSGCCSVPath(configuredPath, workingDir, executableDir string) string {
	configuredPath = strings.TrimSpace(configuredPath)
	if configuredPath == "" {
		configuredPath = defaultPSGCCSVFileName
	}

	if filepath.IsAbs(configuredPath) {
		return configuredPath
	}

	candidates := make([]string, 0, 2)
	if workingDir != "" {
		candidates = append(candidates, filepath.Join(workingDir, configuredPath))
	}
	if executableDir != "" && executableDir != workingDir {
		candidates = append(candidates, filepath.Join(executableDir, configuredPath))
	}
	for _, candidate := range candidates {
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
	}

	if workingDir != "" {
		return filepath.Join(workingDir, configuredPath)
	}
	if executableDir != "" {
		return filepath.Join(executableDir, configuredPath)
	}
	return configuredPath
}

func getEnvOrDefault(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
