package config

import (
	"os"
	"path/filepath"
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

	defaultDBPath := filepath.Join(userConfigDir, "beneficiary-app", "beneficiary.db")

	cfg := Config{
		AppID:       getEnvOrDefault("BENEFICIARY_APP_ID", defaultAppID),
		WindowTitle: getEnvOrDefault("BENEFICIARY_APP_TITLE", defaultWindowTitle),
		DBPath:      getEnvOrDefault("BENEFICIARY_APP_DB_PATH", defaultDBPath),
		PSGCCSVPath: getEnvOrDefault(psgcCSVEnvKey, defaultPSGCCSVFileName),
	}

	return cfg, nil
}

func getEnvOrDefault(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
