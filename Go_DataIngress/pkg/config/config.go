// pkg/config/config.go
package config

import (
	"errors"
	"os"
	"strconv"
	"time"
)

// Config represents the application configuration
type Config struct {
	// Database connections
	Snowflake *SnowflakeConfig
	Postgres  *PostgresConfig

	// Transfer settings
	ChunkSize      int
	RetryAttempts  int
	RetryDelay     time.Duration
	WorkerPoolSize int

	// Logging
	LogLevel  string
	LogFormat string
}

// LoadConfig loads configuration from environment variables
func LoadConfig() (*Config, error) {
	cfg := &Config{
		// Default values
		ChunkSize:      getEnvAsInt("CHUNK_SIZE", 5000),
		RetryAttempts:  getEnvAsInt("RETRY_ATTEMPTS", 3),
		RetryDelay:     time.Duration(getEnvAsInt("RETRY_DELAY_MS", 1000)) * time.Millisecond,
		WorkerPoolSize: getEnvAsInt("WORKER_POOL_SIZE", 0), // 0 means use runtime.NumCPU()
		LogLevel:       getEnv("LOG_LEVEL", "info"),
		LogFormat:      getEnv("LOG_FORMAT", "json"),
	}

	// Load database configurations
	snowConfig, err := LoadSnowflakeConfig()
	if err != nil {
		return nil, errors.New("failed to load Snowflake configuration: " + err.Error())
	}
	cfg.Snowflake = snowConfig

	pgConfig, err := LoadPostgresConfig()
	if err != nil {
		return nil, errors.New("failed to load PostgreSQL configuration: " + err.Error())
	}
	cfg.Postgres = pgConfig

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

// Validate ensures all required configuration is present and valid
func (c *Config) Validate() error {
	if c.Snowflake == nil {
		return errors.New("snowflake configuration is required")
	}

	if c.Postgres == nil {
		return errors.New("postgreSQL configuration is required")
	}

	if c.ChunkSize <= 0 {
		return errors.New("chunk size must be positive")
	}

	if c.RetryAttempts < 0 {
		return errors.New("retry attempts cannot be negative")
	}

	return nil
}

// Helper functions for environment variables
func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

func getEnvAsInt(key string, defaultValue int) int {
	valueStr := getEnv(key, "")
	if valueStr == "" {
		return defaultValue
	}

	value, err := strconv.Atoi(valueStr)
	if err != nil {
		return defaultValue
	}
	return value
}
