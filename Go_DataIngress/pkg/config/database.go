// pkg/config/database.go
package config

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/snowflakedb/gosnowflake"
)

// SnowflakeConfig holds Snowflake connection parameters
type SnowflakeConfig struct {
	User          string
	Password      string
	Account       string
	Warehouse     string
	Database      string // Default: NORSE_STAGING
	Role          string
	Authenticator gosnowflake.AuthType // Changed from string to proper type
	Schemas       []string             // Default: ["WA211", "WITHINREAC", "WHATCOMCOU"]

	// Connection pool settings
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration

	// Query timeout
	QueryTimeout time.Duration
}

// PostgresConfig holds PostgreSQL connection parameters
type PostgresConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
	SSLMode  string

	// Connection pool settings
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration

	// Statement timeout
	StatementTimeout time.Duration
}

// LoadSnowflakeConfig loads Snowflake configuration from environment variables
func LoadSnowflakeConfig() (*SnowflakeConfig, error) {
	user := os.Getenv("SNOWFLAKE_USER")
	if user == "" {
		return nil, errors.New("SNOWFLAKE_USER environment variable is required")
	}

	password := os.Getenv("SNOWFLAKE_PASSWORD")
	if password == "" {
		return nil, errors.New("SNOWFLAKE_PASSWORD environment variable is required")
	}

	account := os.Getenv("SNOWFLAKE_ACCOUNT")
	if account == "" {
		return nil, errors.New("SNOWFLAKE_ACCOUNT environment variable is required")
	}

	warehouse := os.Getenv("SNOWFLAKE_WAREHOUSE")
	if warehouse == "" {
		return nil, errors.New("SNOWFLAKE_WAREHOUSE environment variable is required")
	}

	// Convert authenticator string to proper type
	authString := getEnv("SNOWFLAKE_AUTHENTICATOR", "snowflake")
	var authenticator gosnowflake.AuthType
	switch authString {
	case "snowflake":
		authenticator = gosnowflake.AuthTypeSnowflake
	case "oauth":
		authenticator = gosnowflake.AuthTypeOAuth
	case "externalbrowser":
		authenticator = gosnowflake.AuthTypeExternalBrowser
	case "username_password_mfa":
		authenticator = gosnowflake.AuthTypeUsernamePasswordMFA
	case "jwt":
		authenticator = gosnowflake.AuthTypeJwt
	case "token":
		authenticator = gosnowflake.AuthTypeTokenAccessor
	case "okta":
		authenticator = gosnowflake.AuthTypeOkta
	default:
		authenticator = gosnowflake.AuthTypeSnowflake
	}

	cfg := &SnowflakeConfig{
		User:          user,
		Password:      password,
		Account:       account,
		Warehouse:     warehouse,
		Database:      getEnv("SNOWFLAKE_DATABASE", "NORSE_STAGING"),
		Role:          getEnv("SNOWFLAKE_ROLE", ""),
		Authenticator: authenticator,
		Schemas:       getEnvAsStringSlice("SNOWFLAKE_SCHEMAS", []string{"WA211", "WITHINREAC", "WHATCOMCOU"}),

		MaxOpenConns:    getEnvAsInt("SNOWFLAKE_MAX_OPEN_CONNS", 10),
		MaxIdleConns:    getEnvAsInt("SNOWFLAKE_MAX_IDLE_CONNS", 5),
		ConnMaxLifetime: time.Duration(getEnvAsInt("SNOWFLAKE_CONN_MAX_LIFETIME_SECONDS", 600)) * time.Second,
		ConnMaxIdleTime: time.Duration(getEnvAsInt("SNOWFLAKE_CONN_MAX_IDLE_TIME_SECONDS", 300)) * time.Second,
		QueryTimeout:    time.Duration(getEnvAsInt("SNOWFLAKE_QUERY_TIMEOUT_SECONDS", 300)) * time.Second,
	}

	return cfg, nil
}

// LoadPostgresConfig loads PostgreSQL configuration from environment variables
func LoadPostgresConfig() (*PostgresConfig, error) {
	user := os.Getenv("POSTGRES_USER")
	if user == "" {
		return nil, errors.New("POSTGRES_USER environment variable is required")
	}

	password := os.Getenv("POSTGRES_PASSWORD")
	if password == "" {
		return nil, errors.New("POSTGRES_PASSWORD environment variable is required")
	}

	database := os.Getenv("POSTGRES_DB")
	if database == "" {
		return nil, errors.New("POSTGRES_DB environment variable is required")
	}

	host := getEnv("POSTGRES_HOST", "localhost")
	port := getEnvAsInt("POSTGRES_PORT", getEnvAsInt("TUNNEL_PORT", 5432))

	cfg := &PostgresConfig{
		Host:     host,
		Port:     port,
		User:     user,
		Password: password,
		Database: database,
		SSLMode:  getEnv("POSTGRES_SSLMODE", "disable"),

		MaxOpenConns:     getEnvAsInt("POSTGRES_MAX_OPEN_CONNS", 25),
		MaxIdleConns:     getEnvAsInt("POSTGRES_MAX_IDLE_CONNS", 10),
		ConnMaxLifetime:  time.Duration(getEnvAsInt("POSTGRES_CONN_MAX_LIFETIME_SECONDS", 1800)) * time.Second,
		ConnMaxIdleTime:  time.Duration(getEnvAsInt("POSTGRES_CONN_MAX_IDLE_TIME_SECONDS", 600)) * time.Second,
		StatementTimeout: time.Duration(getEnvAsInt("POSTGRES_STATEMENT_TIMEOUT_SECONDS", 300)) * time.Second,
	}

	return cfg, nil
}

// ConnectionString returns a formatted Snowflake DSN
func (c *SnowflakeConfig) ConnectionString() string {
	dsn := fmt.Sprintf("%s:%s@%s/%s?warehouse=%s&authenticator=%s",
		c.User,
		c.Password,
		c.Account,
		c.Database,
		c.Warehouse,
		c.Authenticator,
	)

	if c.Role != "" {
		dsn += "&role=" + c.Role
	}

	return dsn
}

// ConnectionString returns a formatted PostgreSQL connection string
func (c *PostgresConfig) ConnectionString() string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		c.Host,
		c.Port,
		c.User,
		c.Password,
		c.Database,
		c.SSLMode,
	)
}

// Helper function to parse string slice from environment
func getEnvAsStringSlice(key string, defaultValue []string) []string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}

	// Simple comma-separated parsing
	var result []string
	for _, v := range splitCommaDelimited(value) {
		if v != "" {
			result = append(result, v)
		}
	}

	if len(result) == 0 {
		return defaultValue
	}

	return result
}

// Split comma-delimited string and trim whitespace
func splitCommaDelimited(s string) []string {
	result := make([]string, 0)
	current := ""
	inQuotes := false

	for _, char := range s {
		switch char {
		case '"':
			inQuotes = !inQuotes
		case ',':
			if !inQuotes {
				result = append(result, current)
				current = ""
			} else {
				current += string(char)
			}
		default:
			current += string(char)
		}
	}

	if current != "" {
		result = append(result, current)
	}

	// Trim whitespace
	for i, v := range result {
		result[i] = trimSpace(v)
	}

	return result
}

// Simple whitespace trimming
func trimSpace(s string) string {
	// Remove leading/trailing whitespace and quotes
	result := ""
	for _, c := range s {
		if c != ' ' && c != '\t' && c != '\n' && c != '\r' && c != '"' {
			result += string(c)
		}
	}
	return result
}
