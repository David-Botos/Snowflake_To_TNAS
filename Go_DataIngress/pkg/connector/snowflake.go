// pkg/connector/snowflake.go
package connector

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	sf "github.com/snowflakedb/gosnowflake"
	"go.uber.org/zap"

	"github.com/David-Botos/data-ingress/pkg/config"
)

// SnowflakeConnector implements the DatabaseConnector interface for Snowflake
type SnowflakeConnector struct {
	db     *sql.DB
	logger *zap.Logger
	cfg    *config.SnowflakeConfig
}

// NewSnowflakeConnector creates a new Snowflake connection
func NewSnowflakeConnector(ctx context.Context, cfg *config.SnowflakeConfig) (*SnowflakeConnector, error) {
	logger := zap.L().Named("snowflake-connector")

	// Create DSN using Snowflake's DSN builder
	sfConfig := &sf.Config{
		Account:       cfg.Account,
		User:          cfg.User,
		Password:      cfg.Password,
		Database:      cfg.Database,
		Warehouse:     cfg.Warehouse,
		Role:          cfg.Role,
		Authenticator: cfg.Authenticator,
	}

	// Log connection attempt (without credentials)
	logger.Info("Connecting to Snowflake",
		zap.String("account", cfg.Account),
		zap.String("user", cfg.User),
		zap.String("database", cfg.Database),
		zap.String("warehouse", cfg.Warehouse),
		zap.String("role", cfg.Role))

	dsn, err := sf.DSN(sfConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to build Snowflake DSN: %w", err)
	}

	// Open connection pool
	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize Snowflake connection: %w", err)
	}

	// Configure connection pool
	ApplyConnectionSettings(
		db,
		cfg.MaxOpenConns,
		cfg.MaxIdleConns,
		cfg.ConnMaxLifetime,
		cfg.ConnMaxIdleTime,
	)

	// Set query timeout if configured
	if cfg.QueryTimeout > 0 {
		_, err = db.ExecContext(
			ctx,
			fmt.Sprintf("ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = %d",
				int(cfg.QueryTimeout.Seconds())),
		)
		if err != nil {
			logger.Warn("Failed to set statement timeout", zap.Error(err))
		}
	}

	// Verify connection
	if err := PingWithTimeout(ctx, db, 10*time.Second); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to connect to Snowflake: %w", err)
	}

	connector := &SnowflakeConnector{
		db:     db,
		logger: logger,
		cfg:    cfg,
	}

	LogConnectionStats(logger, cfg.Database, db)
	return connector, nil
}

// DB returns the underlying database connection
func (c *SnowflakeConnector) DB() *sql.DB {
	return c.db
}

// Validate verifies the Snowflake connection and access rights
func (c *SnowflakeConnector) Validate() error {
	// Check basic connectivity and permissions
	var role, database, warehouse string
	err := c.db.QueryRow("SELECT CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_WAREHOUSE()").Scan(
		&role, &database, &warehouse)
	if err != nil {
		return fmt.Errorf("failed to verify Snowflake access: %w", err)
	}

	c.logger.Info("Connected to Snowflake",
		zap.String("role", role),
		zap.String("database", database),
		zap.String("warehouse", warehouse))

	// Verify we're connected to the correct database
	if database != c.cfg.Database {
		return fmt.Errorf("connected to wrong database: %s (expected: %s)",
			database, c.cfg.Database)
	}

	// Verify schemas exist
	missingSchemas, err := c.verifySchemas()
	if err != nil {
		return fmt.Errorf("failed to verify schemas: %w", err)
	}

	if len(missingSchemas) > 0 {
		c.logger.Warn("Some required schemas not found",
			zap.Strings("missing_schemas", missingSchemas))
	}

	return nil
}

// Close closes the database connection
func (c *SnowflakeConnector) Close() error {
	c.logger.Info("Closing Snowflake connection")
	LogConnectionStats(c.logger, c.cfg.Database, c.db)
	return c.db.Close()
}

// verifySchemas checks if all required schemas exist in the database
func (c *SnowflakeConnector) verifySchemas() ([]string, error) {
	rows, err := c.db.Query("SHOW SCHEMAS IN DATABASE " + c.cfg.Database)
	if err != nil {
		return nil, fmt.Errorf("failed to query schemas: %w", err)
	}
	defer rows.Close()

	// Schema names are in the second column in SHOW SCHEMAS output
	schemas := make(map[string]bool)
	var dummy, schemaName string
	for rows.Next() {
		// Skip unused columns in SHOW SCHEMAS output (8 columns total)
		if err := rows.Scan(&dummy, &schemaName, &dummy, &dummy, &dummy, &dummy, &dummy, &dummy); err != nil {
			return nil, fmt.Errorf("failed to scan schema row: %w", err)
		}
		schemas[strings.ToUpper(schemaName)] = true
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating schemas: %w", err)
	}

	// Check if required schemas exist
	var missingSchemas []string
	for _, schema := range c.cfg.Schemas {
		upperSchema := strings.ToUpper(schema)
		if !schemas[upperSchema] {
			missingSchemas = append(missingSchemas, upperSchema)
		}
	}

	return missingSchemas, nil
}

// GetTables retrieves all tables in a schema
func (c *SnowflakeConnector) GetTables(schema string) ([]string, error) {
	rows, err := c.db.Query("SHOW TABLES IN SCHEMA " + schema)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve tables from schema %s: %w", schema, err)
	}
	defer rows.Close()

	// Table names are in the second column in SHOW TABLES output
	var tables []string
	var dummy, tableName string
	for rows.Next() {
		// Skip unused columns in SHOW TABLES output (varies by Snowflake version)
		if err := rows.Scan(&dummy, &tableName, &dummy, &dummy, &dummy, &dummy, &dummy, &dummy, &dummy); err != nil {
			return nil, fmt.Errorf("failed to scan table row: %w", err)
		}
		tables = append(tables, tableName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating tables: %w", err)
	}

	return tables, nil
}

// QueryWithTimeout executes a query with a timeout
func (c *SnowflakeConnector) QueryWithTimeout(
	ctx context.Context,
	query string,
	timeout time.Duration,
	args ...interface{},
) (*sql.Rows, error) {
	queryCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return c.db.QueryContext(queryCtx, query, args...)
}

// ExecWithTimeout executes a statement with a timeout
func (c *SnowflakeConnector) ExecWithTimeout(
	ctx context.Context,
	query string,
	timeout time.Duration,
	args ...interface{},
) (sql.Result, error) {
	queryCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return c.db.ExecContext(queryCtx, query, args...)
}

// BatchQuery fetches data in batches to handle large result sets
func (c *SnowflakeConnector) BatchQuery(
	ctx context.Context,
	query string,
	batchSize int,
	processor func(*sql.Rows) error,
) error {
	// Set default batch size if not provided
	if batchSize <= 0 {
		batchSize = 10000
	}

	// Execute query with LIMIT and OFFSET to fetch data in batches
	offset := 0
	for {
		batchQuery := fmt.Sprintf("%s LIMIT %d OFFSET %d", query, batchSize, offset)
		rows, err := c.QueryWithTimeout(ctx, batchQuery, 30*time.Second)
		if err != nil {
			return fmt.Errorf("batch query failed at offset %d: %w", offset, err)
		}

		// Process rows in this batch
		rowCount := 0
		for rows.Next() {
			rowCount++
			if err := processor(rows); err != nil {
				rows.Close()
				return fmt.Errorf("row processing failed at offset %d: %w", offset, err)
			}
		}

		rows.Close()
		if err := rows.Err(); err != nil {
			return fmt.Errorf("error iterating rows at offset %d: %w", offset, err)
		}

		// If fewer rows than batch size were returned, we're done
		if rowCount < batchSize {
			break
		}

		// Move to next batch
		offset += batchSize
	}

	return nil
}
