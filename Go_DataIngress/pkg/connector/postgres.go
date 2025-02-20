// pkg/connector/postgres.go
package connector

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v4/stdlib"
	"go.uber.org/zap"

	"github.com/David-Botos/data-ingress/pkg/config"
)

// PostgresConnector implements the DatabaseConnector interface for PostgreSQL
type PostgresConnector struct {
	db     *sql.DB
	logger *zap.Logger
	cfg    *config.PostgresConfig
}

// NewPostgresConnector creates and initializes a new PostgreSQL connector
func NewPostgresConnector(ctx context.Context, cfg *config.PostgresConfig) (*PostgresConnector, error) {
	logger := zap.L().Named("postgres-connector")

	// Log connection attempt
	logger.Info("Connecting to PostgreSQL",
		zap.String("host", cfg.Host),
		zap.Int("port", cfg.Port),
		zap.String("database", cfg.Database),
		zap.String("user", cfg.User))

	// Open database connection
	connStr := cfg.ConnectionString()
	db, err := sql.Open("pgx", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize PostgreSQL connection: %w", err)
	}

	// Configure connection pool
	ApplyConnectionSettings(
		db,
		cfg.MaxOpenConns,
		cfg.MaxIdleConns,
		cfg.ConnMaxLifetime,
		cfg.ConnMaxIdleTime,
	)

	// Set statement timeout if configured
	if cfg.StatementTimeout > 0 {
		_, err = db.ExecContext(
			ctx,
			fmt.Sprintf("SET statement_timeout = %d", cfg.StatementTimeout.Milliseconds()),
		)
		if err != nil {
			logger.Warn("Failed to set statement timeout", zap.Error(err))
		}
	}

	// Verify connection
	if err := PingWithTimeout(ctx, db, 5*time.Second); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}

	connector := &PostgresConnector{
		db:     db,
		logger: logger,
		cfg:    cfg,
	}

	LogConnectionStats(logger, cfg.Database, db)
	return connector, nil
}

// DB returns the underlying database connection
func (c *PostgresConnector) DB() *sql.DB {
	return c.db
}

// Validate verifies the PostgreSQL connection and required permissions
func (c *PostgresConnector) Validate() error {
	// Check database version
	var version string
	err := c.db.QueryRow("SELECT version()").Scan(&version)
	if err != nil {
		return fmt.Errorf("failed to query PostgreSQL version: %w", err)
	}
	c.logger.Info("Connected to PostgreSQL", zap.String("version", version))

	// Check permissions by creating a temp table
	_, err = c.db.Exec(`
		DO $$
		BEGIN
			CREATE TEMP TABLE _permission_check (id serial, test text);
			INSERT INTO _permission_check (test) VALUES ('test');
			DROP TABLE _permission_check;
		EXCEPTION WHEN OTHERS THEN
			RAISE EXCEPTION 'Permission check failed: %', SQLERRM;
		END $$;
	`)
	if err != nil {
		return fmt.Errorf("permission validation failed: %w", err)
	}

	// Ensure required schemas exist
	requiredSchemas := []string{"wa211", "withinreac", "whatcomcou", "public"}
	for _, schema := range requiredSchemas {
		if err := c.ensureSchema(schema); err != nil {
			return fmt.Errorf("failed to create/verify schema %s: %w", schema, err)
		}
	}

	c.logger.Info("PostgreSQL connection validated",
		zap.String("database", c.cfg.Database),
		zap.String("host", c.cfg.Host),
		zap.Int("port", c.cfg.Port))

	return nil
}

// Close closes the database connection
func (c *PostgresConnector) Close() error {
	c.logger.Info("Closing PostgreSQL connection")
	LogConnectionStats(c.logger, c.cfg.Database, c.db)
	return c.db.Close()
}

// ensureSchema creates a schema if it doesn't exist
func (c *PostgresConnector) ensureSchema(schema string) error {
	_, err := c.db.Exec(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", schema))
	if err != nil {
		return err
	}
	return nil
}

// ExecWithTimeout executes a query with a timeout
func (c *PostgresConnector) ExecWithTimeout(
	ctx context.Context,
	query string,
	timeout time.Duration,
	args ...interface{},
) (sql.Result, error) {
	queryCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return c.db.ExecContext(queryCtx, query, args...)
}

// QueryWithTimeout executes a query with a timeout
func (c *PostgresConnector) QueryWithTimeout(
	ctx context.Context,
	query string,
	timeout time.Duration,
	args ...interface{},
) (*sql.Rows, error) {
	queryCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return c.db.QueryContext(queryCtx, query, args...)
}

// BatchInsert performs a bulk insert into a table
func (c *PostgresConnector) BatchInsert(
	ctx context.Context,
	schema string,
	table string,
	columns []string,
	valueRows [][]interface{},
	batchSize int,
) (int64, error) {
	if len(valueRows) == 0 {
		return 0, nil
	}

	if batchSize <= 0 {
		batchSize = 1000
	}

	// Build the base query
	fullTableName := fmt.Sprintf("%s.%s", schema, table)
	columnStr := strings.Join(columns, ", ")

	var totalRowsInserted int64

	// Process in batches
	for i := 0; i < len(valueRows); i += batchSize {
		end := i + batchSize
		if end > len(valueRows) {
			end = len(valueRows)
		}

		currentBatch := valueRows[i:end]

		// Build placeholders for this batch
		placeholders := make([]string, len(currentBatch))
		args := make([]interface{}, 0, len(currentBatch)*len(columns))

		for j, row := range currentBatch {
			rowPlaceholders := make([]string, len(columns))
			for k, val := range row {
				paramIndex := j*len(columns) + k + 1
				rowPlaceholders[k] = fmt.Sprintf("$%d", paramIndex)
				args = append(args, val)
			}
			placeholders[j] = fmt.Sprintf("(%s)", strings.Join(rowPlaceholders, ", "))
		}

		// Construct the query
		query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
			fullTableName, columnStr, strings.Join(placeholders, ", "))

		// Execute with timeout
		result, err := c.ExecWithTimeout(ctx, query, 30*time.Second, args...)
		if err != nil {
			return totalRowsInserted, fmt.Errorf("batch insert failed: %w", err)
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			c.logger.Warn("Couldn't get rows affected", zap.Error(err))
		} else {
			totalRowsInserted += rowsAffected
		}
	}

	return totalRowsInserted, nil
}

// CreateTableIfNotExists creates a table with the specified schema if it doesn't exist
func (c *PostgresConnector) CreateTableIfNotExists(
	ctx context.Context,
	schema string,
	table string,
	columnDefs []string,
	primaryKey string,
) error {
	// Format fully qualified table name
	fullTableName := fmt.Sprintf("%s.%s", schema, table)

	// Check if table exists
	var exists bool
	query := `
		SELECT EXISTS (
			SELECT FROM information_schema.tables 
			WHERE table_schema = $1 AND table_name = $2
		)
	`

	err := c.db.QueryRowContext(ctx, query, schema, table).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check if table exists: %w", err)
	}

	if exists {
		c.logger.Debug("Table already exists", zap.String("table", fullTableName))
		return nil
	}

	// Build CREATE TABLE statement
	createSQL := fmt.Sprintf(
		"CREATE TABLE %s (\n\t%s",
		fullTableName,
		strings.Join(columnDefs, ",\n\t"),
	)

	// Add primary key if specified
	if primaryKey != "" {
		createSQL += fmt.Sprintf(",\n\tPRIMARY KEY (%s)", primaryKey)
	}
	createSQL += "\n)"

	// Execute CREATE TABLE
	_, err = c.ExecWithTimeout(ctx, createSQL, 30*time.Second)
	if err != nil {
		return fmt.Errorf("failed to create table %s: %w", fullTableName, err)
	}

	c.logger.Info("Created table", zap.String("table", fullTableName))
	return nil
}
