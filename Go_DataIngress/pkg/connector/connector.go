// pkg/connector/connector.go
package connector

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"go.uber.org/zap"
)

// DatabaseConnector defines the interface for database connectors
type DatabaseConnector interface {
	// DB returns the underlying database connection
	DB() *sql.DB

	// Validate verifies the connection and permissions
	Validate() error

	// Close closes the connection and releases resources
	Close() error

	// QueryWithTimeout executes a query with a timeout
	QueryWithTimeout(ctx context.Context, query string, timeout time.Duration, args ...interface{}) (*sql.Rows, error)

	// ExecWithTimeout executes a statement with a timeout
	ExecWithTimeout(ctx context.Context, query string, timeout time.Duration, args ...interface{}) (sql.Result, error)
}

// ConnStats contains standardized connection statistics
type ConnStats struct {
	OpenConnections int
	InUse           int
	Idle            int
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration
}

// GetConnectionStats returns connection pool statistics for logging
func GetConnectionStats(db *sql.DB) ConnStats {
	stats := db.Stats()
	return ConnStats{
		OpenConnections: stats.OpenConnections,
		InUse:           stats.InUse,
		Idle:            stats.Idle,
		MaxOpenConns:    db.Stats().MaxOpenConnections,
	}
}

// LogConnectionStats logs connection pool statistics
func LogConnectionStats(logger *zap.Logger, name string, db *sql.DB) {
	stats := GetConnectionStats(db)
	logger.Debug("Connection pool stats",
		zap.String("database", name),
		zap.Int("open_connections", stats.OpenConnections),
		zap.Int("in_use", stats.InUse),
		zap.Int("idle", stats.Idle),
		zap.Int("max_open", stats.MaxOpenConns),
		zap.Int("max_idle", stats.MaxIdleConns),
		zap.Duration("max_lifetime", stats.ConnMaxLifetime),
		zap.Duration("max_idle_time", stats.ConnMaxIdleTime),
	)
}

// PingWithTimeout attempts to ping a database with a timeout
func PingWithTimeout(ctx context.Context, db *sql.DB, timeout time.Duration) error {
	pingCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- db.PingContext(pingCtx)
	}()

	select {
	case err := <-errCh:
		return err
	case <-pingCtx.Done():
		return fmt.Errorf("ping timed out after %v: %w", timeout, pingCtx.Err())
	}
}

// ApplyConnectionSettings configures database connection pool settings
func ApplyConnectionSettings(db *sql.DB, maxOpen, maxIdle int, maxLifetime, maxIdleTime time.Duration) {
	if maxOpen > 0 {
		db.SetMaxOpenConns(maxOpen)
	}
	if maxIdle > 0 {
		db.SetMaxIdleConns(maxIdle)
	}
	if maxLifetime > 0 {
		db.SetConnMaxLifetime(maxLifetime)
	}
	if maxIdleTime > 0 {
		db.SetConnMaxIdleTime(maxIdleTime)
	}
}
