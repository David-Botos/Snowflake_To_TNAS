// pkg/connector/factory.go
package connector

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/David-Botos/data-ingress/pkg/config"
)

// ConnectorFactory creates database connectors
type ConnectorFactory struct {
	cfg    *config.Config
	logger *zap.Logger
}

// NewConnectorFactory creates a new connector factory
func NewConnectorFactory(cfg *config.Config, logger *zap.Logger) *ConnectorFactory {
	return &ConnectorFactory{
		cfg:    cfg,
		logger: logger,
	}
}

// CreateSnowflakeConnector creates a new Snowflake connector
func (f *ConnectorFactory) CreateSnowflakeConnector(ctx context.Context) (*SnowflakeConnector, error) {
	f.logger.Info("Creating Snowflake connector")

	connector, err := NewSnowflakeConnector(ctx, f.cfg.Snowflake)
	if err != nil {
		return nil, fmt.Errorf("failed to create Snowflake connector: %w", err)
	}

	return connector, nil
}

// CreatePostgresConnector creates a new PostgreSQL connector
func (f *ConnectorFactory) CreatePostgresConnector(ctx context.Context) (*PostgresConnector, error) {
	f.logger.Info("Creating PostgreSQL connector")

	connector, err := NewPostgresConnector(ctx, f.cfg.Postgres)
	if err != nil {
		return nil, fmt.Errorf("failed to create PostgreSQL connector: %w", err)
	}

	return connector, nil
}

// CreateAllConnectors creates both Snowflake and PostgreSQL connectors
func (f *ConnectorFactory) CreateAllConnectors(ctx context.Context) (*SnowflakeConnector, *PostgresConnector, error) {
	snowConn, err := f.CreateSnowflakeConnector(ctx)
	if err != nil {
		return nil, nil, err
	}

	pgConn, err := f.CreatePostgresConnector(ctx)
	if err != nil {
		snowConn.Close() // Clean up the Snowflake connection if PostgreSQL fails
		return nil, nil, err
	}

	return snowConn, pgConn, nil
}