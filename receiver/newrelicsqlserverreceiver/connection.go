// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package newrelicsqlserverreceiver

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
	_ "github.com/microsoft/go-mssqldb"
	"github.com/microsoft/go-mssqldb/azuread"
	"github.com/newrelic/go-agent/v3/newrelic"
	"go.uber.org/zap"
)

// SQLConnection represents a wrapper around a SQL Server connection
type SQLConnection struct {
	Connection *sqlx.DB
	Host       string
	Config     *Config
	logger     *zap.Logger
}

// AuthConnector interface for different authentication methods
type AuthConnector interface {
	Connect(cfg *Config, dbName string) (*sqlx.DB, error)
}

// SQLAuthConnector handles standard SQL Server authentication
type SQLAuthConnector struct{}

func (s SQLAuthConnector) Connect(cfg *Config, dbName string) (*sqlx.DB, error) {
	connectionURL := cfg.CreateConnectionURL(dbName)
	return sqlx.Connect("mssql", connectionURL)
}

// AzureADAuthConnector handles Azure AD Service Principal authentication
type AzureADAuthConnector struct{}

func (a AzureADAuthConnector) Connect(cfg *Config, dbName string) (*sqlx.DB, error) {
	connectionURL := cfg.CreateAzureADConnectionURL(dbName)
	return sqlx.Connect(azuread.DriverName, connectionURL)
}

// NewConnection creates a new SQL Server connection with proper authentication
func NewConnection(cfg *Config) (*sql.DB, error) {
	ctx := context.Background()
	conn, err := NewSQLConnection(ctx, cfg, nil)
	if err != nil {
		return nil, err
	}
	return conn.Connection.DB, nil
}

// NewSQLConnection creates a new SQL Server connection with proper authentication
func NewSQLConnection(ctx context.Context, cfg *Config, logger *zap.Logger) (*SQLConnection, error) {
	return createConnectionWithAuth(ctx, cfg, "", logger)
}

// NewDatabaseConnection creates a connection to a specific database
func NewDatabaseConnection(ctx context.Context, cfg *Config, dbName string, logger *zap.Logger) (*SQLConnection, error) {
	return createConnectionWithAuth(ctx, cfg, dbName, logger)
}

// createConnectionWithAuth creates a connection using the appropriate authentication method
func createConnectionWithAuth(ctx context.Context, cfg *Config, dbName string, logger *zap.Logger) (*SQLConnection, error) {
	connector, err := determineAuthMethod(cfg, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to determine authentication method: %w", err)
	}

	db, err := connector.Connect(cfg, dbName)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to SQL Server: %w", err)
	}

	// Test the connection
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping SQL Server: %w", err)
	}

	return &SQLConnection{
		Connection: db,
		Host:       cfg.Hostname,
		Config:     cfg,
		logger:     logger,
	}, nil
}

// determineAuthMethod determines which authentication method to use based on configuration
func determineAuthMethod(cfg *Config, logger *zap.Logger) (AuthConnector, error) {
	if cfg == nil {
		return nil, fmt.Errorf("configuration cannot be nil")
	}

	switch {
	case cfg.IsAzureADAuth():
		logger.Debug("Detected Azure AD Service Principal authentication - using ClientID, TenantID, and ClientSecret")
		return AzureADAuthConnector{}, nil
	default:
		logger.Debug("Using SQL Server authentication")
		return SQLAuthConnector{}, nil
	}
}

// Close closes the SQL connection
func (sc *SQLConnection) Close() {
	if err := sc.Connection.Close(); err != nil {
		sc.logger.Warn("Unable to close SQL Connection", zap.Error(err))
	}
}

// Query runs a query and loads results into v
func (sc *SQLConnection) Query(ctx context.Context, v interface{}, query string) error {
	sc.logger.Debug("Running query", zap.String("query", query))

	// Extract query name (from comment if present, otherwise table name)
	queryName := extractQueryName(query)

	// Create DatastoreSegment for New Relic Databases section
	txn := newrelic.FromContext(ctx)
	var segment *newrelic.DatastoreSegment
	if txn != nil {
		segment = &newrelic.DatastoreSegment{
			StartTime:          txn.StartSegmentNow(),
			Product:            newrelic.DatastoreMySQL, // Use MySQL as closest match for MSSQL
			Collection:         queryName,                // Table/query name
			Operation:          "SELECT",                 // Default operation
			ParameterizedQuery: query,
			Host:               sc.Host,
			PortPathOrID:       sc.Config.Port,
			DatabaseName:       "",
		}
	}

	// Execute query
	err := sc.Connection.SelectContext(ctx, v, query)

	// End DatastoreSegment
	if segment != nil {
		segment.End()
	}

	return err
}

// QueryRow runs a query that returns a single row
func (sc *SQLConnection) QueryRow(ctx context.Context, query string) *sql.Row {
	sc.logger.Debug("Running single row query", zap.String("query", query))

	// Extract query name (from comment if present, otherwise table name)
	queryName := extractQueryName(query)

	// Create DatastoreSegment for New Relic Databases section
	txn := newrelic.FromContext(ctx)
	var segment *newrelic.DatastoreSegment
	if txn != nil {
		segment = &newrelic.DatastoreSegment{
			StartTime:          txn.StartSegmentNow(),
			Product:            newrelic.DatastoreMySQL,
			Collection:         queryName,
			Operation:          "SELECT",
			ParameterizedQuery: query,
			Host:               sc.Host,
			PortPathOrID:       sc.Config.Port,
			DatabaseName:       "",
		}
	}

	// Execute query
	row := sc.Connection.QueryRowContext(ctx, query)

	// End DatastoreSegment
	if segment != nil {
		segment.End()
	}

	return row
}

// Queryx runs a query and returns a set of rows
func (sc *SQLConnection) Queryx(ctx context.Context, query string) (*sqlx.Rows, error) {
	sc.logger.Debug("Running queryx", zap.String("query", query))

	// Extract query name (from comment if present, otherwise table name)
	queryName := extractQueryName(query)

	// Create DatastoreSegment for New Relic Databases section
	txn := newrelic.FromContext(ctx)
	var segment *newrelic.DatastoreSegment
	if txn != nil {
		segment = &newrelic.DatastoreSegment{
			StartTime:          txn.StartSegmentNow(),
			Product:            newrelic.DatastoreMySQL,
			Collection:         queryName,
			Operation:          "SELECT",
			ParameterizedQuery: query,
			Host:               sc.Host,
			PortPathOrID:       sc.Config.Port,
			DatabaseName:       "",
		}
	}

	// Execute query
	rows, err := sc.Connection.QueryxContext(ctx, query)

	// End DatastoreSegment
	if segment != nil {
		segment.End()
	}

	return rows, err
}

// Ping tests the connection to the database
func (sc *SQLConnection) Ping(ctx context.Context) error {
	return sc.Connection.PingContext(ctx)
}

// Stats returns database connection statistics
func (sc *SQLConnection) Stats() sql.DBStats {
	return sc.Connection.Stats()
}

// extractQueryName extracts query name from SQL comment or falls back to table name
// Looks for /* QueryName */ at the start of the query
func extractQueryName(query string) string {
	query = strings.TrimSpace(query)

	// First, try to extract from comment: /* QueryName */
	if strings.HasPrefix(query, "/*") {
		endIdx := strings.Index(query, "*/")
		if endIdx > 2 {
			// Extract name from comment, trim whitespace
			name := strings.TrimSpace(query[2:endIdx])
			if name != "" {
				return name
			}
		}
	}

	// Fallback: extract table name from query
	return extractTableNameFromQuery(query)
}

// extractTableNameFromQuery attempts to extract the primary table name from a SQL query
// This is the fallback when no comment is present
func extractTableNameFromQuery(query string) string {
	query = strings.TrimSpace(query)
	queryUpper := strings.ToUpper(query)

	// Handle SELECT queries
	if strings.HasPrefix(queryUpper, "SELECT") {
		// Look for FROM clause
		if idx := strings.Index(queryUpper, " FROM "); idx != -1 {
			afterFrom := strings.TrimSpace(query[idx+6:])

			// Get first word after FROM (table/view name)
			parts := strings.FieldsFunc(afterFrom, func(r rune) bool {
				return r == ' ' || r == ',' || r == '(' || r == '\n' || r == '\t'
			})

			if len(parts) > 0 {
				tableName := strings.Trim(parts[0], "[]")

				// Keep schema.table format (e.g., sys.databases, sys.dm_os_performance_counters)
				// This helps identify system DMVs vs user tables
				return tableName
			}
		}
	}

	// For INSERT/UPDATE/DELETE
	if strings.HasPrefix(queryUpper, "INSERT INTO") {
		afterInto := strings.TrimSpace(query[11:])
		parts := strings.Fields(afterInto)
		if len(parts) > 0 {
			return strings.Trim(parts[0], "[]")
		}
	}

	// Fallback: return generic identifier
	return "sql_query"
}
