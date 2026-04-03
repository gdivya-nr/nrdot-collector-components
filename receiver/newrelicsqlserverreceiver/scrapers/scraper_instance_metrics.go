// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package scrapers

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/models"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/queries"
)

// InstanceConfig holds configuration for instance metrics
// SQLConnectionInterface defines the interface for database connections
type SQLConnectionInterface interface {
	Query(ctx context.Context, dest interface{}, query string) error
}

// ScraperConfig interface defines config methods needed by scrapers
type ScraperConfig interface {
	GetEnableInstanceMetrics() bool
	GetEnableWaitTimeMetrics() bool
	GetEnableDatabaseMetrics() bool
}


// InstanceScraper handles SQL Server instance-level metrics collection
type InstanceScraper struct {
	connection    SQLConnectionInterface
	logger        *zap.Logger
	mb            *metadata.MetricsBuilder
	engineEdition int
	config        ScraperConfig
}

// NewInstanceScraper creates a new instance scraper
func NewInstanceScraper(conn SQLConnectionInterface, logger *zap.Logger, mb *metadata.MetricsBuilder, engineEdition int, config ScraperConfig) *InstanceScraper {
	
	return &InstanceScraper{
		connection:    conn,
		logger:        logger,
		mb:            mb,
		engineEdition: engineEdition,
		config:        config,
	}
}

// getQueryForMetric retrieves the appropriate query for a metric based on engine edition with Default fallback
func (s *InstanceScraper) getQueryForMetric(metricName string) (string, bool) {
	query, found := queries.GetQueryForMetric(queries.InstanceQueries, metricName, s.engineEdition)
	if found {
		s.logger.Debug("Using query for metric",
			zap.String("metric_name", metricName),
			zap.String("engine_type", queries.GetEngineTypeName(s.engineEdition)))
	}
	return query, found
}

func (s *InstanceScraper) ScrapeInstanceMemoryMetrics(ctx context.Context) error {
	s.logger.Debug("Scraping SQL Server instance memory metrics")

	// Check if this metric is compatible with the current engine edition
	if !queries.IsMetricCompatible(queries.InstanceQueries, "sqlserver.instance.memory_metrics", s.engineEdition) {
		s.logger.Debug("Instance memory metrics not supported for this engine edition",
			zap.String("engine_type", queries.GetEngineTypeName(s.engineEdition)),
			zap.Int("engine_edition", s.engineEdition))
		return nil // Return nil to indicate successful skip, not an error
	}

	// Get the appropriate query for this engine edition
	query, found := s.getQueryForMetric("sqlserver.instance.memory_metrics")
	if !found {
		return fmt.Errorf("no memory query available for engine edition %d", s.engineEdition)
	}

	s.logger.Debug("Executing memory query",
		zap.String("query", queries.TruncateQuery(query, 100)),
		zap.String("engine_type", queries.GetEngineTypeName(s.engineEdition)))

	var results []models.InstanceMemoryDefinitionsModel
	if err := s.connection.Query(ctx, &results, query); err != nil {
		s.logger.Error("Failed to execute instance memory query",
			zap.Error(err),
			zap.String("query", queries.TruncateQuery(query, 100)),
			zap.Int("engine_edition", s.engineEdition))
		return fmt.Errorf("failed to execute instance memory query: %w", err)
	}

	if len(results) == 0 {
		s.logger.Warn("No results returned from instance memory query - SQL Server may not be ready")
		return fmt.Errorf("no results returned from instance memory query")
	}

	if len(results) > 1 {
		s.logger.Warn("Multiple results returned from instance memory query",
			zap.Int("result_count", len(results)))
	}

	result := results[0]
	if result.TotalPhysicalMemory == nil && result.AvailablePhysicalMemory == nil && result.MemoryUtilization == nil {
		s.logger.Error("All memory metrics are null - invalid query result")
		return fmt.Errorf("all memory metrics are null in query result")
	}

	if err := s.processInstanceMemoryMetrics(result); err != nil {
		s.logger.Error("Failed to process instance memory metrics", zap.Error(err))
		return fmt.Errorf("failed to process instance memory metrics: %w", err)
	}

	s.logger.Debug("Successfully scraped instance memory metrics")
	return nil
}

// ScrapeInstanceComprehensiveStats scrapes comprehensive instance statistics
func (s *InstanceScraper) ScrapeInstanceComprehensiveStats(ctx context.Context) error {
	s.logger.Debug("Scraping SQL Server comprehensive instance statistics")

	// Check if this metric is compatible with the current engine edition
	if !queries.IsMetricCompatible(queries.InstanceQueries, "sqlserver.instance.comprehensive_stats", s.engineEdition) {
		s.logger.Debug("Instance comprehensive stats not supported for this engine edition",
			zap.String("engine_type", queries.GetEngineTypeName(s.engineEdition)),
			zap.Int("engine_edition", s.engineEdition))
		return nil // Return nil to indicate successful skip, not an error
	}

	// Get the appropriate query for this engine edition
	query, found := s.getQueryForMetric("sqlserver.instance.comprehensive_stats")
	if !found {
		return fmt.Errorf("no comprehensive stats query available for engine edition %d", s.engineEdition)
	}

	s.logger.Debug("Executing comprehensive stats query",
		zap.String("query", queries.TruncateQuery(query, 100)),
		zap.String("engine_type", queries.GetEngineTypeName(s.engineEdition)))

	var results []models.InstanceStatsModel
	if err := s.connection.Query(ctx, &results, query); err != nil {
		s.logger.Error("Failed to execute comprehensive stats query",
			zap.Error(err),
			zap.String("query", queries.TruncateQuery(query, 100)),
			zap.Int("engine_edition", s.engineEdition))
		return fmt.Errorf("failed to execute comprehensive stats query: %w", err)
	}

	if len(results) == 0 {
		s.logger.Warn("No results returned from comprehensive stats query - SQL Server may not be ready")
		return fmt.Errorf("no results returned from comprehensive stats query")
	}

	if len(results) > 1 {
		s.logger.Warn("Multiple results returned from comprehensive stats query",
			zap.Int("result_count", len(results)))
	}

	result := results[0]
	if err := s.processInstanceStatsMetrics(result); err != nil {
		s.logger.Error("Failed to process comprehensive stats metrics", zap.Error(err))
		return fmt.Errorf("failed to process comprehensive stats metrics: %w", err)
	}

	s.logger.Debug("Successfully scraped comprehensive instance statistics")
	return nil
}

// processInstanceMemoryMetrics processes memory metrics and creates OpenTelemetry metrics
func (s *InstanceScraper) processInstanceMemoryMetrics(result models.InstanceMemoryDefinitionsModel) error {
	now := pcommon.NewTimestampFromTime(time.Now())

	// ===== CRITICAL METRICS - ALWAYS EMIT =====
	if result.AvailablePhysicalMemory != nil {
		s.mb.RecordSqlserverInstanceMemoryAvailableDataPoint(now, *result.AvailablePhysicalMemory) // MANDATORY: Dashboard metric
	}
	if result.MemoryUtilization != nil {
		s.mb.RecordSqlserverInstanceMemoryUtilizationPercentDataPoint(now, *result.MemoryUtilization)
	}

	// ===== NON-CRITICAL METRICS - ONLY EMIT IF ENABLED =====
	if s.config.GetEnableInstanceMetrics() {
		if result.TotalPhysicalMemory != nil {
			s.mb.RecordSqlserverInstanceMemoryTotalDataPoint(now, *result.TotalPhysicalMemory)
		}
	}

	return nil
}

// ScrapeInstanceStats scrapes comprehensive SQL Server instance statistics
func (s *InstanceScraper) ScrapeInstanceStats(ctx context.Context) error {
	// Check if this metric is compatible with the current engine edition
	if !queries.IsMetricCompatible(queries.InstanceQueries, "sqlserver.instance.comprehensive_stats", s.engineEdition) {
		s.logger.Debug("Instance comprehensive stats not supported for this engine edition",
			zap.String("engine_type", queries.GetEngineTypeName(s.engineEdition)),
			zap.Int("engine_edition", s.engineEdition))
		return nil // Return nil to indicate successful skip, not an error
	}

	s.logger.Debug("Scraping SQL Server comprehensive instance statistics")

	// Get the appropriate query for this engine edition
	query, found := s.getQueryForMetric("sqlserver.instance.comprehensive_stats")
	if !found {
		return fmt.Errorf("no comprehensive stats query available for engine edition %d", s.engineEdition)
	}

	s.logger.Debug("Executing comprehensive stats query",
		zap.String("query", queries.TruncateQuery(query, 100)),
		zap.String("engine_type", queries.GetEngineTypeName(s.engineEdition)))

	var results []models.InstanceStatsModel
	if err := s.connection.Query(ctx, &results, query); err != nil {
		s.logger.Error("Failed to execute instance stats query",
			zap.Error(err),
			zap.String("query", queries.TruncateQuery(query, 100)),
			zap.Int("engine_edition", s.engineEdition))
		return fmt.Errorf("failed to execute instance stats query: %w", err)
	}

	if len(results) == 0 {
		s.logger.Warn("No results returned from instance stats query - SQL Server may not be ready")
		return fmt.Errorf("no results returned from instance stats query")
	}

	if len(results) > 1 {
		s.logger.Warn("Multiple results returned from instance stats query",
			zap.Int("result_count", len(results)))
	}

	result := results[0]
	if err := s.processInstanceStatsMetrics(result); err != nil {
		s.logger.Error("Failed to process instance stats metrics", zap.Error(err))
		return fmt.Errorf("failed to process instance stats metrics: %w", err)
	}

	s.logger.Debug("Successfully scraped comprehensive instance statistics")
	return nil
}

// processInstanceStatsMetrics processes comprehensive instance statistics and creates OpenTelemetry metrics
func (s *InstanceScraper) processInstanceStatsMetrics(result models.InstanceStatsModel) error {
	now := pcommon.NewTimestampFromTime(time.Now())

	// ===== CRITICAL METRICS - ALWAYS EMIT (Required for golden metrics & dashboards) =====
	if result.SQLCompilations != nil {
		s.mb.RecordSqlserverStatsSQLCompilationsPerSecDataPoint(now, int64(*result.SQLCompilations))
	}
	if result.UserConnections != nil {
		s.mb.RecordSqlserverStatsConnectionsDataPoint(now, int64(*result.UserConnections)) // MANDATORY: Golden metric
	}
	if result.LockWaitTimeMs != nil {
		s.mb.RecordSqlserverStatsLockWaitsPerSecDataPoint(now, int64(*result.LockWaitTimeMs)) // MANDATORY: Dashboard metric
	}
	if result.PageSplitsSec != nil {
		s.mb.RecordSqlserverAccessPageSplitsPerSecDataPoint(now, int64(*result.PageSplitsSec)) // MANDATORY: Dashboard metric
	}
	if result.CheckpointPagesSec != nil {
		s.mb.RecordSqlserverBufferCheckpointPagesPerSecDataPoint(now, int64(*result.CheckpointPagesSec)) // MANDATORY: Dashboard metric
	}
	if result.DeadlocksSec != nil {
		s.mb.RecordSqlserverStatsDeadlocksPerSecDataPoint(now, int64(*result.DeadlocksSec)) // MANDATORY: Dashboard metric
	}
	if result.UserErrors != nil {
		s.mb.RecordSqlserverStatsUserErrorsPerSecDataPoint(now, int64(*result.UserErrors)) // MANDATORY: Dashboard metric
	}
	if result.KillConnectionErrors != nil {
		s.mb.RecordSqlserverStatsKillConnectionErrorsPerSecDataPoint(now, int64(*result.KillConnectionErrors))
	}
	if result.PageLifeExpectancySec != nil {
		s.mb.RecordSqlserverBufferpoolPageLifeExpectancyMsDataPoint(now, *result.PageLifeExpectancySec) // MANDATORY: Dashboard metric
	}
	if result.TransactionsSec != nil {
		s.mb.RecordSqlserverInstanceTransactionsPerSecDataPoint(now, int64(*result.TransactionsSec)) // MANDATORY: Golden metric
	}
	if result.ForcedParameterizationsSec != nil {
		s.mb.RecordSqlserverInstanceForcedParameterizationsPerSecDataPoint(now, int64(*result.ForcedParameterizationsSec))
	}
	if result.SQLRecompilations != nil {
		s.mb.RecordSqlserverStatsSQLRecompilationsPerSecDataPoint(now, int64(*result.SQLRecompilations))
	}

	// ===== NON-CRITICAL METRICS - ONLY EMIT IF ENABLED =====
	if s.config.GetEnableInstanceMetrics() {
		if result.BatchRequestSec != nil {
			s.mb.RecordSqlserverBufferpoolBatchRequestsPerSecDataPoint(now, int64(*result.BatchRequestSec))
		}
	}

	return nil
}

// ScrapeInstanceBufferPoolHitPercent scrapes buffer pool hit percent metric
func (s *InstanceScraper) ScrapeInstanceBufferPoolHitPercent(ctx context.Context) error {
	// Check if this metric is compatible with the current engine edition
	if !queries.IsMetricCompatible(queries.InstanceQueries, "sqlserver.instance.buffer_pool.hitPercent", s.engineEdition) {
		s.logger.Debug("Instance buffer pool hit percent not supported for this engine edition",
			zap.String("engine_type", queries.GetEngineTypeName(s.engineEdition)),
			zap.Int("engine_edition", s.engineEdition))
		return nil // Return nil to indicate successful skip, not an error
	}

	query, found := s.getQueryForMetric("sqlserver.instance.buffer_pool.hitPercent")
	if !found {
		return fmt.Errorf("no buffer pool hit percent query available for engine edition %d", s.engineEdition)
	}
	var results []models.BufferPoolHitPercentMetricsModel
	if err := s.connection.Query(ctx, &results, query); err != nil {
		return fmt.Errorf("failed to execute buffer pool hit percent query: %w", err)
	}
	if len(results) == 0 {
		return fmt.Errorf("no results returned from buffer pool hit percent query")
	}
	return s.processInstanceBufferPoolHitPercent(results[0])
}

// processInstanceBufferPoolHitPercent processes buffer pool hit percent metric and creates OpenTelemetry metric
func (s *InstanceScraper) processInstanceBufferPoolHitPercent(result models.BufferPoolHitPercentMetricsModel) error {
	now := pcommon.NewTimestampFromTime(time.Now())

	// ===== CRITICAL METRICS - ALWAYS EMIT =====
	if result.BufferPoolHitPercent != nil {
		s.mb.RecordSqlserverInstanceBufferPoolHitPercentDataPoint(now, *result.BufferPoolHitPercent) // MANDATORY: Golden metric
	}

	return nil
}

// ScrapeInstanceProcessCounts scrapes process counts
func (s *InstanceScraper) ScrapeInstanceProcessCounts(ctx context.Context) error {
	// Check if this metric is compatible with the current engine edition
	if !queries.IsMetricCompatible(queries.InstanceQueries, "sqlserver.instance.process_counts", s.engineEdition) {
		s.logger.Debug("Instance process counts not supported for this engine edition",
			zap.String("engine_type", queries.GetEngineTypeName(s.engineEdition)),
			zap.Int("engine_edition", s.engineEdition))
		return nil // Return nil to indicate successful skip, not an error
	}

	query, found := s.getQueryForMetric("sqlserver.instance.process_counts")
	if !found {
		return fmt.Errorf("no process counts query available for engine edition %d", s.engineEdition)
	}
	var results []models.InstanceProcessCountsModel
	if err := s.connection.Query(ctx, &results, query); err != nil {
		return fmt.Errorf("failed to execute process counts query: %w", err)
	}
	if len(results) == 0 {
		return fmt.Errorf("no results returned from process counts query")
	}
	return s.processInstanceProcessCounts(results[0])
}

// processInstanceProcessCounts processes process counts and creates OpenTelemetry metrics
func (s *InstanceScraper) processInstanceProcessCounts(result models.InstanceProcessCountsModel) error {
	now := pcommon.NewTimestampFromTime(time.Now())

	// ===== CRITICAL METRICS - ALWAYS EMIT =====
	if result.Blocked != nil {
		s.mb.RecordSqlserverInstanceBlockedProcessesCountDataPoint(now, int64(*result.Blocked))
	}

	// ===== NON-CRITICAL METRICS - ONLY EMIT IF ENABLED =====
	if s.config.GetEnableInstanceMetrics() {
		if result.Preconnect != nil {
			s.mb.RecordSqlserverInstancePreconnectProcessesCountDataPoint(now, int64(*result.Preconnect))
		}
		if result.Background != nil {
			s.mb.RecordSqlserverInstanceBackgroundProcessesCountDataPoint(now, int64(*result.Background))
		}
		if result.Dormant != nil {
			s.mb.RecordSqlserverInstanceDormantProcessesCountDataPoint(now, int64(*result.Dormant))
		}
		if result.Runnable != nil {
			s.mb.RecordSqlserverInstanceRunnableProcessesCountDataPoint(now, int64(*result.Runnable))
		}
		if result.Suspended != nil {
			s.mb.RecordSqlserverInstanceSuspendedProcessesCountDataPoint(now, int64(*result.Suspended))
		}
		if result.Running != nil {
			s.mb.RecordSqlserverInstanceRunningProcessesCountDataPoint(now, int64(*result.Running))
		}
		if result.Sleeping != nil {
			s.mb.RecordSqlserverInstanceSleepingProcessesCountDataPoint(now, int64(*result.Sleeping))
		}
	}

	return nil
}

// ScrapeInstanceRunnableTasks scrapes runnable tasks
func (s *InstanceScraper) ScrapeInstanceRunnableTasks(ctx context.Context) error {
	// Check if this metric is compatible with the current engine edition
	if !queries.IsMetricCompatible(queries.InstanceQueries, "sqlserver.instance.runnable_tasks", s.engineEdition) {
		s.logger.Debug("Instance runnable tasks not supported for this engine edition",
			zap.String("engine_type", queries.GetEngineTypeName(s.engineEdition)),
			zap.Int("engine_edition", s.engineEdition))
		return nil // Return nil to indicate successful skip, not an error
	}

	query, found := s.getQueryForMetric("sqlserver.instance.runnable_tasks")
	if !found {
		return fmt.Errorf("no runnable tasks query available for engine edition %d", s.engineEdition)
	}
	var results []models.RunnableTasksMetricsModel
	if err := s.connection.Query(ctx, &results, query); err != nil {
		return fmt.Errorf("failed to execute runnable tasks query: %w", err)
	}
	if len(results) == 0 {
		return fmt.Errorf("no results returned from runnable tasks query")
	}
	return s.processInstanceRunnableTasks(results[0])
}

// processInstanceRunnableTasks processes runnable tasks and creates OpenTelemetry metrics
func (s *InstanceScraper) processInstanceRunnableTasks(result models.RunnableTasksMetricsModel) error {
	now := pcommon.NewTimestampFromTime(time.Now())

	// ===== NON-CRITICAL METRIC - ONLY EMIT IF ENABLED =====
	if s.config.GetEnableInstanceMetrics() {
		if result.RunnableTasksCount != nil {
			s.mb.RecordSqlserverInstanceRunnableTasksDataPoint(now, int64(*result.RunnableTasksCount))
		}
	}

	return nil
}

// ScrapeInstanceDiskMetrics scrapes disk metrics
func (s *InstanceScraper) ScrapeInstanceDiskMetrics(ctx context.Context) error {
	// Check if this metric is compatible with the current engine edition
	if !queries.IsMetricCompatible(queries.InstanceQueries, "sqlserver.instance.disk_metrics", s.engineEdition) {
		s.logger.Debug("Instance disk metrics not supported for this engine edition",
			zap.String("engine_type", queries.GetEngineTypeName(s.engineEdition)),
			zap.Int("engine_edition", s.engineEdition))
		return nil // Return nil to indicate successful skip, not an error
	}

	query, found := s.getQueryForMetric("sqlserver.instance.disk_metrics")
	if !found {
		return fmt.Errorf("no disk metrics query available for engine edition %d", s.engineEdition)
	}
	var results []models.InstanceDiskMetricsModel
	if err := s.connection.Query(ctx, &results, query); err != nil {
		return fmt.Errorf("failed to execute disk metrics query: %w", err)
	}
	if len(results) == 0 {
		return fmt.Errorf("no results returned from disk metrics query")
	}
	return s.processInstanceDiskMetrics(results[0])
}

// processInstanceDiskMetrics processes disk metrics and creates OpenTelemetry metrics
func (s *InstanceScraper) processInstanceDiskMetrics(result models.InstanceDiskMetricsModel) error {
	now := pcommon.NewTimestampFromTime(time.Now())

	// ===== NON-CRITICAL METRIC - ONLY EMIT IF ENABLED =====
	if s.config.GetEnableInstanceMetrics() {
		if result.TotalDiskSpace != nil {
			s.mb.RecordSqlserverInstanceDiskInBytesDataPoint(now, int64(*result.TotalDiskSpace))
		}
	}

	return nil
}

// ScrapeInstanceActiveConnections scrapes active connections
func (s *InstanceScraper) ScrapeInstanceActiveConnections(ctx context.Context) error {
	// Check if this metric is compatible with the current engine edition
	if !queries.IsMetricCompatible(queries.InstanceQueries, "sqlserver.instance.active_connections", s.engineEdition) {
		s.logger.Debug("Instance active connections not supported for this engine edition",
			zap.String("engine_type", queries.GetEngineTypeName(s.engineEdition)),
			zap.Int("engine_edition", s.engineEdition))
		return nil // Return nil to indicate successful skip, not an error
	}

	query, found := s.getQueryForMetric("sqlserver.instance.active_connections")
	if !found {
		return fmt.Errorf("no active connections query available for engine edition %d", s.engineEdition)
	}
	var results []models.InstanceActiveConnectionsMetricsModel
	if err := s.connection.Query(ctx, &results, query); err != nil {
		return fmt.Errorf("failed to execute active connections query: %w", err)
	}
	if len(results) == 0 {
		return fmt.Errorf("no results returned from active connections query")
	}
	return s.processInstanceActiveConnections(results[0])
}

// processInstanceActiveConnections processes active connections and creates OpenTelemetry metrics
func (s *InstanceScraper) processInstanceActiveConnections(result models.InstanceActiveConnectionsMetricsModel) error {
	now := pcommon.NewTimestampFromTime(time.Now())

	// ===== CRITICAL METRICS - ALWAYS EMIT =====
	if result.InstanceActiveConnections != nil {
		s.mb.RecordSqlserverInstanceConnectionsActiveDataPoint(now, int64(*result.InstanceActiveConnections)) // MANDATORY: Golden metric
	}

	return nil
}

// ScrapeInstanceBufferPoolSize scrapes buffer pool size
func (s *InstanceScraper) ScrapeInstanceBufferPoolSize(ctx context.Context) error {
	// Check if this metric is compatible with the current engine edition
	if !queries.IsMetricCompatible(queries.InstanceQueries, "sqlserver.instance.buffer_pool_size", s.engineEdition) {
		s.logger.Debug("Instance buffer pool size not supported for this engine edition",
			zap.String("engine_type", queries.GetEngineTypeName(s.engineEdition)),
			zap.Int("engine_edition", s.engineEdition))
		return nil // Return nil to indicate successful skip, not an error
	}

	query, found := s.getQueryForMetric("sqlserver.instance.buffer_pool_size")
	if !found {
		return fmt.Errorf("no buffer pool size query available for engine edition %d", s.engineEdition)
	}
	var results []models.InstanceBufferMetricsModel
	if err := s.connection.Query(ctx, &results, query); err != nil {
		return fmt.Errorf("failed to execute buffer pool size query: %w", err)
	}
	if len(results) == 0 {
		return fmt.Errorf("no results returned from buffer pool size query")
	}
	return s.processInstanceBufferPoolSize(results[0])
}

// processInstanceBufferPoolSize processes buffer pool size and creates OpenTelemetry metrics
func (s *InstanceScraper) processInstanceBufferPoolSize(result models.InstanceBufferMetricsModel) error {
	now := pcommon.NewTimestampFromTime(time.Now())

	// ===== CRITICAL METRICS - ALWAYS EMIT =====
	if result.BufferPoolSize != nil {
		s.mb.RecordSqlserverInstanceBufferPoolSizeDataPoint(now, int64(*result.BufferPoolSize)) // MANDATORY: Dashboard metric
	}

	return nil
}

// processInstanceTargetMemoryMetrics processes target server memory metrics using reflection
func (s *InstanceScraper) processInstanceTargetMemoryMetrics(result models.InstanceTargetMemoryModel) error {
	now := pcommon.NewTimestampFromTime(time.Now())

	// ===== CRITICAL METRICS - ALWAYS EMIT =====
	if result.TargetServerMemoryKB != nil {
		s.mb.RecordSqlserverInstanceTargetMemoryKbDataPoint(now, *result.TargetServerMemoryKB) // MANDATORY: Dashboard metric
	}

	return nil
}

// processInstancePerformanceRatiosMetrics processes performance ratio metrics using reflection
func (s *InstanceScraper) processInstancePerformanceRatiosMetrics(result models.InstancePerformanceRatiosModel) error {
	now := pcommon.NewTimestampFromTime(time.Now())

	// ===== CRITICAL METRICS - ALWAYS EMIT =====
	if result.CompilationsPerBatch != nil {
		s.mb.RecordSqlserverInstanceCompilationsPerBatchDataPoint(now, *result.CompilationsPerBatch) // MANDATORY: Dashboard metric
	}
	if result.PageSplitsPerBatch != nil {
		s.mb.RecordSqlserverInstancePageSplitsPerBatchDataPoint(now, *result.PageSplitsPerBatch) // MANDATORY: Dashboard metric
	}

	return nil
}

// processInstanceIndexMetrics processes index performance metrics using reflection
func (s *InstanceScraper) processInstanceIndexMetrics(result models.InstanceIndexMetricsModel) error {
	now := pcommon.NewTimestampFromTime(time.Now())

	// ===== CRITICAL METRICS - ALWAYS EMIT =====
	if result.FullScansPerSec != nil {
		s.mb.RecordSqlserverInstanceFullScansRateDataPoint(now, *result.FullScansPerSec) // MANDATORY: Dashboard metric
	}

	return nil
}

// processInstanceLockMetrics processes lock performance metrics using reflection
func (s *InstanceScraper) processInstanceLockMetrics(result models.InstanceLockMetricsModel) error {
	now := pcommon.NewTimestampFromTime(time.Now())

	// ===== CRITICAL METRICS - ALWAYS EMIT =====
	if result.LockTimeoutsPerSec != nil {
		s.mb.RecordSqlserverInstanceLockTimeoutsRateDataPoint(now, *result.LockTimeoutsPerSec) // MANDATORY: Dashboard metric
	}

	return nil
}

// New instance metric scrapers for enhanced performance monitoring

// ScrapeInstanceTargetMemoryMetrics scrapes target server memory metrics
func (s *InstanceScraper) ScrapeInstanceTargetMemoryMetrics(ctx context.Context) error {
	s.logger.Debug("Scraping SQL Server instance target memory metrics")

	// Get the appropriate query for this engine edition
	query, found := s.getQueryForMetric("sqlserver.instance.target_memory_metrics")
	if !found {
		return fmt.Errorf("no target memory query available for engine edition %d", s.engineEdition)
	}

	s.logger.Debug("Executing target memory query",
		zap.String("query", queries.TruncateQuery(query, 100)),
		zap.String("engine_type", queries.GetEngineTypeName(s.engineEdition)))

	var results []models.InstanceTargetMemoryModel
	if err := s.connection.Query(ctx, &results, query); err != nil {
		s.logger.Error("Failed to execute instance target memory query",
			zap.Error(err),
			zap.String("query", queries.TruncateQuery(query, 100)),
			zap.Int("engine_edition", s.engineEdition))
		return fmt.Errorf("failed to execute instance target memory query: %w", err)
	}

	if len(results) == 0 {
		s.logger.Warn("No results returned from instance target memory query - SQL Server may not be ready")
		return fmt.Errorf("no results returned from instance target memory query")
	}

	result := results[0]
	if result.TargetServerMemoryKB == nil {
		s.logger.Error("Target memory metric is null - invalid query result")
		return fmt.Errorf("target memory metric is null in query result")
	}

	if err := s.processInstanceTargetMemoryMetrics(result); err != nil {
		s.logger.Error("Failed to process instance target memory metrics", zap.Error(err))
		return fmt.Errorf("failed to process instance target memory metrics: %w", err)
	}

	s.logger.Debug("Successfully scraped instance target memory metrics")
	return nil
}

// ScrapeInstancePerformanceRatiosMetrics scrapes performance ratio metrics
func (s *InstanceScraper) ScrapeInstancePerformanceRatiosMetrics(ctx context.Context) error {
	s.logger.Debug("Scraping SQL Server instance performance ratio metrics")

	// Get the appropriate query for this engine edition
	query, found := s.getQueryForMetric("sqlserver.instance.performance_ratios_metrics")
	if !found {
		return fmt.Errorf("no performance ratios query available for engine edition %d", s.engineEdition)
	}

	s.logger.Debug("Executing performance ratios query",
		zap.String("query", queries.TruncateQuery(query, 100)),
		zap.String("engine_type", queries.GetEngineTypeName(s.engineEdition)))

	var results []models.InstancePerformanceRatiosModel
	if err := s.connection.Query(ctx, &results, query); err != nil {
		s.logger.Error("Failed to execute instance performance ratios query",
			zap.Error(err),
			zap.String("query", queries.TruncateQuery(query, 100)),
			zap.Int("engine_edition", s.engineEdition))
		return fmt.Errorf("failed to execute instance performance ratios query: %w", err)
	}

	if len(results) == 0 {
		s.logger.Warn("No results returned from instance performance ratios query - SQL Server may not be ready")
		return fmt.Errorf("no results returned from instance performance ratios query")
	}

	result := results[0]
	if result.CompilationsPerBatch == nil && result.PageSplitsPerBatch == nil {
		s.logger.Error("All performance ratio metrics are null - invalid query result")
		return fmt.Errorf("all performance ratio metrics are null in query result")
	}

	if err := s.processInstancePerformanceRatiosMetrics(result); err != nil {
		s.logger.Error("Failed to process instance performance ratios metrics", zap.Error(err))
		return fmt.Errorf("failed to process instance performance ratios metrics: %w", err)
	}

	s.logger.Debug("Successfully scraped instance performance ratios metrics")
	return nil
}

// ScrapeInstanceIndexMetrics scrapes index performance metrics
func (s *InstanceScraper) ScrapeInstanceIndexMetrics(ctx context.Context) error {
	s.logger.Debug("Scraping SQL Server instance index metrics")

	// Get the appropriate query for this engine edition
	query, found := s.getQueryForMetric("sqlserver.instance.index_metrics")
	if !found {
		return fmt.Errorf("no index metrics query available for engine edition %d", s.engineEdition)
	}

	s.logger.Debug("Executing index metrics query",
		zap.String("query", queries.TruncateQuery(query, 100)),
		zap.String("engine_type", queries.GetEngineTypeName(s.engineEdition)))

	var results []models.InstanceIndexMetricsModel
	if err := s.connection.Query(ctx, &results, query); err != nil {
		s.logger.Error("Failed to execute instance index metrics query",
			zap.Error(err),
			zap.String("query", queries.TruncateQuery(query, 100)),
			zap.Int("engine_edition", s.engineEdition))
		return fmt.Errorf("failed to execute instance index metrics query: %w", err)
	}

	if len(results) == 0 {
		s.logger.Warn("No results returned from instance index metrics query - SQL Server may not be ready")
		return fmt.Errorf("no results returned from instance index metrics query")
	}

	result := results[0]
	if result.FullScansPerSec == nil {
		s.logger.Error("Index metrics are null - invalid query result")
		return fmt.Errorf("index metrics are null in query result")
	}

	if err := s.processInstanceIndexMetrics(result); err != nil {
		s.logger.Error("Failed to process instance index metrics", zap.Error(err))
		return fmt.Errorf("failed to process instance index metrics: %w", err)
	}

	s.logger.Debug("Successfully scraped instance index metrics")
	return nil
}

// ScrapeInstanceLockMetrics scrapes lock performance metrics
func (s *InstanceScraper) ScrapeInstanceLockMetrics(ctx context.Context) error {
	s.logger.Debug("Scraping SQL Server instance lock metrics")

	// Get the appropriate query for this engine edition
	query, found := s.getQueryForMetric("sqlserver.instance.lock_metrics")
	if !found {
		return fmt.Errorf("no lock metrics query available for engine edition %d", s.engineEdition)
	}

	s.logger.Debug("Executing lock metrics query",
		zap.String("query", queries.TruncateQuery(query, 100)),
		zap.String("engine_type", queries.GetEngineTypeName(s.engineEdition)))

	var results []models.InstanceLockMetricsModel
	if err := s.connection.Query(ctx, &results, query); err != nil {
		s.logger.Error("Failed to execute instance lock metrics query",
			zap.Error(err),
			zap.String("query", queries.TruncateQuery(query, 100)),
			zap.Int("engine_edition", s.engineEdition))
		return fmt.Errorf("failed to execute instance lock metrics query: %w", err)
	}

	if len(results) == 0 {
		s.logger.Warn("No results returned from instance lock metrics query - SQL Server may not be ready")
		return fmt.Errorf("no results returned from instance lock metrics query")
	}

	result := results[0]

	if err := s.processInstanceLockMetrics(result); err != nil {
		s.logger.Error("Failed to process instance lock metrics", zap.Error(err))
		return fmt.Errorf("failed to process instance lock metrics: %w", err)
	}

	s.logger.Debug("Successfully scraped instance lock metrics")
	return nil
}
