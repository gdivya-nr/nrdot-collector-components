// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package newrelicsqlserverreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver"

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/newrelic/go-agent/v3/newrelic"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/scraper/scrapererror"
	"go.uber.org/multierr"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/helpers"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/models"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/queries"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/scrapers"
)

// GlobalNRApp is the New Relic application instance set by main package
var GlobalNRApp *newrelic.Application

// SetGlobalNewRelicApp sets the New Relic app for transaction tracking
func SetGlobalNewRelicApp(app *newrelic.Application) {
	GlobalNRApp = app
}

// Global metrics that can be read by New Relic reporter
var (
	TotalScrapeCount        int64
	TotalMetricsCollected   int64
	LastScrapeDurationMs    int64
	DatabaseScraperCalls    int64
	InstanceScraperCalls    int64
	SlowQueryCount          int64
	ActiveQueryCount        int64
	LastDatabaseDurationMs  int64
	LastInstanceDurationMs  int64
	LastSlowQueryDurationMs int64
	LastActiveQueryDurationMs int64
)

// sqlServerScraper handles SQL Server metrics collection
type sqlServerScraper struct {
	connection              *SQLConnection
	config                  *Config
	logger                  *zap.Logger
	startTime               pcommon.Timestamp
	settings                receiver.Settings
	mb                      *metadata.MetricsBuilder // Shared MetricsBuilder for all scrapers (Oracle pattern)
	instanceScraper         *scrapers.InstanceScraper
	queryPerformanceScraper *scrapers.QueryPerformanceScraper
	// slowQueryScraper  *scrapers.SlowQueryScraper
	databaseScraper               *scrapers.DatabaseScraper
	userConnectionScraper         *scrapers.UserConnectionScraper
	failoverClusterScraper        *scrapers.FailoverClusterScraper
	databasePrincipalsScraper     *scrapers.DatabasePrincipalsScraper
	databaseRoleMembershipScraper *scrapers.DatabaseRoleMembershipScraper
	waitTimeScraper               *scrapers.WaitTimeScraper         // Add this line
	securityScraper               *scrapers.SecurityScraper         // Security metrics scraper
	lockScraper                   *scrapers.LockScraper             // Lock analysis metrics scraper
	threadPoolHealthScraper       *scrapers.ThreadPoolHealthScraper // Thread pool health monitoring
	tempdbContentionScraper       *scrapers.TempDBContentionScraper // TempDB contention monitoring
	metadataCache                 *helpers.MetadataCache            // Metadata cache for wait resource enrichment
	engineEdition                 int                               // SQL Server engine edition (0=Unknown, 5=Azure DB, 8=Azure MI)
}

// newSqlServerScraper creates a new SQL Server scraper with structured approach
func newSqlServerScraper(settings receiver.Settings, cfg *Config) *sqlServerScraper {
	return &sqlServerScraper{
		config:   cfg,
		logger:   settings.Logger,
		settings: settings,
	}
}

// Start initializes the scraper and establishes database connection
func (s *sqlServerScraper) Start(ctx context.Context, _ component.Host) error {
	s.logger.Info("Starting SQL Server receiver")

	connection, err := NewSQLConnection(ctx, s.config, s.logger)
	if err != nil {
		s.logger.Error("Failed to connect to SQL Server", zap.Error(err))
		return err
	}
	s.connection = connection
	s.startTime = pcommon.NewTimestampFromTime(time.Now())

	if err := s.connection.Ping(ctx); err != nil {
		s.logger.Error("Failed to ping SQL Server", zap.Error(err))
		return err
	}

	// Get EngineEdition
	s.engineEdition = 0 // Default to 0 (Unknown)
	s.engineEdition, err = s.detectEngineEdition(ctx)
	if err != nil {
		s.logger.Debug("Failed to get engine edition, using default", zap.Error(err))
		s.engineEdition = queries.StandardSQLServerEngineEdition
	} else {
		s.logger.Info("Detected SQL Server engine edition",
			zap.Int("engine_edition", s.engineEdition),
			zap.String("engine_type", queries.GetEngineTypeName(s.engineEdition)))
	}

	// Create ONE MetricsBuilder that will be shared across all scrapers (Oracle pattern)
	s.mb = metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), s.settings)

	// Initialize metadata cache for wait resource enrichment if enabled
	if s.config.EnableWaitResourceEnrichment {
		refreshInterval := time.Duration(s.config.WaitResourceMetadataRefreshMinutes) * time.Minute
		s.metadataCache = helpers.NewMetadataCache(s.connection.Connection.DB, refreshInterval, s.config.MonitoredDatabases)

		// Perform initial cache refresh
		s.logger.Info("Initializing metadata cache for wait resource enrichment",
			zap.Int("refresh_interval_minutes", s.config.WaitResourceMetadataRefreshMinutes),
			zap.Strings("monitored_databases", s.config.MonitoredDatabases))

		if err := s.metadataCache.Refresh(ctx); err != nil {
			s.logger.Warn("Failed to perform initial metadata cache refresh",
				zap.Error(err))
			// Continue - cache will retry on next scrape
		} else {
			stats := s.metadataCache.GetCacheStats()
			s.logger.Info("Metadata cache initialized successfully",
				zap.Int("databases", stats["databases"]),
				zap.Int("objects", stats["objects"]),
				zap.Int("hobts", stats["hobts"]),
				zap.Int("partitions", stats["partitions"]))
		}
	} else {
		s.logger.Info("Wait resource enrichment disabled, skipping metadata cache initialization")
	}

	// Initialize instance scraper with engine edition for engine-specific queries
	// Create instance scraper for instance-level metrics
	s.instanceScraper = scrapers.NewInstanceScraper(s.connection, s.logger, s.mb, s.engineEdition, s.config)

	// Create database scraper for database-level metrics
	s.databaseScraper = scrapers.NewDatabaseScraper(s.connection, s.logger, s.mb, s.engineEdition, s.config)

	// Create failover cluster scraper for Always On Availability Group metrics
	s.failoverClusterScraper = scrapers.NewFailoverClusterScraper(s.connection, s.logger, s.mb, s.engineEdition)

	// Create database principals scraper for database security metrics
	s.databasePrincipalsScraper = scrapers.NewDatabasePrincipalsScraper(s.connection, s.logger, s.mb, s.engineEdition)

	// Create database role membership scraper for database role and membership metrics
	s.databaseRoleMembershipScraper = scrapers.NewDatabaseRoleMembershipScraper(s.logger, s.connection, s.mb, s.engineEdition)

	// Initialize query performance scraper for blocking sessions and performance monitoring
	// Pass smoothing and interval calculator configuration parameters from config
	// Note: Interval-based averaging is always enabled (no longer configurable)
	s.queryPerformanceScraper = scrapers.NewQueryPerformanceScraper(
		s.connection,
		s.logger,
		s.mb,
		s.engineEdition,
		s.config.EnableSlowQuerySmoothing,
		s.config.SlowQuerySmoothingFactor,
		s.config.SlowQuerySmoothingDecayThreshold,
		s.config.SlowQuerySmoothingMaxAgeMinutes,
		true, // Always enable interval-based averaging
		s.config.IntervalCalculatorCacheTTLMinutes,
		s.config.ActiveRunningQueriesCountThreshold,
		s.metadataCache,
	)
	// s.slowQueryScraper = scrapers.NewSlowQueryScraper(s.logger, s.connection)

	// Initialize user connection scraper for user connection and authentication metrics
	s.userConnectionScraper = scrapers.NewUserConnectionScraper(s.connection, s.logger, s.engineEdition, s.mb)

	// Initialize wait time scraper for wait time metrics
	s.waitTimeScraper = scrapers.NewWaitTimeScraper(s.connection, s.logger, s.engineEdition, s.mb, s.config)

	// Initialize security scraper for server-level security metrics
	s.securityScraper = scrapers.NewSecurityScraper(s.connection, s.logger, s.mb, s.engineEdition)

	// Initialize lock scraper for lock analysis metrics
	s.lockScraper = scrapers.NewLockScraper(s.connection, s.logger, s.mb, s.engineEdition)

	// Initialize thread pool health scraper for thread pool monitoring
	s.threadPoolHealthScraper = scrapers.NewThreadPoolHealthScraper(s.connection, s.logger, s.mb)

	// Initialize TempDB contention scraper for TempDB monitoring
	s.tempdbContentionScraper = scrapers.NewTempDBContentionScraper(s.connection, s.logger, s.mb)

	s.logger.Info("Successfully connected to SQL Server",
		zap.String("hostname", s.config.Hostname),
		zap.String("port", s.config.Port),
		zap.Int("engine_edition", s.engineEdition),
		zap.String("engine_type", queries.GetEngineTypeName(s.engineEdition)))

	return nil
}

// Shutdown closes the database connection
func (s *sqlServerScraper) Shutdown(ctx context.Context) error {
	s.logger.Info("Shutting down SQL Server receiver")
	if s.connection != nil {
		s.connection.Close()
	}
	return nil
}

// detectEngineEdition detects the SQL Server engine edition following nri-mssql pattern
// detectEngineEdition detects the SQL Server engine edition
func (s *sqlServerScraper) detectEngineEdition(ctx context.Context) (int, error) {
	queryFunc := func(query string) (int, error) {
		var results []struct {
			EngineEdition int `db:"EngineEdition"`
		}

		err := s.connection.Query(ctx, &results, query)
		if err != nil {
			return 0, err
		}

		if len(results) == 0 {
			s.logger.Debug("EngineEdition query returned empty output.")
			return 0, nil
		}

		s.logger.Debug("Detected EngineEdition", zap.Int("engine_edition", results[0].EngineEdition))
		return results[0].EngineEdition, nil
	}

	return queries.DetectEngineEdition(queryFunc)
}

// scrape collects SQL Server instance metrics using structured approach
func (s *sqlServerScraper) scrape(ctx context.Context) (pmetric.Metrics, error) {
	// Track scrape start time for New Relic reporting
	scrapeStartTime := time.Now()

	s.logger.Debug("Starting SQL Server metrics collection",
		zap.String("hostname", s.config.Hostname),
		zap.String("port", s.config.Port))

	// Track scraping errors but continue with partial results
	var scrapeErrors []error

	// Check connection health and refresh metadata cache
	if err := s.healthCheck(ctx); err != nil {
		scrapeErrors = collectErrors(scrapeErrors, fmt.Errorf("connection health check failed: %w", err))
	}
	s.refreshMetadataCache(ctx)

	// === Database Metrics Category ===
	// ALWAYS scrape database metrics - mandatory metrics will always be emitted
	databaseStartTime := time.Now()

	// START New Relic transaction for database category
	var databaseTxn *newrelic.Transaction
	if GlobalNRApp != nil {
		databaseTxn = GlobalNRApp.StartTransaction("sqlserver/database_metrics")
		databaseTxn.AddAttribute("category", "database_metrics")
		databaseTxn.AddAttribute("start_time", databaseStartTime.Unix())
	}

	s.logger.Info("Starting database metrics collection", zap.Time("start_time", databaseStartTime))

	// Scrape database metrics concurrently (independent metrics)
	databaseScrapers := map[string]scrapeFunc{
		"database IO metrics":              s.databaseScraper.ScrapeDatabaseIOMetrics,
		"database log growth metrics":      s.databaseScraper.ScrapeDatabaseLogGrowthMetrics,
		"database page file metrics":       s.databaseScraper.ScrapeDatabasePageFileMetrics,
		"database page file total metrics": s.databaseScraper.ScrapeDatabasePageFileTotalMetrics,
		"database memory metrics":          s.databaseScraper.ScrapeDatabaseMemoryMetrics,
		"database size metrics":            s.databaseScraper.ScrapeDatabaseSizeMetrics,
		"database disk metrics":            s.databaseScraper.ScrapeDatabaseDiskMetrics,
		"database transaction log metrics": s.databaseScraper.ScrapeDatabaseTransactionLogMetrics,
		"database log space usage metrics": s.databaseScraper.ScrapeDatabaseLogSpaceUsageMetrics,
	}

	// Add conditional buffer metrics if enabled
	if s.config.EnableDatabaseBufferMetrics {
		databaseScrapers["database buffer metrics"] = s.databaseScraper.ScrapeDatabaseBufferMetrics
	}

	// Execute all database scrapers concurrently
	dbErrors := s.concurrentScrape(ctx, databaseScrapers)
	for _, err := range dbErrors {
		scrapeErrors = collectErrors(scrapeErrors, err)
	}
	databaseDuration := time.Since(databaseStartTime)
	atomic.AddInt64(&DatabaseScraperCalls, 1)
	atomic.StoreInt64(&LastDatabaseDurationMs, databaseDuration.Milliseconds())

	// END New Relic transaction for database category
	if databaseTxn != nil {
		databaseTxn.AddAttribute("end_time", time.Now().Unix())
		databaseTxn.AddAttribute("duration_ms", databaseDuration.Milliseconds())
		databaseTxn.AddAttribute("total_scrapers", len(databaseScrapers))
		databaseTxn.AddAttribute("queries_passed", len(databaseScrapers)-len(dbErrors))
		databaseTxn.AddAttribute("queries_failed", len(dbErrors))
		if len(dbErrors) > 0 {
			for _, err := range dbErrors {
				databaseTxn.NoticeError(err)
			}
		}
		databaseTxn.End()
	}

	s.logger.Info("Completed database metrics collection",
		zap.Duration("duration", databaseDuration),
		zap.Int("total_scrapers", len(databaseScrapers)),
		zap.Int("queries_passed", len(databaseScrapers)-len(dbErrors)),
		zap.Int("queries_failed", len(dbErrors)))

	// Scrape slow query metrics (always enabled)
	slowQueryStartTime := time.Now()

	// START New Relic transaction for slow query category
	var slowQueryTxn *newrelic.Transaction
	if GlobalNRApp != nil {
		slowQueryTxn = GlobalNRApp.StartTransaction("sqlserver/slow_query_metrics")
		slowQueryTxn.AddAttribute("category", "slow_query_metrics")
		slowQueryTxn.AddAttribute("start_time", slowQueryStartTime.Unix())
	}

	s.logger.Info("Starting slow query metrics collection", zap.Time("start_time", slowQueryStartTime))
	// Store query IDs and lightweight plan data (5 fields only) for correlation with active queries
	// Create a fresh APM metadata cache for this scrape cycle
	// This cache will be shared between active and slow query scrapers and discarded at scrape end
	apmMetadataCache := helpers.NewAPMMetadataCache(s.logger)
	s.logger.Debug("Created fresh APM metadata cache for current scrape cycle")

	var slowQueryIDs []string
	var slowQueryPlanDataMap map[string]models.SlowQueryPlanData

	// Initialize empty map to prevent nil pointer panic when backfilling for active queries
	slowQueryPlanDataMap = make(map[string]models.SlowQueryPlanData)

	slowQueryCtx, slowQueryCancel := context.WithTimeout(ctx, s.config.Timeout)
	defer slowQueryCancel()

	// Use config values for slow query monitoring
	intervalSeconds := s.config.QueryMonitoringFetchInterval
	elapsedTimeThresholdMS := s.config.QueryMonitoringResponseTimeThreshold
	topN := s.config.QueryMonitoringCountThreshold

	s.logger.Info("Attempting to scrape slow query metrics with filtering",
		zap.Int("interval_seconds", intervalSeconds),
		zap.Int("elapsed_time_threshold_ms", elapsedTimeThresholdMS),
		zap.Int("top_n", topN))

	slowQueries, err := s.queryPerformanceScraper.ScrapeSlowQueryMetrics(
		slowQueryCtx,
		intervalSeconds,
		elapsedTimeThresholdMS,
		topN,
		true,
		apmMetadataCache,
	)
	if err != nil {
		s.logger.Warn("Failed to scrape slow query metrics - continuing with other metrics",
			zap.Error(err),
			zap.Duration("timeout", s.config.Timeout),
			zap.Int("interval_seconds", intervalSeconds))
		// Don't add to scrapeErrors - just warn and continue
	} else {
		s.logger.Info("Successfully scraped slow query metrics with filtering applied",
			zap.Int("interval_seconds", intervalSeconds),
			zap.Int("elapsed_time_threshold_ms", elapsedTimeThresholdMS),
			zap.Int("top_n", topN),
			zap.Int("slow_query_count", len(slowQueries)))

		// Extract query IDs and lightweight plan data (5 fields only) for active query correlation
		slowQueryIDs, slowQueryPlanDataMap = s.queryPerformanceScraper.ExtractQueryDataFromSlowQueries(slowQueries)
		s.logger.Info("Extracted query IDs and lightweight plan data (5 fields only, in-memory)",
			zap.Int("unique_query_id_count", len(slowQueryIDs)),
			zap.Int("plan_data_map_size", len(slowQueryPlanDataMap)))
	}
	// Update global metrics for New Relic
	atomic.StoreInt64(&SlowQueryCount, int64(len(slowQueries)))
	atomic.StoreInt64(&LastSlowQueryDurationMs, time.Since(slowQueryStartTime).Milliseconds())

	// END New Relic transaction for slow query category
	slowQueryDuration := time.Since(slowQueryStartTime)
	if slowQueryTxn != nil {
		slowQueryTxn.AddAttribute("end_time", time.Now().Unix())
		slowQueryTxn.AddAttribute("duration_ms", slowQueryDuration.Milliseconds())
		slowQueryTxn.AddAttribute("queries_found", len(slowQueries))
		slowQueryTxn.AddAttribute("queries_passed", len(slowQueries))
		slowQueryTxn.AddAttribute("queries_failed", 0)
		if err != nil {
			slowQueryTxn.AddAttribute("queries_failed", 1)
			slowQueryTxn.NoticeError(err)
		}
		slowQueryTxn.End()
	}

	s.logger.Info("Slow query metrics transaction completed",
		zap.Duration("duration", slowQueryDuration),
		zap.Int("queries_found", len(slowQueries)),
		zap.Bool("success", err == nil))

	// Scrape active running queries metrics - split into linked and orphan queries
	activeQueryStartTime := time.Now()

	// START New Relic transaction for active query category
	var activeQueryTxn *newrelic.Transaction
	if GlobalNRApp != nil {
		activeQueryTxn = GlobalNRApp.StartTransaction("sqlserver/active_query_metrics")
		activeQueryTxn.AddAttribute("category", "active_query_metrics")
		activeQueryTxn.AddAttribute("start_time", activeQueryStartTime.Unix())
	}

	scrapeCtx, cancel := context.WithTimeout(ctx, s.config.Timeout)
	defer cancel()

	s.logger.Info("Attempting to scrape active running queries metrics (linked + orphan)",
		zap.Int("slow_query_hash_count", len(slowQueryIDs)))

	// Step 1a: Fetch LINKED active queries (query_hash IN slow_query_hashes)
	linkedActiveQueries, err := s.queryPerformanceScraper.ScrapeLinkedActiveQueries(scrapeCtx, slowQueryIDs)
	if err != nil {
		s.logger.Warn("Failed to fetch linked active running queries - continuing with orphan queries",
			zap.Error(err),
			zap.Duration("timeout", s.config.Timeout))
		// Don't add to scrapeErrors - just warn and continue
		linkedActiveQueries = []models.ActiveRunningQuery{} // Empty slice for processing
	} else {
		s.logger.Info("Linked active queries fetched (correlated with slow queries)",
			zap.Int("linked_count", len(linkedActiveQueries)))
	}

	// Step 1b: Fetch ORPHAN active queries (query_hash NOT IN slow_query_hashes)
	orphanActiveQueries, err := s.queryPerformanceScraper.ScrapeOrphanActiveQueries(scrapeCtx, slowQueryIDs)
	if err != nil {
		s.logger.Warn("Failed to fetch orphan active running queries - continuing with linked queries",
			zap.Error(err),
			zap.Duration("timeout", s.config.Timeout))
		// Don't add to scrapeErrors - just warn and continue
		orphanActiveQueries = []models.ActiveRunningQuery{} // Empty slice for processing
	} else {
		s.logger.Info("Orphan active queries fetched (not correlated with slow queries)",
			zap.Int("orphan_count", len(orphanActiveQueries)))
	}

	// Combine linked and orphan queries for processing
	activeQueries := append(linkedActiveQueries, orphanActiveQueries...)

	if len(activeQueries) == 0 {
		s.logger.Info("No active queries found (linked + orphan = 0)")
	} else {
		s.logger.Info("Active queries fetched with balanced linked/orphan approach",
			zap.Int("total_active_queries", len(activeQueries)),
			zap.Int("linked_queries", len(linkedActiveQueries)),
			zap.Int("orphan_queries", len(orphanActiveQueries)))

		// Phase 1: Identify active queries missing from slow query map (need backfill)
		// Collect unique query_hashes that don't have plan data yet
		missingQueryHashes := make(map[string]bool) // Use map for automatic deduplication
		for _, activeQuery := range activeQueries {
			if activeQuery.QueryID != nil && !activeQuery.QueryID.IsEmpty() {
				queryIDStr := activeQuery.QueryID.String()
				planData, found := slowQueryPlanDataMap[queryIDStr]

				// Backfill if ANY of these conditions are true:
				// 1. Query ID not found in slow query map
				// 2. Found but plan_handle is NULL/empty (plan evicted from cache)
				// 3. Found but missing required fields for sqlserver.plan.avg_elapsed_time_ms metric
				needsBackfill := !found ||
					planData.PlanHandle == nil || planData.PlanHandle.IsEmpty() ||
					planData.AvgElapsedTimeMs == nil ||
					planData.CreationTime == nil ||
					planData.LastExecutionTime == nil

				if needsBackfill {
					// Mark for backfill - will fetch from dm_exec_query_stats
					missingQueryHashes[queryIDStr] = true
				}
			}
		}

		if len(missingQueryHashes) > 0 {
			s.logger.Info("Identified active queries missing plan data - will attempt backfill",
				zap.Int("missing_count", len(missingQueryHashes)))

			// Phase 2: Backfill missing plan handles from dm_exec_requests / dm_exec_query_stats
			// Convert map keys to slice for backfill function
			missingHashList := make([]string, 0, len(missingQueryHashes))
			for queryHash := range missingQueryHashes {
				missingHashList = append(missingHashList, queryHash)
			}

			// Create a new context for backfill (separate timeout from active query fetch)
			backfillCtx, backfillCancel := context.WithTimeout(ctx, s.config.Timeout)
			defer backfillCancel()

			// Call backfill function to fetch plan_handles for missing queries
			backfilledPlanData, err := s.queryPerformanceScraper.BackfillPlanHandlesForActiveQueries(
				backfillCtx, missingHashList)

			if err != nil {
				s.logger.Warn("Failed to backfill plan handles - continuing without backfill",
					zap.Error(err),
					zap.Int("missing_count", len(missingQueryHashes)))
			} else if len(backfilledPlanData) > 0 {
				// Phase 3: Merge backfilled data into slowQueryPlanDataMap
				originalSize := len(slowQueryPlanDataMap)
				for queryHash, planData := range backfilledPlanData {
					slowQueryPlanDataMap[queryHash] = planData
				}

				s.logger.Info("Merged backfilled plan data into slow query map",
					zap.Int("original_size", originalSize),
					zap.Int("backfilled_count", len(backfilledPlanData)),
					zap.Int("new_size", len(slowQueryPlanDataMap)),
					zap.Int("coverage_percent", (len(slowQueryPlanDataMap)*100)/len(activeQueries)))
			} else {
				s.logger.Info("Backfill completed but no plan handles found (queries not in dm_exec_requests or dm_exec_query_stats)",
					zap.Int("missing_count", len(missingQueryHashes)))
			}
		} else {
			s.logger.Info("All active queries matched with slow queries - no backfill needed")
		}

		// Step 2: Emit metrics for active queries (using lightweight plan data from memory and APM metadata cache)
		if err := s.queryPerformanceScraper.EmitActiveRunningQueriesMetrics(scrapeCtx, activeQueries, slowQueryPlanDataMap, apmMetadataCache); err != nil {
			s.logger.Warn("Failed to emit active running queries metrics",
				zap.Error(err))
		} else {
			s.logger.Info("Successfully emitted active running queries metrics",
				zap.Int("active_query_count", len(activeQueries)))
		}

		// Step 2.5: Emit blocking queries as custom events (metrics → logs via metricsaslogs connector)
		if err := s.queryPerformanceScraper.EmitBlockingQueriesAsCustomEvents(activeQueries); err != nil {
			s.logger.Warn("Failed to emit blocking query events",
				zap.Error(err))
		} else {
			s.logger.Info("Successfully emitted blocking query events")
		}

		// Step 2.6: Emit active query details as custom events (metrics → logs via metricsaslogs connector)
		// This stores the full query text in SqlServerQueryDetails event, bypassing the 2KB metric attribute limit
		if err := s.queryPerformanceScraper.EmitActiveQueryDetailsAsCustomEvents(activeQueries); err != nil {
			s.logger.Warn("Failed to emit active query details events",
				zap.Error(err))
		} else {
			s.logger.Info("Successfully emitted active query details events")
		}

		// Step 3: Emit execution plan statistics using lightweight plan data from memory (5 fields only, NO database query)
		if err := s.queryPerformanceScraper.ScrapeActiveQueryPlanStatistics(scrapeCtx, activeQueries, slowQueryPlanDataMap); err != nil {
			s.logger.Warn("Failed to scrape active query execution plan statistics - continuing with other metrics",
				zap.Error(err))
			// Don't fail the entire scrape, just log the warning
		} else {
			s.logger.Info("Successfully emitted execution plan statistics as metrics",
				zap.Int("active_query_count", len(activeQueries)))
		}
	}
	// Update global metrics for New Relic
	atomic.StoreInt64(&ActiveQueryCount, int64(len(activeQueries)))
	atomic.StoreInt64(&LastActiveQueryDurationMs, time.Since(activeQueryStartTime).Milliseconds())

	// END New Relic transaction for active query category
	activeQueryDuration := time.Since(activeQueryStartTime)
	if activeQueryTxn != nil {
		activeQueryTxn.AddAttribute("end_time", time.Now().Unix())
		activeQueryTxn.AddAttribute("duration_ms", activeQueryDuration.Milliseconds())
		activeQueryTxn.AddAttribute("queries_found", len(activeQueries))
		activeQueryTxn.AddAttribute("linked_queries", len(linkedActiveQueries))
		activeQueryTxn.AddAttribute("orphan_queries", len(orphanActiveQueries))
		activeQueryTxn.AddAttribute("queries_passed", len(activeQueries))
		activeQueryTxn.AddAttribute("queries_failed", 0)
		activeQueryTxn.End()
	}

	s.logger.Info("Active query metrics transaction completed",
		zap.Duration("duration", activeQueryDuration),
		zap.Int("queries_found", len(activeQueries)),
		zap.Int("linked_queries", len(linkedActiveQueries)),
		zap.Int("orphan_queries", len(orphanActiveQueries)))

	// === Instance Metrics Category ===
	// ALWAYS scrape instance metrics - mandatory metrics will always be emitted
	instanceStartTime := time.Now()

	// START New Relic transaction for instance category
	var instanceTxn *newrelic.Transaction
	if GlobalNRApp != nil {
		instanceTxn = GlobalNRApp.StartTransaction("sqlserver/instance_metrics")
		instanceTxn.AddAttribute("category", "instance_metrics")
		instanceTxn.AddAttribute("start_time", instanceStartTime.Unix())
	}

	// Scrape all instance metrics concurrently (all independent)
	instanceScrapers := map[string]scrapeFunc{
		"instance comprehensive stats":     s.instanceScraper.ScrapeInstanceComprehensiveStats,
		"instance memory metrics":          s.instanceScraper.ScrapeInstanceMemoryMetrics,
		"instance process counts":          s.instanceScraper.ScrapeInstanceProcessCounts,
		"instance runnable tasks":          s.instanceScraper.ScrapeInstanceRunnableTasks,
		"instance active connections":      s.instanceScraper.ScrapeInstanceActiveConnections,
		"instance buffer pool hit percent": s.instanceScraper.ScrapeInstanceBufferPoolHitPercent,
		"instance disk metrics":            s.instanceScraper.ScrapeInstanceDiskMetrics,
		"instance buffer pool size":        s.instanceScraper.ScrapeInstanceBufferPoolSize,
		"instance target memory":           s.instanceScraper.ScrapeInstanceTargetMemoryMetrics,
		"instance performance ratios":      s.instanceScraper.ScrapeInstancePerformanceRatiosMetrics,
		"instance index metrics":           s.instanceScraper.ScrapeInstanceIndexMetrics,
		"instance lock metrics":            s.instanceScraper.ScrapeInstanceLockMetrics,
	}

	// Execute all instance scrapers concurrently
	instanceErrors := s.concurrentScrape(ctx, instanceScrapers)
	for _, err := range instanceErrors {
		scrapeErrors = collectErrors(scrapeErrors, err)
	}
	// Update global metrics for New Relic
	atomic.AddInt64(&InstanceScraperCalls, 1)
	atomic.StoreInt64(&LastInstanceDurationMs, time.Since(instanceStartTime).Milliseconds())

	// END New Relic transaction for instance category
	instanceDuration := time.Since(instanceStartTime)
	if instanceTxn != nil {
		instanceTxn.AddAttribute("end_time", time.Now().Unix())
		instanceTxn.AddAttribute("duration_ms", instanceDuration.Milliseconds())
		instanceTxn.AddAttribute("total_scrapers", len(instanceScrapers))
		instanceTxn.AddAttribute("queries_passed", len(instanceScrapers)-len(instanceErrors))
		instanceTxn.AddAttribute("queries_failed", len(instanceErrors))
		if len(instanceErrors) > 0 {
			for _, err := range instanceErrors {
				instanceTxn.NoticeError(err)
			}
		}
		instanceTxn.End()
	}

	s.logger.Info("Instance metrics transaction completed",
		zap.Duration("duration", instanceDuration),
		zap.Int("total_scrapers", len(instanceScrapers)),
		zap.Int("queries_passed", len(instanceScrapers)-len(instanceErrors)),
		zap.Int("queries_failed", len(instanceErrors)))

	// === User Connection Metrics Category ===
	if s.config.EnableUserConnectionMetrics {
		userConnStartTime := time.Now()

		// START New Relic transaction
		var userConnTxn *newrelic.Transaction
		if GlobalNRApp != nil {
			userConnTxn = GlobalNRApp.StartTransaction("sqlserver/user_connection_metrics")
			userConnTxn.AddAttribute("category", "user_connection_metrics")
			userConnTxn.AddAttribute("start_time", userConnStartTime.Unix())
		}

		userConnectionScrapers := map[string]scrapeFunc{
			"user connection summary":        s.userConnectionScraper.ScrapeUserConnectionSummaryMetrics,
			"user connection utilization":    s.userConnectionScraper.ScrapeUserConnectionUtilizationMetrics,
			"user connection by client":      s.userConnectionScraper.ScrapeUserConnectionByClientMetrics,
			"user connection client summary": s.userConnectionScraper.ScrapeUserConnectionClientSummaryMetrics,
			"user connection stats":          s.userConnectionScraper.ScrapeUserConnectionStatsMetrics,
			"login logout summary":           s.userConnectionScraper.ScrapeLoginLogoutSummaryMetrics,
			"failed login summary":           s.userConnectionScraper.ScrapeFailedLoginSummaryMetrics,
		}

		userConnErrors := s.concurrentScrape(ctx, userConnectionScrapers)
		for _, err := range userConnErrors {
			scrapeErrors = collectErrors(scrapeErrors, err)
		}

		// END New Relic transaction
		userConnDuration := time.Since(userConnStartTime)
		if userConnTxn != nil {
			userConnTxn.AddAttribute("end_time", time.Now().Unix())
			userConnTxn.AddAttribute("duration_ms", userConnDuration.Milliseconds())
			userConnTxn.AddAttribute("total_scrapers", len(userConnectionScrapers))
			userConnTxn.AddAttribute("queries_passed", len(userConnectionScrapers)-len(userConnErrors))
			userConnTxn.AddAttribute("queries_failed", len(userConnErrors))
			if len(userConnErrors) > 0 {
				for _, err := range userConnErrors {
					userConnTxn.NoticeError(err)
				}
			}
			userConnTxn.End()
		}
	} else {
		s.logger.Info("User connection metrics scraping SKIPPED - EnableUserConnectionMetrics is false")
	}

	// === Failover Cluster Metrics Category ===
	if s.config.EnableFailoverClusterMetrics {
		failoverStartTime := time.Now()

		// START New Relic transaction
		var failoverTxn *newrelic.Transaction
		if GlobalNRApp != nil {
			failoverTxn = GlobalNRApp.StartTransaction("sqlserver/failover_cluster_metrics")
			failoverTxn.AddAttribute("category", "failover_cluster_metrics")
			failoverTxn.AddAttribute("start_time", failoverStartTime.Unix())
		}

		failoverScrapers := map[string]scrapeFunc{
			"failover cluster replica":                  s.failoverClusterScraper.ScrapeFailoverClusterMetrics,
			"failover availability group health":        s.failoverClusterScraper.ScrapeFailoverClusterAvailabilityGroupHealthMetrics,
			"failover availability group configuration": s.failoverClusterScraper.ScrapeFailoverClusterAvailabilityGroupMetrics,
			"failover cluster redo queue":               s.failoverClusterScraper.ScrapeFailoverClusterRedoQueueMetrics,
		}

		failoverErrors := s.concurrentScrape(ctx, failoverScrapers)
		for _, err := range failoverErrors {
			scrapeErrors = collectErrors(scrapeErrors, err)
		}

		// END New Relic transaction
		failoverDuration := time.Since(failoverStartTime)
		if failoverTxn != nil {
			failoverTxn.AddAttribute("end_time", time.Now().Unix())
			failoverTxn.AddAttribute("duration_ms", failoverDuration.Milliseconds())
			failoverTxn.AddAttribute("total_scrapers", len(failoverScrapers))
			failoverTxn.AddAttribute("queries_passed", len(failoverScrapers)-len(failoverErrors))
			failoverTxn.AddAttribute("queries_failed", len(failoverErrors))
			if len(failoverErrors) > 0 {
				for _, err := range failoverErrors {
					failoverTxn.NoticeError(err)
				}
			}
			failoverTxn.End()
		}
	} else {
		s.logger.Info("Failover cluster metrics scraping SKIPPED - EnableFailoverClusterMetrics is false")
	}

	// === Database Principals Metrics Category ===
	if s.config.EnableDatabasePrincipalsMetrics {
		principalsStartTime := time.Now()

		// START New Relic transaction
		var principalsTxn *newrelic.Transaction
		if GlobalNRApp != nil {
			principalsTxn = GlobalNRApp.StartTransaction("sqlserver/database_principals_metrics")
			principalsTxn.AddAttribute("category", "database_principals_metrics")
			principalsTxn.AddAttribute("start_time", principalsStartTime.Unix())
		}

		principalsScrapers := map[string]scrapeFunc{
			"database principals summary":  s.databasePrincipalsScraper.ScrapeDatabasePrincipalsSummaryMetrics,
			"database principals activity": s.databasePrincipalsScraper.ScrapeDatabasePrincipalActivityMetrics,
		}

		principalsErrors := s.concurrentScrape(ctx, principalsScrapers)
		for _, err := range principalsErrors {
			scrapeErrors = collectErrors(scrapeErrors, err)
		}

		// END New Relic transaction
		principalsDuration := time.Since(principalsStartTime)
		if principalsTxn != nil {
			principalsTxn.AddAttribute("end_time", time.Now().Unix())
			principalsTxn.AddAttribute("duration_ms", principalsDuration.Milliseconds())
			principalsTxn.AddAttribute("total_scrapers", len(principalsScrapers))
			principalsTxn.AddAttribute("queries_passed", len(principalsScrapers)-len(principalsErrors))
			principalsTxn.AddAttribute("queries_failed", len(principalsErrors))
			if len(principalsErrors) > 0 {
				for _, err := range principalsErrors {
					principalsTxn.NoticeError(err)
				}
			}
			principalsTxn.End()
		}
	} else {
		s.logger.Info("Database principals metrics scraping SKIPPED - EnableDatabasePrincipalsMetrics is false")
	}

	// === Database Role Membership Metrics Category ===
	if s.config.EnableDatabaseRoleMembershipMetrics {
		roleMembershipStartTime := time.Now()

		// START New Relic transaction
		var roleMembershipTxn *newrelic.Transaction
		if GlobalNRApp != nil {
			roleMembershipTxn = GlobalNRApp.StartTransaction("sqlserver/database_role_membership_metrics")
			roleMembershipTxn.AddAttribute("category", "database_role_membership_metrics")
			roleMembershipTxn.AddAttribute("start_time", roleMembershipStartTime.Unix())
		}

		roleMembershipScrapers := map[string]scrapeFunc{
			"database role membership summary": s.databaseRoleMembershipScraper.ScrapeDatabaseRoleMembershipSummaryMetrics,
			"database role activity":           s.databaseRoleMembershipScraper.ScrapeDatabaseRoleActivityMetrics,
			"database role permission matrix":  s.databaseRoleMembershipScraper.ScrapeDatabaseRolePermissionMatrixMetrics,
		}

		roleMembershipErrors := s.concurrentScrape(ctx, roleMembershipScrapers)
		for _, err := range roleMembershipErrors {
			scrapeErrors = collectErrors(scrapeErrors, err)
		}

		// END New Relic transaction
		roleMembershipDuration := time.Since(roleMembershipStartTime)
		if roleMembershipTxn != nil {
			roleMembershipTxn.AddAttribute("end_time", time.Now().Unix())
			roleMembershipTxn.AddAttribute("duration_ms", roleMembershipDuration.Milliseconds())
			roleMembershipTxn.AddAttribute("total_scrapers", len(roleMembershipScrapers))
			roleMembershipTxn.AddAttribute("queries_passed", len(roleMembershipScrapers)-len(roleMembershipErrors))
			roleMembershipTxn.AddAttribute("queries_failed", len(roleMembershipErrors))
			if len(roleMembershipErrors) > 0 {
				for _, err := range roleMembershipErrors {
					roleMembershipTxn.NoticeError(err)
				}
			}
			roleMembershipTxn.End()
		}
	} else {
		s.logger.Info("Database role membership metrics scraping SKIPPED - EnableDatabaseRoleMembershipMetrics is false")
	}

	// === Wait Time Metrics Category ===
	// ALWAYS scrape wait time metrics - mandatory metrics will always be emitted
	waitTimeStartTime := time.Now()

	// START New Relic transaction
	var waitTimeTxn *newrelic.Transaction
	if GlobalNRApp != nil {
		waitTimeTxn = GlobalNRApp.StartTransaction("sqlserver/wait_time_metrics")
		waitTimeTxn.AddAttribute("category", "wait_time_metrics")
		waitTimeTxn.AddAttribute("start_time", waitTimeStartTime.Unix())
	}

	waitTimeScrapers := map[string]scrapeFunc{
		"wait time metrics":       s.waitTimeScraper.ScrapeWaitTimeMetrics,
		"latch wait time metrics": s.waitTimeScraper.ScrapeLatchWaitTimeMetrics,
	}

	waitTimeErrors := s.concurrentScrape(ctx, waitTimeScrapers)
	for _, err := range waitTimeErrors {
		scrapeErrors = collectErrors(scrapeErrors, err)
	}

	// END New Relic transaction
	waitTimeDuration := time.Since(waitTimeStartTime)
	if waitTimeTxn != nil {
		waitTimeTxn.AddAttribute("end_time", time.Now().Unix())
		waitTimeTxn.AddAttribute("duration_ms", waitTimeDuration.Milliseconds())
		waitTimeTxn.AddAttribute("total_scrapers", len(waitTimeScrapers))
		waitTimeTxn.AddAttribute("queries_passed", len(waitTimeScrapers)-len(waitTimeErrors))
		waitTimeTxn.AddAttribute("queries_failed", len(waitTimeErrors))
		if len(waitTimeErrors) > 0 {
			for _, err := range waitTimeErrors {
				waitTimeTxn.NoticeError(err)
			}
		}
		waitTimeTxn.End()
	}

	// === Security Metrics Category ===
	if s.config.EnableSecurityMetrics {
		securityStartTime := time.Now()

		// START New Relic transaction
		var securityTxn *newrelic.Transaction
		if GlobalNRApp != nil {
			securityTxn = GlobalNRApp.StartTransaction("sqlserver/security_metrics")
			securityTxn.AddAttribute("category", "security_metrics")
			securityTxn.AddAttribute("start_time", securityStartTime.Unix())
		}

		securityScrapers := map[string]scrapeFunc{
			"security principals":   s.securityScraper.ScrapeSecurityPrincipalsMetrics,
			"security role members": s.securityScraper.ScrapeSecurityRoleMembersMetrics,
		}

		securityErrors := s.concurrentScrape(ctx, securityScrapers)
		for _, err := range securityErrors {
			scrapeErrors = collectErrors(scrapeErrors, err)
		}

		// END New Relic transaction
		securityDuration := time.Since(securityStartTime)
		if securityTxn != nil {
			securityTxn.AddAttribute("end_time", time.Now().Unix())
			securityTxn.AddAttribute("duration_ms", securityDuration.Milliseconds())
			securityTxn.AddAttribute("total_scrapers", len(securityScrapers))
			securityTxn.AddAttribute("queries_passed", len(securityScrapers)-len(securityErrors))
			securityTxn.AddAttribute("queries_failed", len(securityErrors))
			if len(securityErrors) > 0 {
				for _, err := range securityErrors {
					securityTxn.NoticeError(err)
				}
			}
			securityTxn.End()
		}
	} else {
		s.logger.Info("Security metrics scraping SKIPPED - EnableSecurityMetrics is false")
	}

	// === Lock Metrics Category ===
	if s.config.EnableLockMetrics {
		lockStartTime := time.Now()

		// START New Relic transaction
		var lockTxn *newrelic.Transaction
		if GlobalNRApp != nil {
			lockTxn = GlobalNRApp.StartTransaction("sqlserver/lock_metrics")
			lockTxn.AddAttribute("category", "lock_metrics")
			lockTxn.AddAttribute("start_time", lockStartTime.Unix())
		}

		lockScrapers := map[string]scrapeFunc{
			"lock resource metrics": s.lockScraper.ScrapeLockResourceMetrics,
			"lock mode metrics":     s.lockScraper.ScrapeLockModeMetrics,
		}

		lockErrors := s.concurrentScrape(ctx, lockScrapers)
		for _, err := range lockErrors {
			scrapeErrors = collectErrors(scrapeErrors, err)
		}

		// END New Relic transaction
		lockDuration := time.Since(lockStartTime)
		if lockTxn != nil {
			lockTxn.AddAttribute("end_time", time.Now().Unix())
			lockTxn.AddAttribute("duration_ms", lockDuration.Milliseconds())
			lockTxn.AddAttribute("total_scrapers", len(lockScrapers))
			lockTxn.AddAttribute("queries_passed", len(lockScrapers)-len(lockErrors))
			lockTxn.AddAttribute("queries_failed", len(lockErrors))
			if len(lockErrors) > 0 {
				for _, err := range lockErrors {
					lockTxn.NoticeError(err)
				}
			}
			lockTxn.End()
		}
	} else {
		s.logger.Info("Lock metrics scraping SKIPPED - EnableLockMetrics is false")
	}

	// === Thread Pool Metrics Category ===
	if s.config.EnableThreadPoolMetrics {
		threadPoolStartTime := time.Now()

		// START New Relic transaction
		var threadPoolTxn *newrelic.Transaction
		if GlobalNRApp != nil {
			threadPoolTxn = GlobalNRApp.StartTransaction("sqlserver/thread_pool_metrics")
			threadPoolTxn.AddAttribute("category", "thread_pool_metrics")
			threadPoolTxn.AddAttribute("start_time", threadPoolStartTime.Unix())
		}

		threadPoolErr := s.executeConditionalScrape(ctx, s.config.EnableThreadPoolMetrics,
			"thread pool health metrics", s.threadPoolHealthScraper.ScrapeThreadPoolHealthMetrics)
		scrapeErrors = collectErrors(scrapeErrors, threadPoolErr)

		// END New Relic transaction
		threadPoolDuration := time.Since(threadPoolStartTime)
		if threadPoolTxn != nil {
			threadPoolTxn.AddAttribute("end_time", time.Now().Unix())
			threadPoolTxn.AddAttribute("duration_ms", threadPoolDuration.Milliseconds())
			threadPoolTxn.AddAttribute("total_scrapers", 1)
			if threadPoolErr != nil {
				threadPoolTxn.AddAttribute("queries_passed", 0)
				threadPoolTxn.AddAttribute("queries_failed", 1)
				threadPoolTxn.NoticeError(threadPoolErr)
			} else {
				threadPoolTxn.AddAttribute("queries_passed", 1)
				threadPoolTxn.AddAttribute("queries_failed", 0)
			}
			threadPoolTxn.End()
		}
	} else {
		s.logger.Info("Thread pool metrics scraping SKIPPED - EnableThreadPoolMetrics is false")
	}

	// === TempDB Metrics Category ===
	if s.config.EnableTempDBMetrics {
		tempDBStartTime := time.Now()

		// START New Relic transaction
		var tempDBTxn *newrelic.Transaction
		if GlobalNRApp != nil {
			tempDBTxn = GlobalNRApp.StartTransaction("sqlserver/tempdb_metrics")
			tempDBTxn.AddAttribute("category", "tempdb_metrics")
			tempDBTxn.AddAttribute("start_time", tempDBStartTime.Unix())
		}

		tempDBErr := s.executeConditionalScrape(ctx, s.config.EnableTempDBMetrics,
			"TempDB contention metrics", s.tempdbContentionScraper.ScrapeTempDBContentionMetrics)
		scrapeErrors = collectErrors(scrapeErrors, tempDBErr)

		// END New Relic transaction
		tempDBDuration := time.Since(tempDBStartTime)
		if tempDBTxn != nil {
			tempDBTxn.AddAttribute("end_time", time.Now().Unix())
			tempDBTxn.AddAttribute("duration_ms", tempDBDuration.Milliseconds())
			tempDBTxn.AddAttribute("total_scrapers", 1)
			if tempDBErr != nil {
				tempDBTxn.AddAttribute("queries_passed", 0)
				tempDBTxn.AddAttribute("queries_failed", 1)
				tempDBTxn.NoticeError(tempDBErr)
			} else {
				tempDBTxn.AddAttribute("queries_passed", 1)
				tempDBTxn.AddAttribute("queries_failed", 0)
			}
			tempDBTxn.End()
		}
	} else {
		s.logger.Info("TempDB metrics scraping SKIPPED - EnableTempDBMetrics is false")
	}

	// Build final metrics using MetricsBuilder
	metrics := s.buildMetrics(ctx)

	// Calculate scrape duration for New Relic reporting
	scrapeDuration := time.Since(scrapeStartTime)
	metricsCollected := metrics.MetricCount()

	// Create summary transaction for New Relic
	if GlobalNRApp != nil {
		summaryTxn := GlobalNRApp.StartTransaction("sqlserver/summary")
		summaryTxn.AddAttribute("error_count", len(scrapeErrors))
		summaryTxn.AddAttribute("metrics_collected", metricsCollected)
		summaryTxn.AddAttribute("duration_ms", scrapeDuration.Milliseconds())
		if len(scrapeErrors) > 0 {
			for _, err := range scrapeErrors {
				summaryTxn.NoticeError(err)
			}
		}
		summaryTxn.End()
	}

	// Update global metrics for New Relic dashboard
	atomic.AddInt64(&TotalScrapeCount, 1)
	atomic.AddInt64(&TotalMetricsCollected, int64(metricsCollected))
	atomic.StoreInt64(&LastScrapeDurationMs, scrapeDuration.Milliseconds())

	// Log summary of scraping results
	if len(scrapeErrors) > 0 {
		s.logger.Warn("Completed scraping with errors",
			zap.Int("error_count", len(scrapeErrors)),
			zap.Int("metrics_collected", metricsCollected),
			zap.Duration("duration", scrapeDuration))

		// Return all errors combined as a PartialScrapeError with partial metrics
		return metrics, scrapererror.NewPartialScrapeError(multierr.Combine(scrapeErrors...), len(scrapeErrors))
	}

	s.logger.Debug("Successfully completed SQL Server metrics collection",
		zap.Int("metrics_collected", metricsCollected),
		zap.Duration("duration", scrapeDuration))

	return metrics, nil
}

// buildMetrics constructs the final metrics output with resource attributes.
// Sets server.address (hostname) and server.port as separate resource attributes
// following OpenTelemetry semantic conventions.
func (s *sqlServerScraper) buildMetrics(ctx context.Context) pmetric.Metrics {
	rb := s.mb.NewResourceBuilder()
	rb.SetServerAddress(s.config.Hostname) // server.address = hostname only (not hostname:port)
	rb.SetServerPort(s.config.Port)        // server.port = port number
	return s.mb.Emit(metadata.WithResource(rb.Emit()))
}

// Helper functions to safely extract values from pointers for logging
func getStringValueFromMap(ptr *string) string {
	if ptr != nil {
		return *ptr
	}
	return ""
}

func getIntValueFromMap(ptr *int) int {
	if ptr != nil {
		return *ptr
	}
	return 0
}

func getInt64ValueFromMap(ptr *int64) int64 {
	if ptr != nil {
		return *ptr
	}
	return 0
}

func getBoolValueFromMap(ptr *bool) bool {
	if ptr != nil {
		return *ptr
	}
	return false
}

// CollectSystemInformation retrieves comprehensive system and host information
// This information should be included as resource attributes with all metrics
func (s *sqlServerScraper) CollectSystemInformation(ctx context.Context) (*models.SystemInformation, error) {
	s.logger.Debug("Collecting SQL Server system and host information")

	var results []models.SystemInformation
	if err := s.connection.Query(ctx, &results, queries.SystemInformationQuery); err != nil {
		s.logger.Error("Failed to execute system information query",
			zap.Error(err),
			zap.String("query", queries.TruncateQuery(queries.SystemInformationQuery, 100)),
			zap.Int("engine_edition", s.engineEdition))
		return nil, fmt.Errorf("failed to execute system information query: %w", err)
	}

	if len(results) == 0 {
		s.logger.Warn("No results returned from system information query - SQL Server may not be ready")
		return nil, fmt.Errorf("no results returned from system information query")
	}

	if len(results) > 1 {
		s.logger.Warn("Multiple results returned from system information query",
			zap.Int("result_count", len(results)))
	}

	result := results[0]

	// Log collected system information for debugging
	s.logger.Info("Successfully collected system information",
		zap.String("server_name", getStringValueFromMap(result.ServerName)),
		zap.String("computer_name", getStringValueFromMap(result.ComputerName)),
		zap.String("edition", getStringValueFromMap(result.Edition)),
		zap.Int("engine_edition", getIntValueFromMap(result.EngineEdition)),
		zap.String("product_version", getStringValueFromMap(result.ProductVersion)),
		zap.Int("cpu_count", getIntValueFromMap(result.CPUCount)),
		zap.Int64("server_memory_kb", getInt64ValueFromMap(result.ServerMemoryKB)),
		zap.Bool("is_clustered", getBoolValueFromMap(result.IsClustered)),
		zap.Bool("is_hadr_enabled", getBoolValueFromMap(result.IsHadrEnabled)))

	return &result, nil
}
