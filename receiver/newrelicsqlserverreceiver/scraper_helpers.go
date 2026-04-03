// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package newrelicsqlserverreceiver

import (
	"context"
	"time"

	"github.com/newrelic/go-agent/v3/newrelic"
	"go.uber.org/zap"
)

// scrapeFunc is a function that performs a scraping operation
type scrapeFunc func(ctx context.Context) error

// executeScrape executes a scraping function with timeout, error handling, and logging
// This helper eliminates the repeated pattern of context creation, error handling, and logging
func (s *sqlServerScraper) executeScrape(ctx context.Context, metricName string, fn scrapeFunc) error {
	// Create context with timeout
	scrapeCtx, cancel := context.WithTimeout(ctx, s.config.Timeout)
	defer cancel()

	// Execute the scraping function
	if err := fn(scrapeCtx); err != nil {
		s.logger.Error("Failed to scrape "+metricName,
			zap.Error(err),
			zap.Duration("timeout", s.config.Timeout))
		return err
	}

	s.logger.Debug("Successfully scraped " + metricName)
	return nil
}

// executeConditionalScrape executes a scraping function only if the condition is true
// This helper handles the common pattern of conditional scraping with logging
func (s *sqlServerScraper) executeConditionalScrape(
	ctx context.Context,
	condition bool,
	metricName string,
	fn scrapeFunc,
) error {
	if !condition {
		s.logger.Debug(metricName + " scraping SKIPPED - feature flag disabled")
		return nil
	}

	s.logger.Debug("Starting " + metricName + " scraping")
	return s.executeScrape(ctx, metricName, fn)
}

// collectErrors appends a non-nil error to the error slice
// This helper simplifies error collection
func collectErrors(errors []error, err error) []error {
	if err != nil {
		return append(errors, err)
	}
	return errors
}

// scrapeWithTimeout is a lower-level helper that creates a context with timeout
// and executes a function, returning any error
func scrapeWithTimeout(ctx context.Context, timeout time.Duration, fn scrapeFunc) error {
	scrapeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return fn(scrapeCtx)
}

// scrapeResult encapsulates the result of a scraping operation for concurrent execution
type scrapeResult struct {
	MetricName string
	Error      error
}

// concurrentScrape executes multiple scraping functions concurrently
// Returns a slice of errors from all scraping operations
func (s *sqlServerScraper) concurrentScrape(ctx context.Context, scrapers map[string]scrapeFunc) []error {
	results := make(chan scrapeResult, len(scrapers))
	var errors []error

	// Launch all scrapers concurrently
	for name, fn := range scrapers {
		go func(metricName string, scraper scrapeFunc) {
			// Create a goroutine-safe copy of the transaction context
			// This prevents segment ordering issues when running concurrent queries
			goroutineCtx := s.newGoroutineContext(ctx)

			err := s.executeScrape(goroutineCtx, metricName, scraper)
			results <- scrapeResult{
				MetricName: metricName,
				Error:      err,
			}
		}(name, fn)
	}

	// Collect results
	for i := 0; i < len(scrapers); i++ {
		result := <-results
		if result.Error != nil {
			errors = append(errors, result.Error)
		}
	}

	return errors
}

// healthCheck performs a connection health check before scraping
// Returns an error if the health check fails
func (s *sqlServerScraper) healthCheck(ctx context.Context) error {
	if s.connection == nil {
		s.logger.Error("No database connection available for scraping")
		return nil // Return nil to allow scraping to continue with partial results
	}

	if err := s.connection.Ping(ctx); err != nil {
		s.logger.Error("Connection health check failed before scraping", zap.Error(err))
		return err // Return error but don't stop scraping
	}

	return nil
}

// refreshMetadataCache refreshes the metadata cache if enabled
// Logs warnings but doesn't fail the scraping operation
func (s *sqlServerScraper) refreshMetadataCache(ctx context.Context) {
	if !s.config.EnableWaitResourceEnrichment || s.metadataCache == nil {
		return
	}

	if err := s.metadataCache.Refresh(ctx); err != nil {
		s.logger.Warn("Failed to refresh metadata cache", zap.Error(err))
		// Continue scraping - stale cache is better than no data
	}
}

// newGoroutineContext creates a goroutine-safe copy of the transaction context
// This is required when using New Relic transactions across multiple goroutines
// to prevent segment ordering violations
func (s *sqlServerScraper) newGoroutineContext(ctx context.Context) context.Context {
	txn := newrelic.FromContext(ctx)
	if txn == nil {
		// No transaction in context, return original context
		return ctx
	}

	// Create a new goroutine-safe transaction and inject it into a new context
	// This allows each goroutine to end segments independently
	goroutineTxn := txn.NewGoroutine()
	return newrelic.NewContext(ctx, goroutineTxn)
}
