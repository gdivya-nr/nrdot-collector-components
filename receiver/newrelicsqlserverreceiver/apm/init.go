package apm

import (
	"log"
	"os"
	"sync"

	"github.com/newrelic/go-agent/v3/newrelic"
)

var (
	// App is the New Relic application instance
	App *newrelic.Application

	// Ensure initialization happens only once
	initOnce sync.Once
)

// Initialize initializes the New Relic Go agent (safe to call multiple times)
// Returns the initialized app instance so callers can set it where needed
func Initialize() (*newrelic.Application, error) {
	var err error

	initOnce.Do(func() {
		App, err = initNewRelic()
	})

	return App, err
}

// initNewRelic initializes the New Relic Go agent
func initNewRelic() (*newrelic.Application, error) {
	// Get configuration from environment variables
	appName := os.Getenv("NEW_RELIC_APP_NAME")

	if appName == "" {
		appName = "nrdotcol-collector" // Default name
	}

	licenseKey := os.Getenv("NEW_RELIC_LICENSE_KEY")
	if licenseKey == "" {
		log.Println("Warning: NEW_RELIC_LICENSE_KEY not set, New Relic monitoring disabled")
		return nil, nil
	}

	// Check if using staging environment
	host := "collector.newrelic.com" // Default production
	if os.Getenv("NEW_RELIC_HOST") != "" {
		host = os.Getenv("NEW_RELIC_HOST")
	}

	app, err := newrelic.NewApplication(
		newrelic.ConfigAppName(appName),
		newrelic.ConfigLicense(licenseKey),
		newrelic.ConfigAppLogForwardingEnabled(true),
		newrelic.ConfigDistributedTracerEnabled(true),
		newrelic.ConfigCodeLevelMetricsEnabled(true), // Enable call stack / code-level metrics
		func(cfg *newrelic.Config) {
			cfg.Host = host
			cfg.Logger = newrelic.NewDebugLogger(os.Stdout) // Enable debug logging
		},
	)

	if err != nil {
		return nil, err
	}

	log.Printf("New Relic APM agent initialized successfully for app: %s, sending to host: %s", appName, host)

	return app, nil
}

// Shutdown gracefully shuts down the New Relic agent
func Shutdown() {
	if App != nil {
		App.Shutdown(10000) // 10 second timeout
	}
}
