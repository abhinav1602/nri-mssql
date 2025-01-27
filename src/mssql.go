//go:generate goversioninfo
package main

import (
	"fmt"
	"github.com/newrelic/go-agent/v3/newrelic"
	"github.com/newrelic/infra-integrations-sdk/v3/integration"
	"github.com/newrelic/infra-integrations-sdk/v3/log"
	"github.com/newrelic/nri-mssql/src/queryanalysis"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/newrelic/nri-mssql/src/args"
	"github.com/newrelic/nri-mssql/src/connection"
	"github.com/newrelic/nri-mssql/src/instance"
	"github.com/newrelic/nri-mssql/src/inventory"
	"github.com/newrelic/nri-mssql/src/metrics"
)

const (
	integrationName = "com.newrelic.mssql"
)

var (
	integrationVersion = "0.0.0"
	gitCommit          = ""
	buildDate          = ""
)

func main() {

	// If an application could not be created then err will reveal why.
	if err != nil {
		log.Debug("unable to create New Relic Application", err)
		return
	}
	defer app.Shutdown(10 * time.Second) // Use the app variable

	// Ensure the application is connected
	if err := app.WaitForConnection(10 * time.Second); err != nil {
		log.Debug("New Relic Application did not connect:", err)
		return
	}

	totalNriMsSqlExecutionTime := app.StartTransaction("TotalNriMsSqlExecutionTime")

	var args args.ArgumentList
	// Create Integration
	i, err := integration.New(integrationName, integrationVersion, integration.Args(&args))
	if err != nil {
		log.Error(err.Error())
		os.Exit(1)
	}

	// Create an Application:
	app, err := newrelic.NewApplication(
		// Name your application
		newrelic.ConfigAppName(args.APPLICATION_NAME),
		// Fill in your New Relic license key
		newrelic.ConfigLicense(args.YOUR_LICENSE_KEY),
		// Add logging:
		newrelic.ConfigDebugLogger(os.Stdout),
		// Optional: add additional changes to your configuration via a config function:
		func(cfg *newrelic.Config) {
			cfg.CustomInsightsEvents.Enabled = true
		},
	)

	if args.ShowVersion {
		fmt.Printf(
			"New Relic %s integration Version: %s, Platform: %s, GoVersion: %s, GitCommit: %s, BuildDate: %s\n",
			strings.Title(strings.Replace(integrationName, "com.newrelic.", "", 1)),
			integrationVersion,
			fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH),
			runtime.Version(),
			gitCommit,
			buildDate)
		os.Exit(0)
	}

	// Setup logging with verbose
	log.SetupLogging(args.Verbose)

	// Validate arguments
	if err := args.Validate(); err != nil {
		log.Error("Configuration error: %s", err)
		os.Exit(1)
	}

	// Create a new connection
	con, err := connection.NewConnection(&args)
	if err != nil {
		log.Error("Error creating connection to SQL Server: %s", err.Error())
		os.Exit(1)
	}

	// Create the entity for the instance
	instanceEntity, err := instance.CreateInstanceEntity(i, con)
	if err != nil {
		log.Error("Unable to create entity for instance: %s", err.Error())
		os.Exit(1)
	}

	// Inventory collection
	if args.HasInventory() {
		inventory.PopulateInventory(instanceEntity, con)
	}

	// Metric collection
	if args.HasMetrics() {
		if err := metrics.PopulateDatabaseMetrics(i, instanceEntity.Metadata.Name, con, args); err != nil {
			log.Error("Error collecting metrics for databases: %s", err.Error())
		}

		metrics.PopulateInstanceMetrics(instanceEntity, con, args)
	}

	// Close connection when done
	defer con.Close()

	if err = i.Publish(); err != nil {
		log.Error(err.Error())
		return
	}

	if args.EnableQueryPerformance {
		totalNriMsSqlQueryPerformanceExecutionTime := app.StartTransaction("TotalNriMsSqlQueryPerformanceExecutionTime")
		queryanalysis.QueryPerformanceMain(i, args, app)
		totalNriMsSqlQueryPerformanceExecutionTime.End()
	}

	totalNriMsSqlExecutionTime.End()
}
