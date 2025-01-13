//go:generate goversioninfo
package main

import (
	"fmt"
	"github.com/newrelic/infra-integrations-sdk/v3/data/metric"
	"github.com/newrelic/nri-mssql/src/queryanalysis/utils"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/newrelic/infra-integrations-sdk/v3/integration"
	"github.com/newrelic/infra-integrations-sdk/v3/log"
	"github.com/newrelic/nri-mssql/src/args"
	"github.com/newrelic/nri-mssql/src/connection"
	"github.com/newrelic/nri-mssql/src/instance"
	"github.com/newrelic/nri-mssql/src/inventory"
	"github.com/newrelic/nri-mssql/src/metrics"
	"github.com/newrelic/nri-mssql/src/queryanalysis"
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
	var args args.ArgumentList
	start := time.Now()
	// Create Integration
	i, err := integration.New(integrationName, integrationVersion, integration.Args(&args))
	if err != nil {
		log.Error(err.Error())
		os.Exit(1)
	}

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

	if err = i.Publish(); err != nil {
		log.Error(err.Error())
		return
	}
	i.Clear()

	if args.EnableQueryPerformance {
		queryanalysis.QueryPerformanceMain(i, args)
	}

	elapsed := time.Since(start)
	instanceEntity, err = instance.CreateInstanceEntity(i, con)
	metricSet := instanceEntity.NewMetricSet("MSSQLQueryPerformance")
	strValue := fmt.Sprintf("%v", elapsed) // Convert the value to a string representation
	metricType := utils.DetectMetricType(strValue)
	if metricType == metric.GAUGE {
		floatValue, err := strconv.ParseFloat(strValue, 64)
		err = metricSet.SetMetric("MSSQLTotalExecutionTimeStamp", floatValue, metric.GAUGE)
		if err != nil {
			log.Error("failed to set metric for key %s: %v", "MSSQLQueryPerformance", err)
		}
		if err != nil {
			log.Error("failed to parse float value for key %s: %v", "MSSQLQueryPerformance", err)
			return
		}
	} else {
		if err := metricSet.SetMetric("MSSQLTotalExecutionTimeStamp", strValue, metric.ATTRIBUTE); err != nil {
			log.Error("failed to set metric: %v", err)
		}
	}
	err = i.Publish()
	if err != nil {
		return
	}
	i.Clear()
	// Close connection when done
	defer con.Close()
}
