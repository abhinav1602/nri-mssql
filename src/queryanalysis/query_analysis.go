package queryanalysis

import (
	"fmt"
	"github.com/newrelic/infra-integrations-sdk/v3/data/metric"
	"github.com/newrelic/infra-integrations-sdk/v3/integration"
	"github.com/newrelic/infra-integrations-sdk/v3/log"
	"github.com/newrelic/nri-mssql/src/args"
	"github.com/newrelic/nri-mssql/src/queryanalysis/connection"
	"github.com/newrelic/nri-mssql/src/queryanalysis/instance"
	"github.com/newrelic/nri-mssql/src/queryanalysis/retrymechanism"
	"github.com/newrelic/nri-mssql/src/queryanalysis/utils"
	"github.com/newrelic/nri-mssql/src/queryanalysis/validation"
	"strconv"
	"time"
)

// queryPerformanceMain runs all types of analyzes
func QueryPerformanceMain(integration *integration.Integration, arguments args.ArgumentList) {

	// Create a new connection
	sqlConnection, err := connection.NewConnection(&arguments)
	if err != nil {
		log.Error("Error creating connection to SQL Server: %s", err.Error())
		return
	}

	// Validate preconditions
	isPreconditionPassed := validation.ValidatePreConditions(sqlConnection)
	if !isPreconditionPassed {
		log.Error("Error validating preconditions")
		return
	}

	utils.ValidateAndSetDefaults(&arguments)

	var retryMechanism retrymechanism.RetryMechanism = &retrymechanism.RetryMechanismImpl{}

	queryDetails, err := utils.LoadQueries(arguments)
	if err != nil {
		log.Error("Error loading query configuration: %v", err)
		return
	}

	for _, queryDetailsDto := range queryDetails {
		err := retryMechanism.Retry(func() error {
			queryExecutionStart := time.Now()
			queryResults, err := utils.ExecuteQuery(arguments, queryDetailsDto, integration, sqlConnection)
			if err != nil {
				log.Error("Failed to execute query: %s", err)
				return err
			}

			elapsed := time.Since(queryExecutionStart)
			instanceEntity, err := instance.CreateInstanceEntity(integration, sqlConnection)
			metricSet := instanceEntity.NewMetricSet("MSSQLQueryPerformance")
			strValue := fmt.Sprintf("%v", elapsed) // Convert the value to a string representation
			metricType := utils.DetectMetricType(strValue)

			if metricType == metric.GAUGE {
				floatValue, err := strconv.ParseFloat(strValue, 64)
				if queryDetailsDto.Name == "MSSQLTopSlowQueries" {
					err = metricSet.SetMetric("MSSQLTopSlowQueriesExecutionTimeStamp", floatValue, metric.GAUGE)
					if err != nil {
						log.Error("failed to set metric for key %s: %v", "MSSQLQueryPerformance", err)
					}
				} else if queryDetailsDto.Name == "MSSQLWaitTimeAnalysis" {
					err = metricSet.SetMetric("MSSQLWaitTimeAnalysisTimeStamp", floatValue, metric.GAUGE)
					if err != nil {
						log.Error("failed to set metric for key %s: %v", "MSSQLQueryPerformance", err)
					}
				} else {
					err = metricSet.SetMetric("MSSQLBlockingSessionQueriesTimeStamp", floatValue, metric.GAUGE)
					if err != nil {
						log.Error("failed to set metric for key %s: %v", "MSSQLQueryPerformance", err)
					}
				}
			} else {
				if queryDetailsDto.Name == "MSSQLTopSlowQueries" {
					if err := metricSet.SetMetric("MSSQLTopSlowQueriesTotalExecutionTimeStamp", strValue, metric.ATTRIBUTE); err != nil {
						log.Error("failed to set metric for key %s: %v", "MSSQLQueryPerformance", err)
					}
				} else if queryDetailsDto.Name == "MSSQLWaitTimeAnalysis" {
					if err := metricSet.SetMetric("MSSQLWaitTimeAnalysisTotalExecutionTimeStamp", strValue, metric.ATTRIBUTE); err != nil {
						log.Error("failed to set metric for key %s: %v", "MSSQLQueryPerformance", err)
					}
				} else {
					if err := metricSet.SetMetric("MSSQLBlockingSessionQueriesTotalExecutionTimeStamp", strValue, metric.ATTRIBUTE); err != nil {
						log.Error("failed to set metric for key %s: %v", "MSSQLQueryPerformance", err)
					}
				}
			}

			err = integration.Publish()
			if err != nil {
				log.Error("Faild to publish", err)
			}
			integration.Clear()

			err = utils.IngestQueryMetricsInBatches(queryResults, queryDetailsDto, integration, sqlConnection)
			if err != nil {
				log.Error("Failed to ingest metrics: %s", err)
				return err
			}
			return nil
		})
		if err != nil {
			log.Error("Failed after retries: %s", err)
		}
	}
}
