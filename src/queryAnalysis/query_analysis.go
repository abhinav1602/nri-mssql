package queryAnalysis

import (
	"fmt"
	"github.com/newrelic/infra-integrations-sdk/v3/integration"
	"github.com/newrelic/infra-integrations-sdk/v3/log"
	"github.com/newrelic/nri-mssql/src/args"
	"github.com/newrelic/nri-mssql/src/queryAnalysis/connection"
	"github.com/newrelic/nri-mssql/src/queryAnalysis/retryMechanism"
	"github.com/newrelic/nri-mssql/src/queryAnalysis/validation"
)

// queryPerformanceMain runs all types of analyses
func QueryPerformanceMain(integration *integration.Integration, arguments args.ArgumentList) {

	fmt.Println("Starting query analysis...")

	// Create a new connection
	sqlConnection, err := connection.NewConnection(&arguments)
	if err != nil {
		log.Error("Error creating connection to SQL Server: %s", err.Error())
		return
	}

	// Validate preconditions
	err = validation.ValidatePreConditions(sqlConnection)
	if err != nil {
		log.Error("Error validating preconditions: %s", err.Error())
		return // Abort further operations if validations fail
	}

	var retryMechanism retryMechanism.RetryMechanism = &retryMechanism.RetryMechanismImpl{}

	queryDetails, err := LoadQueries()
	if err != nil {
		log.Error("Error loading query configuration: %v", err)
		return
	}

	for _, queryDetailsDto := range queryDetails {
		err := retryMechanism.Retry(func() error {
			queryResults, err := ExecuteQuery(queryDetailsDto, integration, sqlConnection)
			if err != nil {
				log.Error("Failed to execute query: %s", err)
				return err
			}
			// Anonymize query results
			err = IngestQueryMetricsInBatches(queryResults, queryDetailsDto, integration, sqlConnection)
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

	//var wg sync.WaitGroup
	//
	//for _, queryDetailsDto := range queryDetails {
	//	wg.Add(1)
	//
	//	// Launch a goroutine for each queryDetailsDto
	//	go func(queryDetailsDto models.QueryDetailsDto) {
	//		defer wg.Done()
	//
	//		err := retryMechanism.Retry(func() error {
	//			queryResults, err := ExecuteQuery(queryDetailsDto, integration, sqlConnection)
	//			if err != nil {
	//				log.Error("Failed to execute query: %s", err)
	//				return err
	//			}
	//			// Anonymize query results
	//			err = IngestQueryMetricsInBatches(queryResults, queryDetailsDto, integration, sqlConnection)
	//			if err != nil {
	//				log.Error("Failed to ingest metrics: %s", err)
	//				return err
	//			}
	//			return nil
	//		})
	//
	//		if err != nil {
	//			log.Error("Failed after retries: %s", err)
	//		}
	//	}(queryDetailsDto) // Pass queryDetailsDto as a parameter to avoid closure capture issues
	//}
	//
	//// Wait for all goroutines to complete
	//wg.Wait()
}
