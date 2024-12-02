package queryAnalysis

import (
	"fmt"
	"github.com/newrelic/nri-mssql/src/queryAnalysis/constants"
	"github.com/newrelic/nri-mssql/src/queryAnalysis/validation"
	"sync"

	"github.com/newrelic/infra-integrations-sdk/v3/integration"
	"github.com/newrelic/infra-integrations-sdk/v3/log"
	"github.com/newrelic/nri-mssql/src/args"
	"github.com/newrelic/nri-mssql/src/queryAnalysis/connection"
	"github.com/newrelic/nri-mssql/src/queryAnalysis/instance"
	"github.com/newrelic/nri-mssql/src/queryAnalysis/models"
	"github.com/newrelic/nri-mssql/src/queryAnalysis/queryHandler"
	"github.com/newrelic/nri-mssql/src/queryAnalysis/retryMechanism"
)

// RunAnalysis runs all types of analyses
func RunAnalysis(integration *integration.Integration, arguments args.ArgumentList) {

	fmt.Println("Starting query analysis...")

	// Create a new connection
	sqlConnection, err := connection.NewConnection(&arguments)
	if err != nil {
		log.Error("Error creating connection to SQL Server: %s", err.Error())
		return
	}

	// Validate preconditions
	_, err = validation.ValidatePreConditions(sqlConnection)
	if err != nil {
		log.Error("Error validating preconditions: %s", err.Error())
		return
	}

	// create a instanceEntity
	instanceEntity, err := instance.CreateInstanceEntity(integration, sqlConnection)
	if err != nil {
		log.Error("Error creating instance entity: %s", err.Error())
		return
	}

	var queryhandler queryhandler.QueryHandler = &queryhandler.QueryHandlerImpl{}
	var retryMechanism retryMechanism.RetryMechanism = &retryMechanism.RetryMechanismImpl{}

	queriesDetails, err := queryhandler.LoadQueries()
	if err != nil {
		log.Error("Error loading query configuration: %v", err)
		return
	}

	var wg sync.WaitGroup

	for _, queryDetailsDto := range queriesDetails {
		wg.Add(1)

		// Launch a goroutine for each queryDetailsDto
		go func(queryDetailsDto models.QueryDetailsDto) {
			defer wg.Done()

			err := retryMechanism.Retry(func() error {
				results, err := queryhandler.ExecuteQuery(sqlConnection.Connection, queryDetailsDto)
				if err != nil {
					log.Error("Failed to execute query: %s", err)
					return err
				}
				err = queryhandler.IngestQueryMetrics(instanceEntity, results, queryDetailsDto)
				if err != nil {
					log.Error("Failed to ingest metrics: %s", err)
					return err
				}

				if queryDetailsDto.Name == "MSSQLTopSlowQueries" {
					for _, result := range results {
						slowQuery, ok := result.(models.TopNSlowQueryDetails)
						if ok && slowQuery.QueryID != nil {
							newQueryDetails := models.QueryDetailsDto{
								Type:  "executionPlan",
								Name:  "MSSQLExecutionPlans",
								Query: fmt.Sprintf(constants.ExecutionPlanQueryTemplate, *slowQuery.QueryID),
							}
							queriesDetails = append(queriesDetails, newQueryDetails)
						} else {
							log.Error("Failed to cast result to models.TopNSlowQueryDetails or QueryID is nil")
						}
					}
				}

				return nil
			})

			if err != nil {
				log.Error("Failed after retries: %s", err)
			}
		}(queryDetailsDto) // Pass queryDetailsDto as a parameter to avoid closure capture issues
	}

	// Wait for all goroutines to complete
	wg.Wait()
}
