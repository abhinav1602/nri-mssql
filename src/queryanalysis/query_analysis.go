package queryanalysis

import (
	"github.com/newrelic/go-agent/v3/newrelic"
	"github.com/newrelic/infra-integrations-sdk/v3/integration"
	"github.com/newrelic/infra-integrations-sdk/v3/log"
	"github.com/newrelic/nri-mssql/src/args"
	"github.com/newrelic/nri-mssql/src/queryanalysis/connection"
	"github.com/newrelic/nri-mssql/src/queryanalysis/retrymechanism"
	"github.com/newrelic/nri-mssql/src/queryanalysis/utils"
	"github.com/newrelic/nri-mssql/src/queryanalysis/validation"
)

// queryPerformanceMain runs all types of analyzes
func QueryPerformanceMain(integration *integration.Integration, arguments args.ArgumentList, app *newrelic.Application) {

	createConnectionTxn := app.StartTransaction("createSQLConnection")
	// Create a new connection
	sqlConnection, err := connection.NewConnection(&arguments)
	if err != nil {
		log.Error("Error creating connection to SQL Server: %s", err.Error())
		createConnectionTxn.End()
		return
	}
	createConnectionTxn.End()

	validatePreConditiontxn := app.StartTransaction("validatingPreCondition")
	// Validate preconditions
	isPreconditionPassed := validation.ValidatePreConditions(sqlConnection)
	if !isPreconditionPassed {
		log.Error("Error validating preconditions")
		validatePreConditiontxn.End()
		return
	}
	validatePreConditiontxn.End()

	utils.ValidateAndSetDefaults(&arguments)

	var retryMechanism retrymechanism.RetryMechanism = &retrymechanism.RetryMechanismImpl{}

	loadQueriesTxn := app.StartTransaction("loadQueries")
	queryDetails, err := utils.LoadQueries(arguments)
	if err != nil {
		log.Error("Error loading query configuration: %v", err)
		loadQueriesTxn.End()
		return
	}
	loadQueriesTxn.End()

	for _, queryDetailsDto := range queryDetails {
		err := retryMechanism.Retry(func() error {
			executeAndBindModelTxn := app.StartTransaction("ExecuteQueriesAndBindModels")
			queryResults, err := utils.ExecuteQuery(arguments, queryDetailsDto, integration, sqlConnection, executeAndBindModelTxn)
			if err != nil {
				log.Error("Failed to execute query: %s", err)
				executeAndBindModelTxn.End()
				return err
			}
			executeAndBindModelTxn.End()

			dataInjestionTxn := app.StartTransaction("IngestDataInBatches")
			err = utils.IngestQueryMetricsInBatches(queryResults, queryDetailsDto, integration, sqlConnection)
			if err != nil {
				log.Error("Failed to ingest metrics: %s", err)
				dataInjestionTxn.End()
				return err
			}
			dataInjestionTxn.End()
			return nil
		})
		if err != nil {
			log.Error("Failed after retries: %s", err)
		}
	}

	//
	//var allCustomQueryResults []models.CustomQueryResults
	//
	//// Create a channel to gather the results from each goroutine safely
	//resultsChannel := make(chan models.CustomQueryResults)
	//
	//// Use a WaitGroup to ensure all goroutines finish before moving on
	//var wg sync.WaitGroup
	//
	//for _, queryDetailsDto := range queryDetails {
	//	wg.Add(1)
	//
	//	// Launch a goroutine to handle each query execution
	//	go func(queryDetailsDto models.QueryDetailsDto) {
	//		defer wg.Done()
	//
	//		err := retryMechanism.Retry(func() error {
	//			executeAndBindModelTxn := app.StartTransaction("ExecuteQueriesAndBindModels")
	//			queryResults, err := utils.ExecuteQuery(arguments, queryDetailsDto, integration, sqlConnection, executeAndBindModelTxn)
	//			if err != nil {
	//				log.Error("Failed to execute query: %s", err)
	//				executeAndBindModelTxn.End()
	//				return err
	//			}
	//			executeAndBindModelTxn.End()
	//
	//			var customQueryResults models.CustomQueryResults
	//			customQueryResults.QueryDetailsDto = queryDetailsDto
	//			customQueryResults.Result = queryResults
	//
	//			// Send the results to the channel
	//			resultsChannel <- customQueryResults
	//
	//			return nil
	//		})
	//
	//		if err != nil {
	//			log.Error("Failed after retries: %s", err)
	//		}
	//	}(queryDetailsDto)
	//}
	//
	//// Goroutine to close the resultsChannel once all queries are completed
	//go func() {
	//	wg.Wait()
	//	close(resultsChannel)
	//}()
	//
	//// Collect all results from the resultsChannel
	//for result := range resultsChannel {
	//	allCustomQueryResults = append(allCustomQueryResults, result)
	//}
	//
	//
	//for _, customQueryResults := range allCustomQueryResults {
	//	dataInjestionTxn := app.StartTransaction("IngestDataInBatches")
	//	err = utils.IngestQueryMetricsInBatches(customQueryResults.Result, customQueryResults.QueryDetailsDto, integration, sqlConnection)
	//	if err != nil {
	//		log.Error("Failed to ingest metrics: %s", err)
	//		dataInjestionTxn.End()
	//	}
	//	dataInjestionTxn.End()
	//}

	defer sqlConnection.Close()

}
