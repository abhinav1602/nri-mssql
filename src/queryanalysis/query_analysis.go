package queryanalysis

import (
	"github.com/newrelic/go-agent/v3/newrelic"
	"github.com/newrelic/infra-integrations-sdk/v3/integration"
	"github.com/newrelic/infra-integrations-sdk/v3/log"
	"github.com/newrelic/nri-mssql/src/args"
	"github.com/newrelic/nri-mssql/src/connection"
	"github.com/newrelic/nri-mssql/src/queryanalysis/config"
	"github.com/newrelic/nri-mssql/src/queryanalysis/utils"
	"github.com/newrelic/nri-mssql/src/queryanalysis/validation"
)

// queryPerformanceMain runs all types of analyzes
func PopulateQueryPerformanceMetrics(integration *integration.Integration, arguments args.ArgumentList, app *newrelic.Application) {
	createConnectionTxn := app.StartTransaction("createSQLConnection")
	// Create a new connection
	log.Debug("Starting query analysis...")

	sqlConnection, err := connection.NewConnection(&arguments)
	if err != nil {
		log.Error("Error creating connection to SQL Server: %s", err.Error())
		return
	}
	defer sqlConnection.Close()
	createConnectionTxn.End()

	validatePreConditiontxn := app.StartTransaction("validatingPreCondition")
	// Validate preconditions
	isPreconditionPassed := validation.ValidatePreConditions(sqlConnection)
	if !isPreconditionPassed {
		log.Error("Error validating preconditions")
		return
	}
	validatePreConditiontxn.End()
	utils.ValidateAndSetDefaults(&arguments)

	loadQueriesTxn := app.StartTransaction("loadQueries")
	queries := config.Queries
	queryDetails, err := utils.LoadQueries(queries, arguments)
	if err != nil {
		log.Error("Error loading query configuration: %v", err)
		return
	}
	loadQueriesTxn.End()

	for _, queryDetailsDto := range queryDetails {
		executeAndBindModelTxn := app.StartTransaction("ExecuteQueriesAndBindModels")

		queryResults, err := utils.ExecuteQuery(arguments, queryDetailsDto, integration, sqlConnection, executeAndBindModelTxn)
		if err != nil {
			log.Error("Failed to execute query: %s", err)
			continue
		}
		executeAndBindModelTxn.End()

		dataInjestionTxn := app.StartTransaction("IngestDataInBatches")
		err = utils.IngestQueryMetricsInBatches(queryResults, queryDetailsDto, integration, sqlConnection)
		if err != nil {
			log.Error("Failed to ingest metrics: %s", err)
			continue
		}
		dataInjestionTxn.End()

	}
	log.Debug("Query analysis completed")
}
