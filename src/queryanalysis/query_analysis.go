package queryanalysis

import (
	"github.com/newrelic/go-agent/v3/newrelic"
	"github.com/newrelic/infra-integrations-sdk/v3/integration"
	"github.com/newrelic/infra-integrations-sdk/v3/log"
	"github.com/newrelic/nri-mssql/src/args"
	"github.com/newrelic/nri-mssql/src/queryanalysis/connection"
	"github.com/newrelic/nri-mssql/src/queryanalysis/utils"
	"github.com/newrelic/nri-mssql/src/queryanalysis/validation"
)

// queryPerformanceMain runs all types of analyzes
func PopulateQueryPerformanceMetrics(integration *integration.Integration, arguments args.ArgumentList) {

	// Create an Application:
	app, err := newrelic.NewApplication(
		// Name your application
		newrelic.ConfigAppName("nri-mssql-perf-go-agent"),
		// Fill in your New Relic license key
		newrelic.ConfigLicense("4d5e50ac8ee18de886a411dedbef72ceFFFFNRAL"),
		// Add logging:
		newrelic.ConfigDebugLogger(os.Stdout),
		// Optional: add additional changes to your configuration via a config function:
		func(cfg *newrelic.Config) {
			cfg.CustomInsightsEvents.Enabled = true
		},
	)
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

	createConnectionTxn := app.StartTransaction("createSQLConnection")
	// Create a new connection
	log.Debug("Starting query analysis...")

	sqlConnection, err := connection.NewConnection(&arguments)
	if err != nil {
		log.Error("Error creating connection to SQL Server: %s", err.Error())
		createConnectionTxn.End()
		return
	}
	defer sqlConnection.Close()
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
			queryResults, err := utils.ExecuteQuery(arguments, queryDetailsDto, integration, sqlConnection)
			if err != nil {
				log.Error("Failed to execute query: %s", err)
				return err
			}
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
	log.Debug("Query analysis completed")
}
