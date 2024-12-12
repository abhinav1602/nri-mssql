package queryAnalysis

import (
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/newrelic/infra-integrations-sdk/v3/data/metric"
	"github.com/newrelic/infra-integrations-sdk/v3/integration"
	"github.com/newrelic/infra-integrations-sdk/v3/log"
	"github.com/newrelic/nri-mssql/src/queryAnalysis/config"
	"github.com/newrelic/nri-mssql/src/queryAnalysis/connection"
	"github.com/newrelic/nri-mssql/src/queryAnalysis/instance"
	"github.com/newrelic/nri-mssql/src/queryAnalysis/models"
	"regexp"
	"strconv"
)

//go:embed config/queries.json
var queriesJSON []byte

func LoadQueries() ([]models.QueryDetailsDto, error) {
	var queries []models.QueryDetailsDto
	if err := json.Unmarshal(queriesJSON, &queries); err != nil {
		return nil, fmt.Errorf("failed to unmarshal queries configuration: %w", err)
	}
	return queries, nil
}

func ExecuteQuery(queryDetailsDto models.QueryDetailsDto, integration *integration.Integration, sqlConnection *connection.SQLConnection) ([]interface{}, error) {
	fmt.Println("Executing query...", queryDetailsDto.Name)

	rows, err := sqlConnection.Connection.Queryx(queryDetailsDto.Query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	return BindQueryResults(rows, queryDetailsDto, integration, sqlConnection)
}

// BindQueryResults binds query results to the specified data model using `sqlx`
func BindQueryResults(rows *sqlx.Rows, queryDetailsDto models.QueryDetailsDto, integration *integration.Integration, sqlConnection *connection.SQLConnection) ([]interface{}, error) {
	defer rows.Close()

	results := make([]interface{}, 0)

	for rows.Next() {
		switch queryDetailsDto.Type {
		case "slowQueries":
			var model models.TopNSlowQueryDetailsReceiver
			if err := rows.StructScan(&model); err != nil {
				fmt.Println("Could not scan row: ", err)
				continue
			}
			var queryId = "0x" + hex.EncodeToString(*model.QueryID)
			AnonymizeQueryText(model.QueryText)
			var modelIngestor models.TopNSlowQueryDetailsIngector
			modelIngestor.QueryID = &queryId
			modelIngestor.QueryText = model.QueryText
			modelIngestor.DatabaseName = model.DatabaseName
			modelIngestor.SchemaName = model.SchemaName
			modelIngestor.LastExecutionTimestamp = model.LastExecutionTimestamp
			modelIngestor.ExecutionCount = model.ExecutionCount
			modelIngestor.AvgCPUTimeMS = model.AvgCPUTimeMS
			modelIngestor.AvgElapsedTimeMS = model.AvgElapsedTimeMS
			modelIngestor.AvgDiskReads = model.AvgDiskReads
			modelIngestor.AvgDiskWrites = model.AvgDiskWrites
			modelIngestor.StatementType = model.StatementType
			modelIngestor.CollectionTimestamp = model.CollectionTimestamp
			results = append(results, modelIngestor)

			// fetch and generate execution plan
			//GenerateAndInjestExecutionPlan(queryId, integration, sqlConnection)
		case "waitAnalysis":
			var model models.WaitTimeAnalysisReceiver
			if err := rows.StructScan(&model); err != nil {
				fmt.Println("Could not scan row: ", err)
				continue
			}
			AnonymizeQueryText(model.QueryText)
			var queryId = "0x" + hex.EncodeToString(*model.QueryID)
			var modelIngestor models.WaitTimeAnalysisIngestor
			modelIngestor.QueryID = &queryId
			modelIngestor.QueryText = model.QueryText
			modelIngestor.DatabaseName = model.DatabaseName
			modelIngestor.CustomQueryType = model.CustomQueryType
			modelIngestor.WaitCategory = model.WaitCategory
			modelIngestor.TotalWaitTimeMs = model.TotalWaitTimeMs
			modelIngestor.AvgWaitTimeMs = model.AvgWaitTimeMs
			modelIngestor.WaitEventCount = model.WaitEventCount
			modelIngestor.CollectionTimestamp = model.CollectionTimestamp
			results = append(results, modelIngestor)
		case "blockingSessions":
			var model models.BlockingSessionQueryDetails
			if err := rows.StructScan(&model); err != nil {
				fmt.Println("Could not scan row: ", err)
				continue
			}
			AnonymizeQueryText(model.BlockedQueryText)
			AnonymizeQueryText(model.BlockingQueryText)
			results = append(results, model)
		default:
			return nil, fmt.Errorf("unknown query type: %s", queryDetailsDto.Type)
		}
	}
	return results, nil

}

func GenerateAndInjestExecutionPlan(queryId string, integration *integration.Integration, sqlConnection *connection.SQLConnection) {
	hexQueryId := fmt.Sprintf("%s", queryId)
	executionPlanQuery := fmt.Sprintf(config.ExecutionPlanQueryTemplate, hexQueryId)

	var model models.ExecutionPlanResult

	rows, err := sqlConnection.Connection.Queryx(executionPlanQuery)
	if err != nil {
		log.Error("Failed to execute query: %s", err)
		return
	}
	defer rows.Close()

	results := make([]interface{}, 0)

	for rows.Next() {
		if err := rows.StructScan(&model); err != nil {
			log.Error("Could not scan row: %s", err)
			return
		}
		AnonymizeQueryText(model.SQLText)
		results = append(results, model)
	}

	queryDetailsDto := models.QueryDetailsDto{
		Name:  "MSSQLQueryExecutionPlans",
		Query: "",
		Type:  "executionPlan",
	}

	//Ingest the execution plan
	if err := IngestQueryMetricsInBatches(results, queryDetailsDto, integration, sqlConnection); err != nil {
		log.Error("Failed to ingest execution plan: %s", err)
	}
}

// IngestQueryMetrics processes and ingests query metrics into the New Relic entity
func IngestQueryMetrics(results []interface{}, queryDetailsDto models.QueryDetailsDto, integration *integration.Integration, sqlConnection *connection.SQLConnection) error {

	instanceEntity, err := instance.CreateInstanceEntity(integration, sqlConnection)

	for _, result := range results {
		// Convert the result into a map[string]interface{} for dynamic key-value access
		var resultMap map[string]interface{}
		data, err := json.Marshal(result)
		if err != nil {
			return fmt.Errorf("error marshalling to JSON: %w", err)
		}
		err = json.Unmarshal(data, &resultMap)
		if err != nil {
			return fmt.Errorf("error unmarshalling to map: %w", err)
		}

		// Create a new metric set with the query name
		metricSet := instanceEntity.NewMetricSet(queryDetailsDto.Name)

		// Iterate over the map and add each key-value pair as a metric
		for key, value := range resultMap {
			strValue := fmt.Sprintf("%v", value) // Convert the value to a string representation
			metricType := DetectMetricType(strValue)
			if metricType == metric.GAUGE {
				if floatValue, err := strconv.ParseFloat(strValue, 64); err == nil {
					metricSet.SetMetric(key, floatValue, metric.GAUGE)
				}
			} else {
				metricSet.SetMetric(key, strValue, metric.ATTRIBUTE)
			}
		}
	}
	err = integration.Publish()
	if err != nil {
		return err
	}
	integration.Clear()

	return nil
}

func IngestQueryMetricsInBatches(results []interface{}, queryDetailsDto models.QueryDetailsDto, integration *integration.Integration, sqlConnection *connection.SQLConnection) error {
	const batchSize = 10

	for start := 0; start < len(results); start += batchSize {
		end := start + batchSize
		if end > len(results) {
			end = len(results)
		}

		batchResult := results[start:end]
		fmt.Printf("Processing batch of %s: startIndex: %d to endIndex: %d totalLength: %d \n", queryDetailsDto.Name, start, end, len(results))

		if err := IngestQueryMetrics(batchResult, queryDetailsDto, integration, sqlConnection); err != nil {
			return fmt.Errorf("error ingesting batch from %d to %d: %w", start, end, err)
		}
	}

	return nil
}

func DetectMetricType(value string) metric.SourceType {
	if _, err := strconv.ParseFloat(value, 64); err != nil {
		return metric.ATTRIBUTE
	}

	return metric.GAUGE
}

func AnonymizeQueryText(query *string) {

	re := regexp.MustCompile(`'[^']*'|\d+|".*?"`)

	anonymizedQuery := re.ReplaceAllString(*query, "?")

	*query = anonymizedQuery
}
