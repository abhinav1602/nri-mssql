package utils

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/newrelic/go-agent/v3/newrelic"
	"regexp"
	"strconv"

	"github.com/jmoiron/sqlx"
	"github.com/newrelic/infra-integrations-sdk/v3/data/metric"
	"github.com/newrelic/infra-integrations-sdk/v3/integration"
	"github.com/newrelic/infra-integrations-sdk/v3/log"

	"github.com/newrelic/nri-mssql/src/args"
	"github.com/newrelic/nri-mssql/src/queryanalysis/config"
	"github.com/newrelic/nri-mssql/src/queryanalysis/connection"
	"github.com/newrelic/nri-mssql/src/queryanalysis/instance"
	"github.com/newrelic/nri-mssql/src/queryanalysis/models"
)

var (
	ErrUnknownQueryType       = errors.New("unknown query type")
	ErrCreatingInstanceEntity = errors.New("error creating instance entity")
)

func LoadQueries(arguments args.ArgumentList) ([]models.QueryDetailsDto, error) {
	queries := config.Queries

	for i := range queries {
		switch queries[i].Type {
		case "slowQueries":
			queries[i].Query = fmt.Sprintf(queries[i].Query, arguments.FetchInterval, arguments.QueryCountThreshold,
				arguments.QueryResponseTimeThreshold, config.TextTruncateLimit)
		case "waitAnalysis":
			queries[i].Query = fmt.Sprintf(queries[i].Query, arguments.QueryCountThreshold, config.TextTruncateLimit)
		case "blockingSessions":
			queries[i].Query = fmt.Sprintf(queries[i].Query, arguments.QueryCountThreshold, config.TextTruncateLimit)
		default:
			fmt.Println("Unknown query type:", queries[i].Type)
		}
	}

	return queries, nil
}

func ExecuteQuery(arguments args.ArgumentList, queryDetailsDto models.QueryDetailsDto, integration *integration.Integration, sqlConnection *connection.SQLConnection, executeAndBindTransaction *newrelic.Transaction) ([]interface{}, error) {
	BindQuerySegment := newrelic.DatastoreSegment{
		StartTime: executeAndBindTransaction.StartSegmentNow(),
		// Product is the datastore type.  See the constants in
		// https://github.com/newrelic/go-agent/blob/master/v3/newrelic/datastore.go.  Product
		// is one of the fields primarily responsible for the grouping of Datastore
		// metrics.
		Product: newrelic.DatastoreMSSQL,
		// Collection is the table or group being operated upon in the datastore,
		// e.g. "users_table".  This becomes the db.collection attribute on Span
		// events and Transaction Trace segments.  Collection is one of the fields
		// primarily responsible for the grouping of Datastore metrics.
		Collection: "executingQuery " + queryDetailsDto.Name,
		// Operation is the relevant action, e.g. "SELECT" or "GET".  Operation is
		// one of the fields primarily responsible for the grouping of Datastore
		// metrics.
		Operation: "SELECT",
	}
	rows, err := sqlConnection.Connection.Queryx(queryDetailsDto.Query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()
	BindQuerySegment.End()

	return BindQueryResults(arguments, rows, queryDetailsDto, integration, sqlConnection, executeAndBindTransaction)
}

// BindQueryResults binds query results to the specified data model using `sqlx`
func BindQueryResults(arguments args.ArgumentList,
	rows *sqlx.Rows,
	queryDetailsDto models.QueryDetailsDto,
	integration *integration.Integration,
	sqlConnection *connection.SQLConnection,
	executeAndBindTransaction *newrelic.Transaction,
) ([]interface{}, error) {
	BindQuerySegment := executeAndBindTransaction.StartSegment("bindQueryResults")

	results := make([]interface{}, 0)

	for rows.Next() {
		segment := executeAndBindTransaction.StartSegment("bindQueryResults - " + queryDetailsDto.Name)
		switch queryDetailsDto.Type {
		case "slowQueries":
			var model models.TopNSlowQueryDetails
			if err := rows.StructScan(&model); err != nil {
				log.Error("Could not scan row: ", err)
				continue
			}

			AnonymizeSegment := executeAndBindTransaction.StartSegment("bindQueryResults - " + queryDetailsDto.Name + "Subsegment - Anonymize")
			AnonymizeQueryText(model.QueryText)
			AnonymizeSegment.End()

			results = append(results, model)

			SingleExecutionPlanSegment := executeAndBindTransaction.StartSegment("bindQueryResults - " + "SingleExecutionPlanSubsegment")
			// fetch and generate execution plan
			if model.QueryID != nil {
				GenerateAndInjestExecutionPlan(arguments, integration, sqlConnection, *model.QueryID, executeAndBindTransaction)
			}
			SingleExecutionPlanSegment.End()

		case "waitAnalysis":
			var model models.WaitTimeAnalysis
			if err := rows.StructScan(&model); err != nil {
				log.Error("Could not scan row: ", err)
				continue
			}
			AnonymizeSegment := executeAndBindTransaction.StartSegment("bindQueryResults - " + queryDetailsDto.Name + "Subsegment - Anonymize")
			AnonymizeQueryText(model.QueryText)
			AnonymizeSegment.End()

			results = append(results, model)
		case "blockingSessions":
			var model models.BlockingSessionQueryDetails
			if err := rows.StructScan(&model); err != nil {
				log.Error("Could not scan row: ", err)
				continue
			}

			AnonymizeSegment := executeAndBindTransaction.StartSegment("bindQueryResults - " + queryDetailsDto.Name + "Subsegment - Anonymize")
			AnonymizeQueryText(model.BlockedQueryText)
			AnonymizeQueryText(model.BlockingQueryText)
			AnonymizeSegment.End()

			results = append(results, model)
		default:
			return nil, fmt.Errorf("%w: %s", ErrUnknownQueryType, queryDetailsDto.Type)
		}
		segment.End()
	}
	BindQuerySegment.End()
	return results, nil
}

func GenerateAndInjestExecutionPlan(arguments args.ArgumentList,
	integration *integration.Integration,
	sqlConnection *connection.SQLConnection,
	queryID models.HexString, executeAndBindTransaction *newrelic.Transaction) {
	hexQueryID := string(queryID)
	executionPlanQuery := fmt.Sprintf(config.ExecutionPlanQueryTemplate, min(config.IndividualQueryCountMax, arguments.QueryCountThreshold),
		arguments.QueryResponseTimeThreshold, hexQueryID, arguments.FetchInterval, config.TextTruncateLimit)

	segment := newrelic.DatastoreSegment{
		StartTime: executeAndBindTransaction.StartSegmentNow(),
		// Product is the datastore type.  See the constants in
		// https://github.com/newrelic/go-agent/blob/master/v3/newrelic/datastore.go.  Product
		// is one of the fields primarily responsible for the grouping of Datastore
		// metrics.
		Product: newrelic.DatastoreMSSQL,
		// Collection is the table or group being operated upon in the datastore,
		// e.g. "users_table".  This becomes the db.collection attribute on Span
		// events and Transaction Trace segments.  Collection is one of the fields
		// primarily responsible for the grouping of Datastore metrics.
		Collection: "executingQuery -  " + "MSSQLQueryExecutionPlans",
		// Operation is the relevant action, e.g. "SELECT" or "GET".  Operation is
		// one of the fields primarily responsible for the grouping of Datastore
		// metrics.
		Operation: "SELECT",
	}
	var model models.ExecutionPlanResult

	rows, err := sqlConnection.Connection.Queryx(executionPlanQuery)
	if err != nil {
		log.Error("Failed to execute query: %s", err)
		return
	}
	defer rows.Close()
	segment.End()

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

	injestExecutionPlanSegment := executeAndBindTransaction.StartSegment("injestExecutionPlan")
	// Ingest the execution plan
	if err := IngestQueryMetricsInBatches(results, queryDetailsDto, integration, sqlConnection); err != nil {
		log.Error("Failed to ingest execution plan: %s", err)
	}
	injestExecutionPlanSegment.End()
}

func IngestQueryMetricsInBatches(results []interface{},
	queryDetailsDto models.QueryDetailsDto,
	integration *integration.Integration,
	sqlConnection *connection.SQLConnection) error {
	const batchSize = 100

	for start := 0; start < len(results); start += batchSize {
		end := start + batchSize
		if end > len(results) {
			end = len(results)
		}

		batchResult := results[start:end]

		if err := IngestQueryMetrics(batchResult, queryDetailsDto, integration, sqlConnection); err != nil {
			return fmt.Errorf("error ingesting batch from %d to %d: %w", start, end, err)
		}
	}

	return nil
}

func convertResultToMap(result interface{}) (map[string]interface{}, error) {
	data, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("error marshaling result: %w", err)
	}

	var resultMap map[string]interface{}
	if err := json.Unmarshal(data, &resultMap); err != nil {
		return nil, fmt.Errorf("error unmarshaling to map: %w", err)
	}
	return resultMap, nil
}

// handleGaugeMetric processes the gauge metric and logs any errors encountered
func handleGaugeMetric(key, strValue string, metricSet *metric.Set) {
	floatValue, err := strconv.ParseFloat(strValue, 64)
	if err != nil {
		log.Error("failed to parse float value for key %s: %v", key, err)
		return
	}

	err = metricSet.SetMetric(key, floatValue, metric.GAUGE)
	if err != nil {
		log.Error("failed to set metric for key %s: %v", key, err)
	}
}

// IngestQueryMetrics processes and ingests query metrics into the New Relic entity
func IngestQueryMetrics(results []interface{}, queryDetailsDto models.QueryDetailsDto, integration *integration.Integration, sqlConnection *connection.SQLConnection) error {
	instanceEntity, err := instance.CreateInstanceEntity(integration, sqlConnection)
	if err != nil {
		log.Error("%w: %v", ErrCreatingInstanceEntity, err)
	}

	for _, result := range results {
		// Convert the result into a map[string]interface{} for dynamic key-value access
		resultMap, err := convertResultToMap(result)
		if err != nil {
			log.Error("failed to convert result: %v", err)
			continue
		}

		// Create a new metric set with the query name
		metricSet := instanceEntity.NewMetricSet(queryDetailsDto.Name)

		// Iterate over the map and add each key-value pair as a metric
		for key, value := range resultMap {
			strValue := fmt.Sprintf("%v", value) // Convert the value to a string representation
			metricType := DetectMetricType(strValue)
			if metricType == metric.GAUGE {
				handleGaugeMetric(key, strValue, metricSet)
			} else {
				if err := metricSet.SetMetric(key, strValue, metric.ATTRIBUTE); err != nil {
					// Handle the error. This could be logging, returning the error, etc.
					log.Error("failed to set metric: %v", err)
				}
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

func DetectMetricType(value string) metric.SourceType {
	if _, err := strconv.ParseFloat(value, 64); err != nil {
		return metric.ATTRIBUTE
	}
	return metric.GAUGE
}

func AnonymizeQueryText(query *string) {
	if query == nil {
		return
	}
	re := regexp.MustCompile(`'[^']*'|\d+|".*?"`)
	anonymizedQuery := re.ReplaceAllString(*query, "?")
	*query = anonymizedQuery
}

// ValidateAndSetDefaults checks if fields are invalid and sets defaults
func ValidateAndSetDefaults(args *args.ArgumentList) {
	// Since EnableQueryPerformance is a boolean, no need to reset as it can't be invalid in this context
	if args.QueryResponseTimeThreshold < 0 {
		args.QueryResponseTimeThreshold = config.QueryResponseTimeThresholdDefault
	}

	if args.QueryCountThreshold < 0 {
		args.QueryCountThreshold = config.SlowQueryCountThresholdDefault
	} else if args.QueryCountThreshold >= config.GroupedQueryCountMax {
		args.QueryCountThreshold = config.GroupedQueryCountMax
	}
}
