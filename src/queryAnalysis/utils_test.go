package queryAnalysis

import (
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/newrelic/infra-integrations-sdk/v3/data/metric"
	"github.com/newrelic/infra-integrations-sdk/v3/integration"
	"github.com/newrelic/nri-mssql/src/args"
	"github.com/newrelic/nri-mssql/src/queryAnalysis/config"
	"github.com/newrelic/nri-mssql/src/queryAnalysis/connection"
	"github.com/newrelic/nri-mssql/src/queryAnalysis/models"
	"github.com/stretchr/testify/assert"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
	"testing"
	"time"
)

func TestExecuteQuery_SlowQueriesSuccess(t *testing.T) {
	sqlConn, mock := connection.CreateMockSQL(t)
	defer sqlConn.Connection.Close()

	query := "SELECT * FROM slow_queries WHERE condition"
	mock.ExpectQuery("SELECT \\* FROM slow_queries WHERE condition").
		WillReturnRows(sqlmock.NewRows([]string{
			"query_id", "query_text", "database_name",
		}).
			AddRow(
				[]byte{0x01, 0x02},
				"SELECT * FROM something",
				"example_db",
			))

	queryDetails := models.QueryDetailsDto{
		Name:  "SlowQueries",
		Query: query,
		Type:  "slowQueries",
	}

	integrationObj := &integration.Integration{}
	argList := args.ArgumentList{}

	results, err := ExecuteQuery(argList, queryDetails, integrationObj, sqlConn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	slowQuery, ok := results[0].(models.TopNSlowQueryDetails)
	if !ok {
		t.Fatalf("expected type models.TopNSlowQueryDetails, got %T", results[0])
	}

	expectedQueryID := models.HexString("0x0102")
	if slowQuery.QueryID == nil || *slowQuery.QueryID != expectedQueryID {
		t.Errorf("expected QueryID %v, got %v", expectedQueryID, slowQuery.QueryID)
	}

	if err = mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}

func TestExecuteQuery_WaitTimeAnalysis(t *testing.T) {
	// Set up the mock SQL connection and expectations
	sqlConn, mock := connection.CreateMockSQL(t)
	defer sqlConn.Connection.Close()

	query := "SELECT * FROM wait_analysis WHERE condition"
	mock.ExpectQuery("SELECT \\* FROM wait_analysis WHERE condition").
		WillReturnRows(sqlmock.NewRows([]string{
			"query_id", "database_name", "query_text", "wait_category",
			"total_wait_time_ms", "avg_wait_time_ms", "wait_event_count",
			"last_execution_time", "collection_timestamp",
		}).
			AddRow(
				[]byte{0x01, 0x02}, // Simulated SQL varbinary value
				"example_db",
				"SELECT * FROM waits",
				"CPU",
				100.5,
				50.25,
				10,
				time.Now(),
				time.Now(),
			))

	queryDetails := models.QueryDetailsDto{
		Name:  "WaitTimeAnalysisQuery",
		Query: query,
		Type:  "waitAnalysis",
	}

	integrationObj := &integration.Integration{}
	argList := args.ArgumentList{}

	results, err := ExecuteQuery(argList, queryDetails, integrationObj, sqlConn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	waitTimeAnalysis, ok := results[0].(models.WaitTimeAnalysis)
	if !ok {
		t.Fatalf("expected type models.WaitTimeAnalysis, got %T", results[0])
	}

	expectedQueryID := models.HexString("0x0102")
	if waitTimeAnalysis.QueryID == nil || *waitTimeAnalysis.QueryID != expectedQueryID {
		t.Errorf("expected QueryID %v, got %v", expectedQueryID, waitTimeAnalysis.QueryID)
	}

	expectedDatabaseName := "example_db"
	if waitTimeAnalysis.DatabaseName == nil || *waitTimeAnalysis.DatabaseName != expectedDatabaseName {
		t.Errorf("expected DatabaseName %s, got %v", expectedDatabaseName, waitTimeAnalysis.DatabaseName)
	}

	if err = mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}

func TestExecuteQuery_BlockingSessionsSuccess(t *testing.T) {
	sqlConn, mock := connection.CreateMockSQL(t)
	defer sqlConn.Connection.Close()

	query := "SELECT * FROM blocking_sessions WHERE condition"
	mock.ExpectQuery("SELECT \\* FROM blocking_sessions WHERE condition").
		WillReturnRows(sqlmock.NewRows([]string{
			"blocking_spid", "blocking_status", "blocked_spid", "blocked_status",
			"wait_type", "wait_time_in_seconds", "command_type", "database_name",
			"blocking_query_text", "blocked_query_text",
		}).
			AddRow(
				int64(101),
				"Running",
				int64(202),
				"Suspended",
				"LCK_M_U",
				3.5,
				"SELECT",
				"example_db",
				"SELECT * FROM source",
				"INSERT INTO destination",
			))

	queryDetails := models.QueryDetailsDto{
		Name:  "BlockingSessionsQuery",
		Query: query,
		Type:  "blockingSessions",
	}

	integrationObj := &integration.Integration{}
	argList := args.ArgumentList{}

	results, err := ExecuteQuery(argList, queryDetails, integrationObj, sqlConn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	blockingSession, ok := results[0].(models.BlockingSessionQueryDetails)
	if !ok {
		t.Fatalf("expected type models.BlockingSessionQueryDetails, got %T", results[0])
	}

	expectedBlockingSPID := int64(101)
	if blockingSession.BlockingSPID == nil || *blockingSession.BlockingSPID != expectedBlockingSPID {
		t.Errorf("expected BlockingSPID %v, got %v", expectedBlockingSPID, blockingSession.BlockingSPID)
	}

	expectedBlockedSPID := int64(202)
	if blockingSession.BlockedSPID == nil || *blockingSession.BlockedSPID != expectedBlockedSPID {
		t.Errorf("expected BlockedSPID %v, got %v", expectedBlockedSPID, blockingSession.BlockedSPID)
	}

	expectedDatabaseName := "example_db"
	if blockingSession.DatabaseName == nil || *blockingSession.DatabaseName != expectedDatabaseName {
		t.Errorf("expected DatabaseName %s, got %v", expectedDatabaseName, blockingSession.DatabaseName)
	}

	expectedBlockingQueryText := "SELECT * FROM source"
	if blockingSession.BlockingQueryText == nil || *blockingSession.BlockingQueryText != expectedBlockingQueryText {
		t.Errorf("expected BlockingQueryText %s, got %v", expectedBlockingQueryText, blockingSession.BlockingQueryText)
	}

	if err = mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}

func TestLoadQueries_SlowQueries(t *testing.T) {

	var configQueries []models.QueryDetailsDto = config.Queries

	var args args.ArgumentList
	args.FetchInterval = 15
	args.QueryResponseTimeThreshold = 10
	args.QueryResponseTimeThreshold = 0

	slowQueriesIndex := -1
	for i, query := range config.Queries {
		if query.Type == "slowQueries" {
			slowQueriesIndex = i
			break
		}
	}

	// Ensure the correct query was found
	if slowQueriesIndex == -1 {
		t.Fatalf("could not find 'MSSQLTopSlowQueries' in the list of queries")
	}

	queries, err := LoadQueries(args)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	configQueries[slowQueriesIndex].Query = fmt.Sprintf(configQueries[slowQueriesIndex].Query, args.FetchInterval, args.FetchInterval, args.QueryCountThreshold)
	if queries[slowQueriesIndex].Query != configQueries[slowQueriesIndex].Query {
		t.Errorf("expected: %s, got: %s", configQueries[slowQueriesIndex].Query, queries[slowQueriesIndex].Query)
	}
}

func TestLoadQueries_WaitAnalysis(t *testing.T) {

	var configQueries []models.QueryDetailsDto = config.Queries

	var args args.ArgumentList

	args.FetchInterval = 15
	args.QueryCountThreshold = 10

	waitQueriesIndex := -1
	for i, query := range config.Queries {
		if query.Type == "waitAnalysis" {
			waitQueriesIndex = i
			break
		}
	}

	// Ensure the correct query was found
	if waitQueriesIndex == -1 {
		t.Fatalf("could not find 'MSSQLTopSlowQueries' in the list of queries")
	}

	queries, err := LoadQueries(args)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	configQueries[waitQueriesIndex].Query = fmt.Sprintf(configQueries[waitQueriesIndex].Query, args.FetchInterval, args.FetchInterval, args.QueryCountThreshold)
	if queries[waitQueriesIndex].Query != configQueries[waitQueriesIndex].Query {
		t.Errorf("expected: %s, got: %s", configQueries[waitQueriesIndex].Query, queries[waitQueriesIndex].Query)
	}
}

func TestLoadQueries_BlockingSessions(t *testing.T) {
	config.Queries = []models.QueryDetailsDto{
		{
			Name:  "MSSQLBlockingSessionQueries",
			Query: "Sample blocking session query",
			Type:  "blockingSessions",
		},
	}

	var args args.ArgumentList

	queries, err := LoadQueries(args)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(queries) != 1 {
		t.Fatalf("expected 1 query, got %d", len(queries))
	}

	if queries[0].Query != "Sample blocking session query" {
		t.Errorf("unexpected query content: %s", queries[0].Query)
	}
}

func TestLoadQueries_UnknownType(t *testing.T) {
	config.Queries = []models.QueryDetailsDto{
		{
			Name:  "UnknownTypeQuery",
			Query: "SELECT * FROM mysterious_table",
			Type:  "unknownType",
		},
	}

	var args args.ArgumentList

	args.FetchInterval = 15

	queries, err := LoadQueries(args)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Since the unknown type processing doesn't alter any query, we just check if the query is the same.
	if queries[0].Query != "SELECT * FROM mysterious_table" {
		t.Errorf("unexpected query content for unknown type: %s", queries[0].Query)
	}
}

func TestDetectMetricType_GaugeCase(t *testing.T) {
	// Case where the string can be parsed as a float
	value := "123.45"
	expected := metric.GAUGE

	result := DetectMetricType(value)

	assert.Equal(t, expected, result, "expected GAUGE for a parsable float string")
}

func TestDetectMetricType_AttributeCase(t *testing.T) {
	// Case where the string cannot be parsed as a float
	value := "NotANumber123"
	expected := metric.ATTRIBUTE

	result := DetectMetricType(value)

	assert.Equal(t, expected, result, "expected ATTRIBUTE for a non-parsable float string")
}

func TestDetectMetricType_EmptyString(t *testing.T) {
	// Case where the string is empty
	value := ""
	expected := metric.ATTRIBUTE

	result := DetectMetricType(value)

	assert.Equal(t, expected, result, "expected ATTRIBUTE for an empty string")
}

func TestDetectMetricType_Integer(t *testing.T) {
	// Case where the string is an integer number
	value := "78"
	expected := metric.GAUGE

	result := DetectMetricType(value)

	assert.Equal(t, expected, result, "expected GAUGE for integer string")
}

func TestAnonymizeQueryText_SingleQuotedStrings(t *testing.T) {
	query := "SELECT * FROM users WHERE username = 'admin' AND password = 'secret'"
	expected := "SELECT * FROM users WHERE username = ? AND password = ?"

	AnonymizeQueryText(&query)

	assert.Equal(t, expected, query, "anonymized query should replace single-quoted strings with '?'")
}

func TestAnonymizeQueryText_DoubleQuotedStrings(t *testing.T) {
	query := `SELECT * FROM config WHERE name = "config_value"`
	expected := "SELECT * FROM config WHERE name = ?"

	AnonymizeQueryText(&query)

	assert.Equal(t, expected, query, "anonymized query should replace double-quoted strings with '?'")
}

func TestAnonymizeQueryText_Numbers(t *testing.T) {
	query := "UPDATE orders SET price = 299, quantity = 3 WHERE order_id = 42"
	expected := "UPDATE orders SET price = ?, quantity = ? WHERE order_id = ?"

	AnonymizeQueryText(&query)

	assert.Equal(t, expected, query, "anonymized query should replace numbers with '?'")
}

func TestAnonymizeQueryText_MixedContent(t *testing.T) {
	query := "SELECT name, 'value' FROM table WHERE age > 30 AND id = 2"
	expected := "SELECT name, ? FROM table WHERE age > ? AND id = ?"

	AnonymizeQueryText(&query)

	assert.Equal(t, expected, query, "anonymized query should handle mixed content of strings and numbers")
}

func TestAnonymizeQueryText_EmptyString(t *testing.T) {
	query := ""
	expected := ""

	AnonymizeQueryText(&query)

	assert.Equal(t, expected, query, "anonymized query should handle empty string gracefully")
}

func TestAnonymizeQueryText_NoSensitiveData(t *testing.T) {
	query := "SELECT name FROM users"
	expected := "SELECT name FROM users"

	AnonymizeQueryText(&query)

	assert.Equal(t, expected, query, "anonymized query should remain unchanged if there is no sensitive data")
}

func TestBindQueryResults_BlockingSessions(t *testing.T) {
	mockDB, mock, err := sqlmock.New() // Create a sqlmock database connection
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer mockDB.Close()

	sqlxDB := sqlx.NewDb(mockDB, "sqlmock") // Wrap with sqlx.DB
	rows := sqlmock.NewRows([]string{
		"blocking_spid", "blocking_status", "blocked_spid", "blocked_status",
		"wait_type", "wait_time_in_seconds", "command_type", "database_name",
		"blocking_query_text", "blocked_query_text",
	}).AddRow(
		int64(101), "Running", int64(202), "Suspended",
		"LCK_M_U", 3.5, "SELECT", "example_db",
		"SELECT * FROM source", "INSERT INTO destination",
	)
	mock.ExpectQuery("SELECT \\* FROM blocking_sessions WHERE condition").WillReturnRows(rows)

	queryDetails := models.QueryDetailsDto{
		Name:  "BlockingSessionsQuery",
		Query: "SELECT * FROM blocking_sessions WHERE condition",
		Type:  "blockingSessions",
	}

	integrationObj := &integration.Integration{}
	argList := args.ArgumentList{}
	sqlConnection := &connection.SQLConnection{Connection: sqlxDB}

	rowsOut, err := sqlConnection.Connection.Queryx(queryDetails.Query)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	results, err := BindQueryResults(argList, rowsOut, queryDetails, integrationObj, sqlConnection)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	blockingSession, ok := results[0].(models.BlockingSessionQueryDetails)
	if !ok {
		t.Fatalf("expected type models.BlockingSessionQueryDetails, got %T", results[0])
	}

	expectedBlockingSPID := int64(101)
	if blockingSession.BlockingSPID == nil || *blockingSession.BlockingSPID != expectedBlockingSPID {
		t.Errorf("expected BlockingSPID %v, got %v", expectedBlockingSPID, blockingSession.BlockingSPID)
	}

	expectedBlockedSPID := int64(202)
	if blockingSession.BlockedSPID == nil || *blockingSession.BlockedSPID != expectedBlockedSPID {
		t.Errorf("expected BlockedSPID %v, got %v", expectedBlockedSPID, blockingSession.BlockedSPID)
	}

	expectedDatabaseName := "example_db"
	if blockingSession.DatabaseName == nil || *blockingSession.DatabaseName != expectedDatabaseName {
		t.Errorf("expected DatabaseName %s, got %v", expectedDatabaseName, blockingSession.DatabaseName)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}

func TestBindQueryResults_WaitAnalysis(t *testing.T) {
	// Create a mock sqlmock database connection
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("unexpected error when opening mock database: %v", err)
	}
	defer mockDB.Close()

	sqlxDB := sqlx.NewDb(mockDB, "sqlmock") // Wrap the *sql.DB with sqlx
	now := time.Now()
	rows := sqlmock.NewRows([]string{
		"query_id", "database_name", "query_text", "wait_category",
		"total_wait_time_ms", "avg_wait_time_ms", "wait_event_count",
		"last_execution_time", "collection_timestamp",
	}).
		AddRow(
			[]byte{0xAB, 0xCD},
			"example_db",
			"SELECT * FROM waits",
			"CPU",
			200.75,
			100.05,
			5,
			now,
			now,
		)

	mock.ExpectQuery("SELECT \\* FROM wait_analysis WHERE condition").WillReturnRows(rows)

	queryDetails := models.QueryDetailsDto{
		Name:  "WaitTimeAnalysisQuery",
		Query: "SELECT * FROM wait_analysis WHERE condition",
		Type:  "waitAnalysis",
	}

	integrationObj := &integration.Integration{}
	argList := args.ArgumentList{}
	sqlConnection := &connection.SQLConnection{Connection: sqlxDB}

	rowsOut, err := sqlConnection.Connection.Queryx(queryDetails.Query)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	results, err := BindQueryResults(argList, rowsOut, queryDetails, integrationObj, sqlConnection)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	waitAnalysis, ok := results[0].(models.WaitTimeAnalysis)
	if !ok {
		t.Fatalf("expected type models.WaitTimeAnalysis, got %T", results[0])
	}

	expectedQueryID := models.HexString("0xabcd")
	if waitAnalysis.QueryID == nil || *waitAnalysis.QueryID != expectedQueryID {
		t.Errorf("expected QueryID %v, got %v", expectedQueryID, waitAnalysis.QueryID)
	}

	expectedDatabaseName := "example_db"
	if waitAnalysis.DatabaseName == nil || *waitAnalysis.DatabaseName != expectedDatabaseName {
		t.Errorf("expected DatabaseName %s, got %v", expectedDatabaseName, waitAnalysis.DatabaseName)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %v", err)
	}
}
