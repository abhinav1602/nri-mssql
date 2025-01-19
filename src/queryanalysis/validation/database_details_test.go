package validation

import (
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/newrelic/nri-mssql/src/queryanalysis/connection"
	"github.com/stretchr/testify/assert"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
)

// Mock implementation of the checkSqlServerVersion function to return true
var checkServerVersion = func(sqlConnection *connection.SQLConnection) bool {
	return true
}

//func TestGetDatabaseDetails(t *testing.T) {
//	// Create a new mock database connection
//	db, mock, err := sqlmock.New()
//	assert.NoError(t, err)
//	defer db.Close()
//
//	sqlConnection := &connection.SQLConnection{Connection: sqlx.NewDb(db, "sqlmock")}
//
//	// Define the expected rows
//	rows := sqlmock.NewRows([]string{"database_id", "name", "compatibility_level", "is_query_store_on"}).
//		AddRow(100, "testdb1", 100, true).
//		AddRow(101, "testdb2", 110, false).
//		AddRow(1, "master", 100, true) // Should be filtered out based on database_id
//
//	// Make the regular expression for the SQL query case-insensitive
//	mock.ExpectQuery("(?i)^SELECT database_id, name, compatibility_level, is_query_store_on FROM sys\\.databases$").WillReturnRows(rows)
//
//	// Call the function
//	databaseDetails, err := GetDatabaseDetails(sqlConnection)
//
//	// Assertions
//	assert.NoError(t, err)
//	assert.Len(t, databaseDetails, 2) // Only 2 databases should be returned
//	assert.Equal(t, 100, databaseDetails[0].DatabaseID)
//	assert.Equal(t, "testdb1", databaseDetails[0].Name)
//	assert.Equal(t, 100, databaseDetails[0].Compatibility)
//	assert.Equal(t, true, databaseDetails[0].IsQueryStoreOn)
//	assert.Equal(t, 101, databaseDetails[1].DatabaseID)
//	assert.Equal(t, "testdb2", databaseDetails[1].Name)
//	assert.Equal(t, 110, databaseDetails[1].Compatibility)
//	assert.Equal(t, false, databaseDetails[1].IsQueryStoreOn)
//
//	// Ensure all expectations were met
//	assert.NoError(t, mock.ExpectationsWereMet())
//}

//func TestGetDatabaseDetails(t *testing.T) {
//	// Mock the checkSqlServerVersion to return true to simulate supported version
//	originalCheckSqlServerVersion := checkSqlServerVersion
//
//	defer func() { checkServerVersion = originalCheckSqlServerVersion }()
//	checkServerVersion = func(sqlConnection *connection.SQLConnection) bool {
//		return true
//	}
//
//	// Create a new mock database connection
//	db, mock, err := sqlmock.New()
//	assert.NoError(t, err)
//	defer db.Close()
//
//	sqlConnection := &connection.SQLConnection{Connection: sqlx.NewDb(db, "sqlmock")}
//
//	// Define the expected rows
//	rows := sqlmock.NewRows([]string{"database_id", "name", "compatibility_level", "is_query_store_on"}).
//		AddRow(100, "testdb1", 100, true).
//		AddRow(101, "testdb2", 110, false).
//		AddRow(1, "master", 100, true) // Should be filtered out based on database_id
//
//	// Make the regular expression for the SQL query case-insensitive
//	mock.ExpectQuery("(?i)^SELECT database_id, name, compatibility_level, is_query_store_on FROM sys\\.databases$").WillReturnRows(rows)
//
//	// Call the function
//	databaseDetails, err := GetDatabaseDetails(sqlConnection)
//
//	// Assertions
//	assert.NoError(t, err)
//	assert.Len(t, databaseDetails, 2) // Only 2 databases should be returned
//	assert.Equal(t, 100, databaseDetails[0].DatabaseID)
//	assert.Equal(t, "testdb1", databaseDetails[0].Name)
//	assert.Equal(t, 100, databaseDetails[0].Compatibility)
//	assert.Equal(t, true, databaseDetails[0].IsQueryStoreOn)
//	assert.Equal(t, 101, databaseDetails[1].DatabaseID)
//	assert.Equal(t, "testdb2", databaseDetails[1].Name)
//	assert.Equal(t, 110, databaseDetails[1].Compatibility)
//	assert.Equal(t, false, databaseDetails[1].IsQueryStoreOn)
//
//	// Ensure all expectations were met
//	assert.NoError(t, mock.ExpectationsWereMet())
//}

func TestGetDatabaseDetails_Error(t *testing.T) {
	// Create a new mock database connection
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	sqlConnection := &connection.SQLConnection{Connection: sqlx.NewDb(db, "sqlmock")}

	// Define the expected error
	errQueryError := sqlmock.ErrCancelled // or use the appropriate error you expect

	// Update the mocked query to match the new SQL structure with "database_id"
	mock.ExpectQuery("(?i)^SELECT database_id, name, compatibility_level, is_query_store_on FROM sys\\.databases$").
		WillReturnError(errQueryError)

	// Call the function
	databaseDetails, err := GetDatabaseDetails(sqlConnection)

	// Assertions
	assert.Error(t, err)
	assert.Nil(t, databaseDetails)

	// Ensure all expectations were met
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGetDatabaseDetails_UnsupportedVersion(t *testing.T) {
	// Mock the checkSqlServerVersion to return false to simulate unsupported version
	originalCheckSqlServerVersion := checkSqlServerVersion
	defer func() { checkServerVersion = originalCheckSqlServerVersion }()
	checkServerVersion = func(sqlConnection *connection.SQLConnection) bool {
		return false
	}

	// This should simulate the log.Error call for unsupported SQL server version
	//logError := func(format string, args ...interface{}) {
	//	assert.Equal(t, "Unsupported SQL Server version.", format)
	//	assert.Empty(t, args)
	//}
	//log.Error = logError

	// Create a new mock database connection
	db, _, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	sqlConnection := &connection.SQLConnection{Connection: sqlx.NewDb(db, "sqlmock")}

	// Call the function
	databaseDetails, err := GetDatabaseDetails(sqlConnection)

	// Assertions
	assert.Nil(t, err)
	assert.Nil(t, databaseDetails)
}
