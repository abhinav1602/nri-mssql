package connection

import (
	"testing"

	"github.com/jmoiron/sqlx"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
)

// CreateMockSQL creates a Test SQLConnection. Must Close con when done
func CreateMockSQL(t *testing.T) (con *SQLConnection, mock sqlmock.Sqlmock) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Errorf("Unexpected error while mocking: %s", err.Error())
		t.FailNow()
	}

	con = &SQLConnection{
		Connection: sqlx.NewDb(mockDB, "sqlmock"),
		Host:       "testhost",
	}

	return
}
