package validation

import (
	"regexp"

	"github.com/blang/semver/v4"
	"github.com/newrelic/infra-integrations-sdk/v3/log"
	"github.com/newrelic/nri-mssql/src/queryanalysis/connection"
)

func checkSqlServerVersion(sqlConnection *connection.SQLConnection) bool {

	rows, err := sqlConnection.Queryx("SELECT @@VERSION\n")
	if err != nil {
		log.Error("Error getting Server version:", err)
		return false
	}
	defer rows.Close()

	rows.Next()
	var serverVersion string
	if err := rows.Scan(&serverVersion); err != nil {
		log.Error("Error scanning server version:", err)
		return false
	}

	if serverVersion == "" {
		log.Error("Server version is empty")
		return false
	}

	log.Debug("Server version: %s", serverVersion)

	// Regex to capture the full version number in the format major.minor.build
	re := regexp.MustCompile(`\b(\d+\.\d+\.\d+)\b`)
	versionStr := re.FindString(serverVersion)
	if versionStr == "" {
		log.Error("Could not parse version from server version string")
		return false
	}

	log.Debug("Parsed version string: %s", versionStr)

	version, err := semver.ParseTolerant(versionStr)
	if err != nil {
		log.Error("Error parsing version:", err)
		return false
	}

	log.Debug("Parsed semantic version: %s", version)

	// List of supported major versions
	supportedVersions := []uint64{16, 15, 14} // Corresponding to SQL Server 2022, 2019, and 2017

	isSupported := false
	for _, supportedVersion := range supportedVersions {
		if version.Major == supportedVersion {
			isSupported = true
			break
		}
	}

	if !isSupported {
		log.Error("Unsupported SQL Server version: %s", version.String())
	}

	return isSupported
}
