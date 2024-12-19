#!/bin/bash
# Wait for SQL Server to become available
/opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -Q "SELECT 1"  2>/dev/null
while [[ $? -ne 0 ]]; do
    echo "Waiting for SQL Server..."
    sleep 5
    /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -Q "SELECT 1" 2>/dev/null
done
# Now run the configuration commands (after SQL Server is up)
#/opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -Q "EXEC sp_configure 'show advanced options', 1; RECONFIGURE WITH OVERRIDE;"
#/opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -Q "EXEC sp_configure 'max server memory', 4096; RECONFIGURE WITH OVERRIDE;"
#/opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -Q "EXEC sp_configure 'optimize for ad hoc workloads', 1; RECONFIGURE WITH OVERRIDE;"