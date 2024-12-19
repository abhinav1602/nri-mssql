#!/bin/bash
/opt/mssql/bin/sqlservr &  # Start SQL Server in the background
# Call the configure script
/setup/configure-sql.sh
# Keep the container running (essential for background processes)
wait $!