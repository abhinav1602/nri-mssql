package config

import "github.com/newrelic/nri-mssql/src/queryAnalysis/models"

var Queries = []models.QueryDetailsDto{
	{
		Name: "MSSQLTopSlowQueries",
		Query: `WITH QueryStats AS (
				SELECT
					qs.plan_handle,
					qs.sql_handle,
					SUBSTRING(
						qt.text,
						(qs.statement_start_offset / 2) + 1,
						(
							CASE
								qs.statement_end_offset
								WHEN -1 THEN DATALENGTH(qt.text)
								ELSE qs.statement_end_offset
							END - qs.statement_start_offset
						) / 2 + 1
					) AS query_text,
					qs.query_hash AS query_id,
					qs.last_execution_time,
					qs.execution_count,
					(qs.total_worker_time / qs.execution_count) / 1000.0 AS avg_cpu_time_ms,
					(qs.total_elapsed_time / qs.execution_count) / 1000.0 AS avg_elapsed_time_ms,
					(qs.total_logical_reads / qs.execution_count) AS avg_disk_reads,
					(qs.total_logical_writes / qs.execution_count) AS avg_disk_writes,
					CASE
						WHEN UPPER(
							LTRIM(
								SUBSTRING(qt.text, (qs.statement_start_offset / 2) + 1, 6)
							)
						) LIKE 'SELECT' THEN 'SELECT'
						WHEN UPPER(
							LTRIM(
								SUBSTRING(qt.text, (qs.statement_start_offset / 2) + 1, 6)
							)
						) LIKE 'INSERT' THEN 'INSERT'
						WHEN UPPER(
							LTRIM(
								SUBSTRING(qt.text, (qs.statement_start_offset / 2) + 1, 6)
							)
						) LIKE 'UPDATE' THEN 'UPDATE'
						WHEN UPPER(
							LTRIM(
								SUBSTRING(qt.text, (qs.statement_start_offset / 2) + 1, 6)
							)
						) LIKE 'DELETE' THEN 'DELETE'
						ELSE 'OTHER'
					END AS statement_type,
					CONVERT(INT, pa.value) AS database_id,
					qt.objectid
				FROM
					sys.dm_exec_query_stats qs
					CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS qt
					JOIN sys.dm_exec_cached_plans cp ON qs.plan_handle = cp.plan_handle
					CROSS APPLY sys.dm_exec_plan_attributes(cp.plan_handle) AS pa
				WHERE
					qs.execution_count > 0
					AND pa.attribute = 'dbid'
					AND DB_NAME(CONVERT(INT, pa.value)) NOT IN ('master', 'model', 'msdb', 'tempdb')
					AND qs.last_execution_time >= DATEADD(SECOND, -%d, GETUTCDATE())
					AND qt.text NOT LIKE '%%sys.%%'
					AND qt.text NOT LIKE '%%INFORMATION_SCHEMA%%'
					AND qt.text NOT LIKE '%%schema_name()%%'
					AND qt.text IS NOT NULL
					AND LTRIM(RTRIM(qt.text)) <> ''
			)
			SELECT
				TOP %d qs.query_id,
				MIN(qs.query_text) AS query_text,
				DB_NAME(MIN(qs.database_id)) AS database_name,
				COALESCE(
					OBJECT_SCHEMA_NAME(MIN(qs.objectid), MIN(qs.database_id)),
					'N/A'
				) AS schema_name,
				FORMAT(
					MAX(qs.last_execution_time) AT TIME ZONE 'UTC',
					'yyyy-MM-ddTHH:mm:ssZ'
				) AS last_execution_timestamp,
				SUM(qs.execution_count) AS execution_count,
				AVG(qs.avg_cpu_time_ms) AS avg_cpu_time_ms,
				AVG(qs.avg_elapsed_time_ms) AS avg_elapsed_time_ms,
				AVG(qs.avg_disk_reads) AS avg_disk_reads,
				AVG(qs.avg_disk_writes) AS avg_disk_writes,
				MAX(qs.statement_type) AS statement_type,
				FORMAT(
					SYSDATETIMEOFFSET() AT TIME ZONE 'UTC',
					'yyyy-MM-ddTHH:mm:ssZ'
				) AS collection_timestamp
			FROM
				QueryStats qs
			GROUP BY
				qs.query_id
			HAVING
				AVG(qs.avg_elapsed_time_ms) > %d
			ORDER BY
				avg_elapsed_time_ms DESC;`,
		Type: "slowQueries",
	},
	{
		Name: "MSSQLWaitTimeAnalysis",
		Query: `DECLARE @sql NVARCHAR(MAX) = '';
				DECLARE @dbName NVARCHAR(128);
				
				DECLARE @resultTable TABLE(
					query_id VARBINARY(255),
					database_name NVARCHAR(128),
					query_text NVARCHAR(MAX),
					custom_query_type NVARCHAR(64),
					wait_category NVARCHAR(128),
					total_wait_time_ms FLOAT,
					avg_wait_time_ms FLOAT,
					wait_event_count BIGINT,
					collection_timestamp DATETIME
				);
				
				DECLARE db_cursor CURSOR FOR
				SELECT
					name
				FROM
					sys.databases
				WHERE
					state_desc = 'ONLINE'
					AND is_query_store_on = 1
					AND database_id > 4;
				
				OPEN db_cursor;
				
				FETCH NEXT
				FROM
					db_cursor INTO @dbName;
				
				WHILE @@FETCH_STATUS = 0 BEGIN
				SET
					@sql = N'USE ' + QUOTENAME(@dbName) + ';
				
				WITH WaitStats AS (
					SELECT
						''' + QUOTENAME(@dbName) + ''' AS database_name,
						qs.query_sql_text AS query_text,
						qsq.query_hash AS query_id,
						ws.wait_category_desc AS wait_category,
						CAST(SUM(ws.total_query_wait_time_ms) AS FLOAT) AS total_wait_time_ms,
						CASE
							WHEN SUM(rs.count_executions) > 0 THEN CAST(
								SUM(ws.total_query_wait_time_ms) / SUM(rs.count_executions) AS FLOAT
							)
							ELSE 0
						END AS avg_wait_time_ms,
						SUM(rs.count_executions) AS wait_event_count,
						GETUTCDATE() AS collection_timestamp,
						qsq.query_hash
					FROM
						sys.query_store_wait_stats ws
						INNER JOIN sys.query_store_plan qsp ON ws.plan_id = qsp.plan_id
						INNER JOIN sys.query_store_query qsq ON qsp.query_id = qsq.query_id
						INNER JOIN sys.query_store_query_text qs ON qsq.query_text_id = qs.query_text_id
						INNER JOIN sys.query_store_runtime_stats rs ON ws.plan_id = rs.plan_id
						AND ws.runtime_stats_interval_id = rs.runtime_stats_interval_id
					WHERE
						rs.last_execution_time >= DATEADD(SECOND, -%d, GETUTCDATE())
					GROUP BY
						qs.query_sql_text,
						qsq.query_hash,
						ws.wait_category_desc
				),
				DatabaseInfo AS (
					SELECT
						DISTINCT cp.plan_handle,
						DB_NAME(t.dbid) AS database_name,
						th.query_hash
					FROM
						sys.dm_exec_cached_plans cp
						CROSS APPLY sys.dm_exec_query_plan(cp.plan_handle) p
						CROSS APPLY sys.dm_exec_sql_text(cp.plan_handle) t
						CROSS APPLY (
							SELECT
								query_hash
							FROM
								sys.dm_exec_query_stats qs
							WHERE
								qs.plan_handle = cp.plan_handle
						) th
					WHERE
						DB_NAME(t.dbid) NOT IN ('' master '', '' tempdb '', '' model '', '' msdb '')
				)
				SELECT
					ws.query_id,
					di.database_name,
					ws.query_text,
					'' waitTypesDetails '' AS custom_query_type,
					ws.wait_category,
					ws.total_wait_time_ms,
					ws.avg_wait_time_ms,
					ws.wait_event_count,
					ws.collection_timestamp
				FROM
					WaitStats ws
					LEFT JOIN DatabaseInfo di ON ws.query_hash = di.query_hash
				WHERE
					di.database_name IS NOT NULL;';
				
				INSERT INTO
					@resultTable EXEC sp_executesql @sql;
				
				FETCH NEXT
				FROM
					db_cursor INTO @dbName;
				
				END CLOSE db_cursor;
				
				DEALLOCATE db_cursor;
				
				SELECT
					TOP 10 *
				FROM
					@resultTable
				ORDER BY
					total_wait_time_ms DESC;`,
		Type: "waitAnalysis",
	},
	{
		Name: "MSSQLBlockingSessionQueries",
		Query: `WITH blocking_info AS (
				SELECT
					req.blocking_session_id AS blocking_spid,
					req.session_id AS blocked_spid,
					req.wait_type AS wait_type,
					req.wait_time / 1000.0 AS wait_time_in_seconds,
					req.start_time AS start_time,
					sess.status AS status,
					req.command AS command_type,
					req.database_id AS database_id,
					req.sql_handle AS blocked_sql_handle,
					blocking_req.sql_handle AS blocking_sql_handle,
					blocking_req.start_time AS blocking_start_time
				FROM
					sys.dm_exec_requests AS req
					LEFT JOIN sys.dm_exec_requests AS blocking_req ON blocking_req.session_id = req.blocking_session_id
					LEFT JOIN sys.dm_exec_sessions AS sess ON sess.session_id = req.session_id
				WHERE
					req.blocking_session_id != 0
			)
			SELECT
				blocking_info.blocking_spid,
				blocking_sessions.status AS blocking_status,
				blocking_info.blocked_spid,
				blocked_sessions.status AS blocked_status,
				blocking_info.wait_type,
				blocking_info.wait_time_in_seconds,
				blocking_info.command_type,
				DB_NAME(blocking_info.database_id) AS database_name,
				CASE
					WHEN blocking_sql.text IS NULL THEN input_buffer.event_info
					ELSE blocking_sql.text
				END AS blocking_query_text,
				blocked_sql.text AS blocked_query_text
			FROM
				blocking_info
				JOIN sys.dm_exec_sessions AS blocking_sessions ON blocking_sessions.session_id = blocking_info.blocking_spid
				JOIN sys.dm_exec_sessions AS blocked_sessions ON blocked_sessions.session_id = blocking_info.blocked_spid
				OUTER APPLY sys.dm_exec_sql_text(blocking_info.blocking_sql_handle) AS blocking_sql
				OUTER APPLY sys.dm_exec_sql_text(blocking_info.blocked_sql_handle) AS blocked_sql
				OUTER APPLY sys.dm_exec_input_buffer(blocking_info.blocking_spid, NULL) AS input_buffer
			ORDER BY
				blocking_info.blocking_spid,
				blocking_info.blocked_spid;`,
		Type: "blockingSessions",
	},
}
