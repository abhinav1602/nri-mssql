FROM mcr.microsoft.com/azure-sql-edge
# Set environment variables for enabling features
ENV MSSQL_AGENT_ENABLED=1
ENV MSSQL_QUERY_STORE_ENABLED=1
# Use with extreme caution
ENV MSSQL_XP_CMD_SHELL_ENABLED=1

USER root
# Create a setup directory
RUN mkdir /setup
# Copy configuration files *before* switching users
COPY configure-sql.sh /setup/
COPY entrypoint.sh /setup/
# Set permissions for the scripts
RUN chmod +x /setup/configure-sql.sh
RUN chmod +x /setup/entrypoint.sh
# Switch to the mssql user
USER mssql
# Use the entrypoint script to manage startup and configuration.
# The entrypoint script will run the configure script and then start sqlservr.
ENTRYPOINT ["/setup/entrypoint.sh"]
# Arguments to the entrypoint script (in this case, arguments for sqlservr)
CMD ["/opt/mssql/bin/sqlservr"]