package io.trino.plugin.exasol;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.annotation.Nullable;

import java.util.Optional;

public class ExasolConfig
{
    private int parallelConnectionsWorkerCount = 0;
    private String jdbcDriverLogDir = null;

    public int getParallelConnectionsWorkerCount()
    {
        return parallelConnectionsWorkerCount;
    }

    @ConfigDescription("Maximum number of workers to use for parallel JDBC import. Set to 0 to deactivate parallel import.")
    @Config("exasol.parallel_connections.worker_count")
    public void setParallelConnectionsWorkerCount(int parallelConnectionsWorkerCount)
    {
        this.parallelConnectionsWorkerCount = parallelConnectionsWorkerCount;
    }

    public Optional<String> getJdbcDriverLogDir()
    {
        return Optional.ofNullable(jdbcDriverLogDir);
    }

    @ConfigDescription("Set a log directory to enable logging for the Exasol JDBC driver.")
    @Config("exasol.jdbc_driver.log_dir")
    @Nullable
    public void setJdbcDriverLogDir(String jdbcDriverLogDir)
    {
        this.jdbcDriverLogDir = jdbcDriverLogDir;
    }
}
