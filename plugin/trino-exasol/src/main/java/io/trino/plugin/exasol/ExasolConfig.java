/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.exasol;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.annotation.Nullable;

import java.util.Optional;

public class ExasolConfig
{
    private int parallelConnectionsWorkerCount;
    private String jdbcDriverLogDir;

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
