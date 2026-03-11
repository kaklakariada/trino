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

import com.exasol.jdbc.EXAConnection;
import com.exasol.jdbc.EXADriver;
import com.google.inject.Inject;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.spi.connector.ConnectorSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static java.util.Objects.requireNonNull;

public class ParallelConnectionFactory
{
    private static final Logger log = LoggerFactory.getLogger(ParallelConnectionFactory.class);

    private final CredentialProvider credentialProvider;
    private final OpenTelemetry openTelemetry;

    @Inject
    public ParallelConnectionFactory(CredentialProvider credentialProvider, OpenTelemetry openTelemetry)
    {
        this.credentialProvider = requireNonNull(credentialProvider);
        this.openTelemetry = requireNonNull(openTelemetry);
    }

    public List<Connection> createConnections(ConnectorSession session, Connection connection)
    {
        try {
            EXAConnection exaCon = connection.unwrap(EXAConnection.class);
            int maxWorkers = ExasolSessionProperties.getParallelImportWorkerCount(session);
            int parallelConnectionCount = exaCon.RequestParallelConnections(maxWorkers);
            String[] hosts = exaCon.GetAvailableWorkerHosts();
            long token = exaCon.GetWorkerToken();
            long sessionId = exaCon.getSessionID();
            List<Connection> subConnections = new ArrayList<>(parallelConnectionCount);
            for (String host : hosts) {
                log.info("Creating subconnection to host {}...", host);
                subConnections.add(createSubConnection(session, host, token, sessionId));
            }
            return subConnections;
        }
        catch (SQLException e) {
            throw new RuntimeException("Error creating subconnections: " + e.getMessage(), e);
        }
    }

    private Connection createSubConnection(ConnectorSession session, String host, long token, long sessionId)
    {
        String subConnectionUrl = "jdbc:exa-worker:" + host;
        try {
            return DriverConnectionFactory.builder(
                            new EXADriver(),
                            subConnectionUrl,
                            credentialProvider)
                    .setConnectionProperties(buildConnectionProperties(token, sessionId))
                    .setOpenTelemetry(openTelemetry)
                    .build()
                    .openConnection(session);
        }
        catch (SQLException e) {
            log.error("Failed to create subconnection to {}: {}", subConnectionUrl, e.getMessage(), e);
            throw new RuntimeException("Failed connecting to %s: %s".formatted(subConnectionUrl, e.getMessage()), e);
        }
    }

    private static Properties buildConnectionProperties(long token, long sessionId)
    {
        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("workertoken", String.valueOf(token));
        connectionProperties.setProperty("sessionid", String.valueOf(sessionId));
        connectionProperties.setProperty("autocommit", "0");
        // TODO: validate certificate
        connectionProperties.setProperty("validateservercertificate", "0");
        connectionProperties.setProperty("debug", "1");
        connectionProperties.setProperty("logdir", "/Users/chp/dev/trino/jdbclog");
        return connectionProperties;
    }
}
