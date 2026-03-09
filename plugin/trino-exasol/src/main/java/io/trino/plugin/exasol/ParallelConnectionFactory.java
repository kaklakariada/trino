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

    public List<Connection> createConnections(ConnectorSession session, Connection connection) {
        try {
            EXAConnection exaCon = connection.unwrap(EXAConnection.class);
            int maxWorkers = 20; // TODO: Read from configuration
            int parallelConnectionCount = exaCon.RequestParallelConnections(maxWorkers);
            String[] hosts = exaCon.GetAvailableWorkerHosts();
            long token = exaCon.GetWorkerToken();
            long sessionId = exaCon.getSessionID();
            List<Connection> subConnections = new ArrayList<>(parallelConnectionCount);
            for (String host: hosts) {
                log.info("Creating subconnection to host {}...", host);
                subConnections.add(createSubConnection(session, host, token, sessionId));
            }
            return subConnections;
        }
        catch (SQLException e) {
            throw new RuntimeException("Error creating subconnections: "+e.getMessage(), e);
        }
    }

    private Connection createSubConnection(ConnectorSession session, String host, long token, long sessionId)
    {
        // TODO: validate certificate
        String subConnectionUrl = "jdbc:exa:%s;workertoken=%d;sessionid=%d;autocommit=0;encryption=1;validateservercertificate=0".formatted(host, token, sessionId);
        try {
            return DriverConnectionFactory.builder(
                            new EXADriver(),
                            subConnectionUrl,
                            credentialProvider)
                    .setOpenTelemetry(openTelemetry)
                    .build()
                    .openConnection(session);
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed connecting to %s: %s".formatted(subConnectionUrl, e.getMessage()), e);
        }
    }
}
