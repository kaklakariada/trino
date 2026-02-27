package io.trino.plugin.exasol;

import com.exasol.jdbc.EXADriver;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.spi.connector.ConnectorSession;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;

import static java.util.Objects.requireNonNull;

class ExasolWorkerNodeConnectionFactory
        implements AutoCloseable
{
    private static final Logger log = LoggerFactory.getLogger(ExasolWorkerNodeConnectionFactory.class);
    private CredentialProvider credentialProvider;
    private OpenTelemetry openTelemetry;

    ExasolWorkerNodeConnectionFactory(CredentialProvider credentialProvider, OpenTelemetry openTelemetry) {
        this.credentialProvider = requireNonNull(credentialProvider, "credentialProvider is null");
        this.openTelemetry = requireNonNull(openTelemetry, "openTelemetry is null");
    }

    Connection openConnection(ConnectorSession session, ExasolWorkerNode workerNode)
            throws SQLException
    {
        String connectionUrl = workerNode.getConnectionUrl();
        log.info("Connecting to worker node at {}", connectionUrl);
        return DriverConnectionFactory.builder(
                        new EXADriver(),
                        connectionUrl,
                        credentialProvider)
                .setOpenTelemetry(openTelemetry)
                .build()
                .openConnection(session);
    }

    @PreDestroy
    public void close() throws SQLException
    {}
}
