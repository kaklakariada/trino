package io.trino.plugin.exasol;

import com.google.inject.Inject;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcProcedureHandle;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;

import static io.trino.plugin.jdbc.JdbcDynamicFilteringSessionProperties.dynamicFilteringEnabled;
import static java.util.Objects.requireNonNull;

public class ExasolSplitManager implements ConnectorSplitManager
{
    private final JdbcClient jdbcClient;

    @Inject
    public ExasolSplitManager(JdbcClient jdbcClient)
    {
        this.jdbcClient = requireNonNull(jdbcClient, "jdbcClient is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        if (table instanceof JdbcProcedureHandle procedureHandle) {
            return jdbcClient.getSplits(session, procedureHandle);
        }

        JdbcTableHandle tableHandle = (JdbcTableHandle) table;
        ConnectorSplitSource jdbcSplitSource = jdbcClient.getSplits(session, tableHandle);
        if (dynamicFilteringEnabled(session)) {
            return new ExasolDynamicFilteringJdbcSplitSource(jdbcSplitSource, dynamicFilter, tableHandle);
        }
        return jdbcSplitSource;
    }
}
