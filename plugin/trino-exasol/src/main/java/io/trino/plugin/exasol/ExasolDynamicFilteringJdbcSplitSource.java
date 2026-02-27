package io.trino.plugin.exasol;

import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.TupleDomain;

import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class ExasolDynamicFilteringJdbcSplitSource
        implements ConnectorSplitSource
{
    private final ConnectorSplitSource delegateSplitSource;
    private final DynamicFilter dynamicFilter;
    private final JdbcTableHandle tableHandle;

    public ExasolDynamicFilteringJdbcSplitSource(ConnectorSplitSource delegateSplitSource, DynamicFilter dynamicFilter, JdbcTableHandle tableHandle) {
        this.delegateSplitSource = requireNonNull(delegateSplitSource, "delegateSplitSource is null");
        this.dynamicFilter = requireNonNull(dynamicFilter, "dynamicFilter is null");
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
    }

    @Override
    public CompletableFuture<ConnectorSplitSource.ConnectorSplitBatch> getNextBatch(int maxSize)
    {
        if (!isEligibleForDynamicFilter(tableHandle)) {
            return delegateSplitSource.getNextBatch(maxSize);
        }
        return delegateSplitSource.getNextBatch(maxSize)
                .thenApply(batch -> {
                    TupleDomain<JdbcColumnHandle> dynamicFilterPredicate = dynamicFilter.getCurrentPredicate()
                            .transformKeys(JdbcColumnHandle.class::cast);
                    return new ConnectorSplitSource.ConnectorSplitBatch(
                            batch.getSplits().stream()
                                    // attach dynamic filter constraint to ExasolSplit
                                    .map(split -> {
                                        ExasolSplit jdbcSplit = (ExasolSplit) split;
                                        // If split was a subclass of ExasolSplit, there would be additional information
                                        // that we would need to pass further on.
                                        verify(jdbcSplit.getClass() == ExasolSplit.class, "Unexpected split type %s", jdbcSplit);
                                        return jdbcSplit.withDynamicFilter(dynamicFilterPredicate);
                                    })
                                    .collect(toImmutableList()),
                            batch.isNoMoreSplits());
                });
    }

    @Override
    public void close()
    {
        delegateSplitSource.close();
    }

    @Override
    public boolean isFinished()
    {
        return delegateSplitSource.isFinished();
    }

    public static boolean isEligibleForDynamicFilter(JdbcTableHandle tableHandle)
    {
        // don't pushdown predicate through limit as it could reduce performance
        return tableHandle.getLimit().isEmpty();
    }
}
