package io.trino.plugin.exasol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcSplit;
import io.trino.spi.predicate.TupleDomain;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ExasolSplit extends JdbcSplit
{
    private final ExasolWorkerNode exasolWorkerNode;

    public ExasolSplit(ExasolWorkerNode exasolWorkerNode)
    {
        this(Optional.empty(), TupleDomain.all(), exasolWorkerNode);
    }

    @JsonCreator
    public ExasolSplit(@JsonProperty("additionalPredicate") Optional<String> additionalPredicate,
            @JsonProperty("dynamicFilter") TupleDomain<JdbcColumnHandle> dynamicFilter,
            @JsonProperty("workerNode") ExasolWorkerNode exasolWorkerNode)
    {
        super(additionalPredicate, dynamicFilter);
        this.exasolWorkerNode = requireNonNull(exasolWorkerNode, "exasolWorkerNode is null");
    }

    public ExasolSplit withDynamicFilter(TupleDomain<JdbcColumnHandle> dynamicFilter)
    {
        return new ExasolSplit(getAdditionalPredicate(), dynamicFilter, exasolWorkerNode);
    }

    @JsonProperty
    public ExasolWorkerNode getExasolWorkerNode()
    {
        return exasolWorkerNode;
    }
}
