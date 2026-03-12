package io.trino.plugin.exasol;

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.tpch.TpchTable.NATION;

public class TestParallelConnections
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestingExasolServer exasolServer = closeAfterClass(new TestingExasolServer());
        return ExasolQueryRunner.builder(exasolServer)
                .setInitialTables(List.of(NATION))
                .withParallelWorkerCount(2)
                .build();
    }

    @Test
    void testParallelConnections()
    {
        assertQuery("SELECT count(*) FROM nation", "SELECT 25");
    }
}
