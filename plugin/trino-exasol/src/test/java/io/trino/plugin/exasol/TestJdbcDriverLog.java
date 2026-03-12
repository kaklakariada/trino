package io.trino.plugin.exasol;

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static io.trino.tpch.TpchTable.NATION;
import static org.assertj.core.api.Assertions.assertThat;

public class TestJdbcDriverLog
        extends AbstractTestQueryFramework
{
    @TempDir static Path logDir;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestingExasolServer exasolServer = closeAfterClass(new TestingExasolServer());
        return ExasolQueryRunner.builder(exasolServer)
                .setInitialTables(List.of(NATION))
                .withJdbcDriverLogDir(logDir.toAbsolutePath().toString())
                .build();
    }

    @Test
    void testJdbcDriverCreatesLogFiles()
            throws IOException
    {
        assertQuery("SELECT count(*) FROM nation", "SELECT 25");
        Stream<Path> logFiles = Files.list(logDir);
        assertThat(logFiles).hasSizeGreaterThan(1);
    }
}
