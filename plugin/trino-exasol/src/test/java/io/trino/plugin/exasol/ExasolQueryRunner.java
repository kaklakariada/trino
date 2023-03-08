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

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.tpch.TpchTable;

import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.exasol.ExasolTpchTables.copyAndIngestTpchData;
import static io.trino.plugin.exasol.TestingExasolServer.TEST_SCHEMA;
import static io.trino.testing.TestingSession.testSessionBuilder;

public final class ExasolQueryRunner
{
    private static final Logger log = Logger.get(ExasolQueryRunner.class);

    private ExasolQueryRunner() {}

    public static DistributedQueryRunner createExasolQueryRunner(
            TestingExasolServer server,
            Map<String, String> extraProperties,
            Iterable<TpchTable<?>> tables)
            throws Exception
    {
        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner = DistributedQueryRunner.builder(createSession())
                    .setExtraProperties(extraProperties)
                    .build();

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch", ImmutableMap.of());

            queryRunner.installPlugin(new ExasolPlugin());
            queryRunner.createCatalog("exasol", "exasol", server.connectionProperties());

            log.info("Loading data from exasol.%s...", TEST_SCHEMA);
            for (TpchTable<?> table : tables) {
                log.info("Running import for %s", table.getTableName());
                MaterializedResult rows = queryRunner.execute(ExasolTpchTables.getSelectQuery(table.getTableName()));
                copyAndIngestTpchData(rows, server, table.getTableName());
                log.info("Imported %s rows for %s", rows.getRowCount(), table.getTableName());
            }
            log.info("Loading from exasol.%s complete", TEST_SCHEMA);
            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner, server);
            throw e;
        }
    }

    public static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog("exasol")
                .setSchema(TEST_SCHEMA)
                .build();
    }

    public static void main(String[] args)
            throws Exception
    {
        TestingExasolServer server = TestingExasolServer.start();
        DistributedQueryRunner queryRunner = createExasolQueryRunner(
                server,
                ImmutableMap.of("http-server.http.port", "8080"),
                TpchTable.getTables());

        Logger log = Logger.get(ExasolQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
