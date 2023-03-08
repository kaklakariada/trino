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
import io.trino.plugin.jdbc.BaseJdbcConnectorSmokeTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;

import static io.airlift.testing.Closeables.closeAll;
import static io.trino.plugin.exasol.ExasolQueryRunner.createExasolQueryRunner;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

public class TestExasolConnectorSmokeTest
        extends BaseJdbcConnectorSmokeTest
{
    private TestingExasolServer exasolServer;

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_CREATE_SCHEMA:
                return true;

            case SUPPORTS_CREATE_VIEW:
            case SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS:
                return false;

            // Parallel writing is not supported due to restrictions of the Exasol JDBC driver.
            case SUPPORTS_CREATE_TABLE_WITH_DATA:
            case SUPPORTS_INSERT:
                return false;

            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Test
    public void testCommentColumn()
    {
        String tableName = "test_comment_column_" + randomNameSuffix();
        try {
            assertUpdate("CREATE TABLE " + tableName + "(a integer)");
            assertUpdate("COMMENT ON COLUMN " + tableName + ".a IS 'new comment'");
            assertThat((String) computeActual("SHOW CREATE TABLE " + tableName).getOnlyValue()).contains("COMMENT 'new comment'");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        exasolServer = TestingExasolServer.start();
        return createExasolQueryRunner(
                exasolServer,
                ImmutableMap.of(),
                REQUIRED_TPCH_TABLES);
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
            throws IOException
    {
        closeAll(exasolServer);
        exasolServer = null;
    }
}
