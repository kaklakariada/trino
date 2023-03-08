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

import com.exasol.containers.ExasolContainer;
import com.exasol.containers.ExasolService;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.testing.ResourcePresence;
import io.trino.testing.sql.JdbcSqlExecutor;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;

import static java.lang.String.format;

public class TestingExasolServer
        implements Closeable
{
    private static final Logger log = Logger.get(TestingExasolServer.class);
    public static final String TEST_USER = "trino_test";
    /**
     * Name of the test schema. Must not contain an underscore, required by {@link io.trino.plugin.exasol.TestExasolConnectorTest#testShowSchemasLikeWithEscape()}
     */
    public static final String TEST_SCHEMA = "tpch";
    public static final String TEST_PASS = "trino_test_password";

    private final ExasolContainer<?> container;

    private TestingExasolServer()
    {
        container = startContainer();
    }

    private static ExasolContainer<?> startContainer()
    {
        ExasolContainer<?> container = new ExasolContainer<>("7.1.22").withRequiredServices(ExasolService.JDBC);
        container.start();
        return container;
    }

    public static TestingExasolServer start()
    {
        TestingExasolServer server = new TestingExasolServer();
        server.createUserAndSchema();
        return server;
    }

    private void createUserAndSchema()
    {
        executeAsSys(format("CREATE USER %s IDENTIFIED BY \"%s\"", TEST_USER, TEST_PASS));
        executeAsSys(format("GRANT CREATE SESSION TO %s", TEST_USER));
        executeAsSys(format("GRANT CREATE SCHEMA TO %s", TEST_USER));
        executeAsSys(format("GRANT CREATE TABLE TO %s", TEST_USER));
        executeAsSys(format("GRANT IMPORT TO %s", TEST_USER));
        executeAsSys(format("GRANT CREATE VIEW TO %s", TEST_USER));
        executeAsSys(format("GRANT CREATE ANY VIEW TO %s", TEST_USER));
        execute(format("CREATE SCHEMA %s", TEST_SCHEMA));
    }

    public String getJdbcUrl()
    {
        return container.getJdbcUrl();
    }

    public void execute(String sql)
    {
        execute(sql, TEST_USER, TEST_PASS);
    }

    private void executeAsSys(String sql)
    {
        execute(sql, container.getUsername(), container.getPassword());
    }

    public void execute(String sql, String user, String password)
    {
        try (Connection connection = createConnection(user, password);
                Statement statement = connection.createStatement()) {
            try {
                log.info("Executing statement '%s'", sql);
                statement.execute(sql);
            }
            catch (SQLException e) {
                throw new RuntimeException("Failed to execute statement '" + sql + "'", e);
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private Connection createConnection(String user, String password)
            throws SQLException
    {
        return DriverManager.getConnection(getJdbcUrl(), getProperties(user, password));
    }

    public Properties getProperties(String user, String password)
    {
        Properties properties = new Properties();
        properties.setProperty("user", user);
        properties.setProperty("password", password);
        return properties;
    }

    public Map<String, String> connectionProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put("connection-url", getJdbcUrl())
                .put("connection-user", TEST_USER)
                .put("connection-password", TEST_PASS)
                .buildOrThrow();
    }

    public JdbcSqlExecutor getSqlExecutor()
    {
        return new JdbcSqlExecutor(getJdbcUrl(), getProperties(TEST_USER, TEST_PASS));
    }

    @Override
    public void close()
    {
        container.stop();
    }

    @ResourcePresence
    public boolean isRunning()
    {
        return container.getContainerId() != null;
    }
}
