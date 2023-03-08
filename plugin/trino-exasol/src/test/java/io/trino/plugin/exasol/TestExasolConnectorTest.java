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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import io.trino.testing.sql.TestView;
import org.intellij.lang.annotations.Language;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Optional;
import java.util.OptionalInt;

import static io.airlift.testing.Closeables.closeAll;
import static io.trino.plugin.exasol.ExasolQueryRunner.createExasolQueryRunner;
import static io.trino.plugin.exasol.TestingExasolServer.TEST_SCHEMA;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_MERGE;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.joining;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestExasolConnectorTest
        extends BaseJdbcConnectorTest
{
    private TestingExasolServer exasolServer;

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

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_CREATE_SCHEMA:
            case SUPPORTS_ADD_COLUMN:
            case SUPPORTS_NOT_NULL_CONSTRAINT:
            case SUPPORTS_SET_COLUMN_TYPE:
                return true;

            case SUPPORTS_CREATE_VIEW:
            case SUPPORTS_MERGE:
            case SUPPORTS_UPDATE:
            case SUPPORTS_CANCELLATION:
            case SUPPORTS_MULTI_STATEMENT_WRITES:
            case SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS:
            case SUPPORTS_NEGATIVE_DATE:
            case SUPPORTS_ROW_TYPE:
            case SUPPORTS_ADD_COLUMN_WITH_COMMENT:
            case SUPPORTS_CREATE_TABLE_WITH_COLUMN_COMMENT:
            case SUPPORTS_ARRAY:
                return false;

            case SUPPORTS_AGGREGATION_PUSHDOWN:
            case SUPPORTS_AGGREGATION_PUSHDOWN_STDDEV:
            case SUPPORTS_AGGREGATION_PUSHDOWN_VARIANCE:
            case SUPPORTS_AGGREGATION_PUSHDOWN_COVARIANCE:
            case SUPPORTS_AGGREGATION_PUSHDOWN_COUNT_DISTINCT:
            case SUPPORTS_AGGREGATION_PUSHDOWN_REGRESSION:
            case SUPPORTS_AGGREGATION_PUSHDOWN_CORRELATION:
            case SUPPORTS_JOIN_PUSHDOWN:
                return true;

            case SUPPORTS_TOPN_PUSHDOWN:
            case SUPPORTS_JOIN_PUSHDOWN_WITH_DISTINCT_FROM:
                return false;

            // Parallel writing is not supported due to restrictions of the Exasol JDBC driver.
            case SUPPORTS_INSERT:
            case SUPPORTS_CREATE_TABLE_WITH_DATA:
                return false;

            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup setup)
    {
        if (typeUnsupported(setup.getTrinoTypeName())) {
            return Optional.of(setup.asUnsupported());
        }
        if (setup.getTrinoTypeName().equals("char(3)") && setup.getSampleValueLiteral().equals("'ab'")) {
            // Exasol fills char(3) with spaces, causing test to fail
            return Optional.of(new DataMappingTestSetup("char(3)", "'abc'", "'zzz'"));
        }
        return Optional.of(setup);
    }

    @Override
    protected Optional<SetColumnTypeSetup> filterSetColumnTypesDataProvider(SetColumnTypeSetup setup)
    {
        if (typeUnsupported(setup.sourceColumnType()) || typeUnsupported(setup.newColumnType())) {
            return Optional.of(setup.asUnsupported());
        }
        if (setup.newColumnType().equals("integer")) {
            // Exasol reports integer columns as decimal
            setup = setup.withNewColumnType("decimal(10,0)");
        }
        if (setup.newColumnType().equals("bigint")) {
            // Exasol reports bigint columns as decimal
            setup = setup.withNewColumnType("decimal(19,0)");
        }
        if (setup.newValueLiteral().equals("CAST(CAST(SMALLINT '32767' AS smallint) AS integer)")) {
            // Exasol returns Java type BigDecimal instead of Integer
            setup = setup.withNewValueLiteral("CAST(SMALLINT '32767' AS decimal)");
        }
        if (setup.newValueLiteral().equals("CAST(CAST(2147483647 AS integer) AS bigint)")) {
            // Exasol returns Java type BigDecimal instead of Long
            setup = setup.withNewValueLiteral("CAST(2147483647 AS decimal)");
        }
        if (setup.newValueLiteral().equals("CAST(CAST(BIGINT '-2147483648' AS bigint) AS integer)")) {
            // Exasol returns Java type BigDecimal instead of BigInteger
            setup = setup.withNewValueLiteral("CAST(BIGINT '-2147483648' AS decimal)");
        }
        if (setup.sourceColumnType().equals("timestamp(3)") && setup.newColumnType().equals("timestamp(6)")) {
            // Exasol does not support TIMESTAMP with microsecond precision
            return Optional.of(setup.withNewColumnType("date").withNewValueLiteral("date '2020-02-12'"));
        }
        if (setup.sourceColumnType().equals("char(20)") && setup.newColumnType().equals("varchar")) {
            // Exasol requires length for VARCHAR type
            return Optional.of(setup.withNewColumnType("varchar(25)"));
        }
        if (setup.newColumnType().equals("decimal(38,3)")) {
            // Exasol supports max. precision 36
            return Optional.of(setup.withNewColumnType("decimal(36,3)"));
        }
        return Optional.of(setup);
    }

    private boolean typeUnsupported(String type)
    {
        if (type.equals("time") || type.startsWith("time(")) {
            // Exasol does not have a TIME type
            return true;
        }
        if (type.startsWith("timestamp") && type.endsWith("with time zone")) {
            // Exasol only supports TIMESTAMP WITH _LOCAL_ TIMEZONE
            return true;
        }
        if (type.startsWith("timestamp(6)")) {
            // Exasol supports timestamps with milliseconds precision, not more.
            return true;
        }
        if (type.startsWith("array") || type.startsWith("row")) {
            // Exasol does not support composite types
            return true;
        }
        ImmutableSet<String> unsupportedTypes = ImmutableSet.of("varbinary", "tinyint", "real");
        return unsupportedTypes.contains(type);
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return new TestTable(
                onRemoteDatabase(),
                TestingExasolServer.TEST_SCHEMA + ".test_default_cols",
                "(col_required decimal(20,0) NOT NULL," +
                        "col_nullable decimal(20,0)," +
                        "col_default decimal(20,0) DEFAULT 43," +
                        "col_nonnull_default decimal(20,0) DEFAULT 42 NOT NULL ," +
                        "col_required2 decimal(20,0) NOT NULL)");
    }

    // Override test because INSERT is not supported
    @Override
    public void testNativeQuerySelectUnsupportedType()
    {
        try (TestTable testTable = createTableWithUnsupportedColumn()) {
            String unqualifiedTableName = testTable.getName().replaceAll("^\\w+\\.", "");
            // Check that column 'two' is not supported.
            assertQuery("SELECT column_name FROM information_schema.columns WHERE table_name = '" + unqualifiedTableName + "'", "VALUES 'one', 'three'");
        }
    }

    @Override
    protected TestTable createTableWithUnsupportedColumn()
    {
        return new TestTable(
                onRemoteDatabase(),
                TEST_SCHEMA + ".test_unsupported_col",
                "(one NUMBER(19), two GEOMETRY, three VARCHAR(10 CHAR))");
    }

    @Override
    public void testShowColumns()
    {
        // Test expects
        // - bigint instead of decimal(19,0) for columns orderkey, custkey
        // - varchar instead of varchar(n) for columns orderstatus, orderpriority and comment
        // - double instead of decimal(10,2) for column totalprice
        // - integer instead of decimal(10,0) for column shippriority
        MaterializedResult actual = computeActual("SHOW COLUMNS FROM orders");
        MaterializedResult expected = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "decimal(19,0)", "", "")
                .row("custkey", "decimal(19,0)", "", "")
                .row("orderstatus", "varchar(1)", "", "")
                .row("totalprice", "decimal(10,2)", "", "")
                .row("orderdate", "date", "", "")
                .row("orderpriority", "varchar(15)", "", "")
                .row("clerk", "varchar(15)", "", "")
                .row("shippriority", "decimal(10,0)", "", "")
                .row("comment", "varchar(79)", "", "")
                .build();
        assertThat(actual).isEqualTo(expected);
    }

    @Override
    public void testDescribeTable()
    {
        // Exasol reports bigint columns as decimal(19,0)
        MaterializedResult expectedColumns = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "decimal(19,0)", "", "")
                .row("custkey", "decimal(19,0)", "", "")
                .row("orderstatus", "varchar(1)", "", "")
                .row("totalprice", "decimal(10,2)", "", "")
                .row("orderdate", "date", "", "")
                .row("orderpriority", "varchar(15)", "", "")
                .row("clerk", "varchar(15)", "", "")
                .row("shippriority", "decimal(10,0)", "", "")
                .row("comment", "varchar(79)", "", "")
                .build();
        MaterializedResult actualColumns = computeActual("DESCRIBE orders");
        assertThat(actualColumns).isEqualTo(expectedColumns);
    }

    @Override
    public void testShowCreateTable()
    {
        // Exasol reports bigint columns as decimal(19, 0) and integer as decimal(10, 0)
        String catalog = getSession().getCatalog().orElseThrow();
        String schema = getSession().getSchema().orElseThrow();
        assertThat(computeScalar("SHOW CREATE TABLE orders"))
                // If the connector reports additional column properties, the expected value needs to be adjusted in the test subclass
                .isEqualTo(format("""
                                CREATE TABLE %s.%s.orders (
                                   orderkey decimal(19, 0),
                                   custkey decimal(19, 0),
                                   orderstatus varchar(1),
                                   totalprice decimal(10, 2),
                                   orderdate date,
                                   orderpriority varchar(15),
                                   clerk varchar(15),
                                   shippriority decimal(10, 0),
                                   comment varchar(79)
                                )""",
                        catalog, schema));
    }

    @Override
    public void testSetColumnType()
    {
        // Exasol reports bigint columns as decimal(36,0)
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_set_column_type_", "(col integer)")) {
            assertUpdate("ALTER TABLE " + table.getName() + " ALTER COLUMN col SET DATA TYPE bigint");
            assertEquals(getColumnType(table.getName(), "col"), "decimal(36,0)");
        }
    }

    @Override
    public void testAggregationWithUnsupportedResultType()
    {
        assertThat(query("SELECT array_agg(nationkey) FROM nation"))
                .skipResultsCorrectnessCheckForPushdown() // array_agg doesn't have a deterministic order of elements in result array
                .isNotFullyPushedDown(AggregationNode.class);
        assertThat(query("SELECT histogram(regionkey) FROM nation")).isNotFullyPushedDown(AggregationNode.class);
        assertThat(query("SELECT multimap_agg(regionkey, nationkey) FROM nation"))
                .skipResultsCorrectnessCheckForPushdown() // multimap_agg doesn't have a deterministic order of values for a key
                .isNotFullyPushedDown(AggregationNode.class);
        // Overridden because for approx_set(bigint) a ProjectNode is present above table scan because Exasol doesn't support bigint
        assertThat(query("SELECT approx_set(nationkey) FROM nation")).isNotFullyPushedDown(AggregationNode.class, ProjectNode.class);
    }

    @Test
    public void testViews()
    {
        try (TestView view = new TestView(onRemoteDatabase(), TEST_SCHEMA + ".test_view", "SELECT 'O' as status")) {
            assertQuery("SELECT status FROM " + view.getName(), "SELECT 'O'");
        }
    }

    @Test
    public void testPredicatePushdown()
    {
        // varchar equality
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name = 'ROMANIA'"))
                .matches("VALUES (CAST(3 AS DECIMAL(19,0)), CAST(19 AS DECIMAL(19,0)), CAST('ROMANIA' AS varchar(25)))")
                .isFullyPushedDown();

        // varchar range
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name BETWEEN 'POLAND' AND 'RPA'"))
                .matches("VALUES (CAST(3 AS DECIMAL(19,0)), CAST(19 AS DECIMAL(19,0)), CAST('ROMANIA' AS varchar(25)))")
                .isFullyPushedDown();

        // varchar different case
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name = 'romania'"))
                .returnsEmptyResult()
                .isFullyPushedDown();

        // date equality
        assertThat(query("SELECT orderkey FROM orders WHERE orderdate = DATE '1992-09-29'"))
                .matches("VALUES CAST(1250 AS DECIMAL(19,0)), 34406, 38436, 57570")
                .isFullyPushedDown();

        // predicate over aggregation key (likely to be optimized before being pushed down into the connector)
        assertThat(query("SELECT * FROM (SELECT regionkey, sum(nationkey) FROM nation GROUP BY regionkey) WHERE regionkey = 3"))
                .matches("VALUES (CAST(3 AS decimal(19,0)), CAST(77 AS decimal(38,0)))")
                .isFullyPushedDown();

        // predicate over aggregation result
        assertThat(query("SELECT regionkey, sum(nationkey) FROM nation GROUP BY regionkey HAVING sum(nationkey) = 77"))
                .matches("VALUES (CAST(3 AS decimal(19,0)), CAST(77 AS decimal(38,0)))")
                .isFullyPushedDown();
    }

    @Test
    public void testPredicatePushdownForNumerics()
    {
        predicatePushdownTest("DECIMAL(9, 3)", "123.321", "<=", "124");
        predicatePushdownTest("DECIMAL(9, 3)", "123.321", "<=", "123.321");
        predicatePushdownTest("DECIMAL(9, 3)", "123.321", "=", "123.321");
        predicatePushdownTest("DECIMAL(30, 10)", "123456789.987654321", "<=", "123456790");
        predicatePushdownTest("DECIMAL(30, 10)", "123456789.987654321", "<=", "123456789.987654321");
        predicatePushdownTest("DECIMAL(30, 10)", "123456789.987654321", "=", "123456789.987654321");
        predicatePushdownTest("FLOAT", "123456789.987654321", "<=", "CAST(123456789.99 AS REAL)");
        predicatePushdownTest("DOUBLE PRECISION", "123456789.987654321", "<=", "CAST(123456789.99 AS DOUBLE)");
        predicatePushdownTest("NUMBER(5,3)", "5.0", "=", "CAST(5.0 AS DECIMAL(5,3))");
    }

    @Test
    public void testPredicatePushdownForChars()
    {
        predicatePushdownTest("CHAR(1)", "'0'", "=", "'0'");
        predicatePushdownTest("CHAR(1)", "'0'", "<=", "'0'");
        predicatePushdownTest("CHAR(7)", "'my_char'", "=", "CAST('my_char' AS CHAR(7))");
        predicatePushdownTest("VARCHAR(7)", "'my_char'", "=", "CAST('my_char' AS VARCHAR(7))");
    }

    @Override
    @Language("RegExp")
    protected String errorMessageForInsertIntoNotNullColumn(String columnName)
    {
        return format("constraint violation - not null \\(column %s in table.*", columnName.toUpperCase(ENGLISH));
    }

    @Override
    @Language("RegExp")
    protected String errorMessageForCreateTableAsSelectNegativeDate(String date)
    {
        return format("negative date %s as select blubb", date);
    }

    @Override
    @Language("RegExp")
    protected String errorMessageForInsertNegativeDate(String date)
    {
        return format("negative date %s insert blubb", date);
    }

    @Override
    protected void verifyAddNotNullColumnToNonEmptyTableFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageContaining("constraint violation - not null (NN/PK on table");
    }

    @Override
    protected void verifyConcurrentAddColumnFailurePermissible(Exception e)
    {
        assertThat(e)
                .hasMessage("xx: The DDL cannot be run concurrently with other DDLs\n");
    }

    @Override
    protected void verifySetColumnTypeFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageStartingWith("data exception - ");
    }

    @Override
    protected OptionalInt maxSchemaNameLength()
    {
        return OptionalInt.of(128);
    }

    @Override
    protected void verifySchemaNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageContaining("maximum length of identifier exceeded");
    }

    @Override
    protected OptionalInt maxTableNameLength()
    {
        return OptionalInt.of(128);
    }

    @Override
    protected void verifyTableNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageStartingWith("maximum length of identifier exceeded");
    }

    @Override
    protected OptionalInt maxColumnNameLength()
    {
        return OptionalInt.of(128);
    }

    @Override
    protected void verifyColumnNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageContaining("maximum length of identifier exceeded");
    }

    @Override
    protected Optional<String> filterColumnNameTestData(String columnName)
    {
        // Exasol does not support '.' in identifiers
        if (columnName.contains(".")) {
            return Optional.empty();
        }
        else {
            return Optional.of(columnName);
        }
    }

    @Override
    public void testDeleteWithVarcharGreaterAndLowerPredicate()
    {
        throw new SkipException("Exasol does not support DELETE with < or > predicate");
    }

    @Override
    public void testDeleteWithVarcharInequalityPredicate()
    {
        throw new SkipException("Exasol does not support DELETE with != predicate");
    }

    // Override to specify test schema
    @Override
    public void testNativeQueryCreateStatement()
    {
        assertFalse(getQueryRunner().tableExists(getSession(), "numbers"));
        assertThatThrownBy(() -> query("SELECT * FROM TABLE(system.query(query => 'CREATE TABLE " + TEST_SCHEMA + ".numbers(n INTEGER)'))"))
                .hasMessageContaining("Query not supported: ResultSetMetaData not available for query: CREATE TABLE ");
        assertFalse(getQueryRunner().tableExists(getSession(), "numbers"));
    }

    @Override
    public void testMergeCasts()
    {
        // Exasol doesn't support type real, using a different type here.
        skipTestUnless(hasBehavior(SUPPORTS_MERGE));

        String targetTable = "merge_cast_target_" + randomNameSuffix();
        String sourceTable = "merge_cast_source_" + randomNameSuffix();

        assertUpdate(createTableForWrites(format("CREATE TABLE %s (col1 INT, col2 DOUBLE, col3 INT, col4 BIGINT, col5 DOUBLE, col6 DOUBLE)", targetTable)));

        assertUpdate(format("INSERT INTO %s VALUES (1, 2, 3, 4, 5, 6)", targetTable), 1);

        assertUpdate(createTableForWrites(format("CREATE TABLE %s (col1 BIGINT, col2 DOUBLE, col3 DOUBLE, col4 INT, col5 INT, col6 DOUBLE)", sourceTable)));

        assertUpdate(format("INSERT INTO %s VALUES (2, 3, 4, 5, 6, 7)", sourceTable), 1);

        assertUpdate(format("MERGE INTO %s t USING %s s", targetTable, sourceTable) +
                        "    ON (t.col1 + 1 = s.col1)" +
                        "    WHEN MATCHED THEN UPDATE SET col1 = s.col1, col2 = s.col2, col3 = s.col3, col4 = s.col4, col5 = s.col5, col6 = s.col6",
                1);

        assertQuery("SELECT * FROM " + targetTable, "VALUES (2, 3.0, 4, 5, 6.0, 7.0)");

        assertUpdate("DROP TABLE " + sourceTable);
        assertUpdate("DROP TABLE " + targetTable);
    }

    @Override
    public void testDateYearOfEraPredicate()
    {
        // Override because Exasol does not support negative dates
        assertQuery("SELECT orderdate FROM orders WHERE orderdate = DATE '1997-09-14'", "VALUES DATE '1997-09-14'");
    }

    @Override
    public void testInformationSchemaFiltering()
    {
        // Exasol reports bigint columns as decimal(19,0)
        assertQuery(
                "SELECT table_name FROM information_schema.tables WHERE table_name = 'orders' LIMIT 1",
                "SELECT 'orders' table_name");
        assertQuery(
                "SELECT table_name FROM information_schema.columns WHERE data_type = 'decimal(19,0)' AND table_name = 'customer' AND column_name = 'custkey' LIMIT 1",
                "SELECT 'customer' table_name");
    }

    // Override because Exasol's AVG() always returns DOUBLE, causing slightly different results for type decimal(30, 10):
    // 111728394.9938271616 vs. 111728394.9938271605
    @Override
    public void testNumericAggregationPushdown()
    {
        String schemaName = getSession().getSchema().orElseThrow();
        // empty table
        try (TestTable emptyTable = createAggregationTestTable(schemaName + ".test_num_agg_pd", ImmutableList.of())) {
            assertThat(query("SELECT min(short_decimal), min(long_decimal), min(a_bigint), min(t_double) FROM " + emptyTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT max(short_decimal), max(long_decimal), max(a_bigint), max(t_double) FROM " + emptyTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT sum(short_decimal), sum(long_decimal), sum(a_bigint), sum(t_double) FROM " + emptyTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT avg(short_decimal), avg(long_decimal), avg(a_bigint), avg(t_double) FROM " + emptyTable.getName())).isFullyPushedDown();
        }

        try (TestTable testTable = createAggregationTestTable(schemaName + ".test_num_agg_pd",
                ImmutableList.of("100.000, 100000000.000000000, 100.000, 100000000", "123.321, 123456789.987654321, 123.321, 123456789"))) {
            assertThat(query("SELECT min(short_decimal), min(long_decimal), min(a_bigint), min(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT max(short_decimal), max(long_decimal), max(a_bigint), max(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT sum(short_decimal), sum(long_decimal), sum(a_bigint), sum(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT avg(short_decimal), avg(a_bigint), avg(t_double) FROM " + testTable.getName())).isFullyPushedDown();

            // smoke testing of more complex cases
            // WHERE on aggregation column
            assertThat(query("SELECT min(short_decimal), min(long_decimal) FROM " + testTable.getName() + " WHERE short_decimal < 110 AND long_decimal < 124")).isFullyPushedDown();
            // WHERE on non-aggregation column
            assertThat(query("SELECT min(long_decimal) FROM " + testTable.getName() + " WHERE short_decimal < 110")).isFullyPushedDown();
            // GROUP BY
            assertThat(query("SELECT short_decimal, min(long_decimal) FROM " + testTable.getName() + " GROUP BY short_decimal")).isFullyPushedDown();
            // GROUP BY with WHERE on both grouping and aggregation column
            assertThat(query("SELECT short_decimal, min(long_decimal) FROM " + testTable.getName() + " WHERE short_decimal < 110 AND long_decimal < 124 GROUP BY short_decimal")).isFullyPushedDown();
            // GROUP BY with WHERE on grouping column
            assertThat(query("SELECT short_decimal, min(long_decimal) FROM " + testTable.getName() + " WHERE short_decimal < 110 GROUP BY short_decimal")).isFullyPushedDown();
            // GROUP BY with WHERE on aggregation column
            assertThat(query("SELECT short_decimal, min(long_decimal) FROM " + testTable.getName() + " WHERE long_decimal < 124 GROUP BY short_decimal")).isFullyPushedDown();
        }
    }

    private void predicatePushdownTest(String exasolType, String exasolLiteral, String operator, String filterLiteral)
    {
        String tableName = "test_pdown_" + exasolType.replaceAll("[^a-zA-Z0-9]", "");
        try (TestTable table = new TestTable(onRemoteDatabase(), TEST_SCHEMA + "." + tableName, format("(c %s)", exasolType))) {
            onRemoteDatabase().execute(format("INSERT INTO %s VALUES (%s)", table.getName(), exasolLiteral));

            assertThat(query(format("SELECT * FROM %s WHERE c %s %s", table.getName(), operator, filterLiteral)))
                    .isFullyPushedDown();
        }
    }

    @Override
    public void testCharTrailingSpace()
    {
        throw new SkipException("Exasol pads CHAR(n) with spaces");
    }

    @Override
    public void testNativeQuerySimple()
    {
        // Override because Exasol requires named columns
        assertQuery("SELECT * FROM TABLE(system.query(query => 'SELECT 1 AS res'))", "VALUES 1");
    }

    @Test
    public void testNativeMultipleInClauses()
    {
        String longInClauses = range(0, 10)
                .mapToObj(value -> getLongInClause(value * 1_000, 1_000))
                .collect(joining(" OR "));
        onRemoteDatabase().execute(format("SELECT count(*) FROM %s.orders WHERE %s", TEST_SCHEMA, longInClauses));
    }

    private String getLongInClause(int start, int length)
    {
        String longValues = range(start, start + length)
                .mapToObj(Integer::toString)
                .collect(joining(", "));
        return "orderkey IN (" + longValues + ")";
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return exasolServer::execute;
    }
}
