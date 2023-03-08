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

import io.airlift.log.Logger;
import io.trino.spi.type.Type;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.tpch.OrderColumn;
import io.trino.tpch.TpchTable;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import static io.trino.plugin.exasol.TestingExasolServer.TEST_SCHEMA;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

final class ExasolTpchTables
{
    private static final Logger log = Logger.get(ExasolTpchTables.class);

    private ExasolTpchTables() {}

    static String getSelectQuery(String tpchTableName)
    {
        return "SELECT * FROM tpch.tiny." + tpchTableName;
    }

    static void copyAndIngestTpchData(MaterializedResult rows, TestingExasolServer server, String tableName)
    {
        Path tempFile = createCsvFile(rows, tableName);
        StringBuilder columnDefinitions = new StringBuilder();
        for (int i = 0; i < rows.getTypes().size(); i++) {
            if (i != 0) {
                columnDefinitions.append(", ");
            }
            columnDefinitions.append(rows.getColumnNames().get(i) + " " + convertType(tableName, rows.getColumnNames().get(i), rows.getTypes().get(i)));
        }
        String createTableStatement = format("CREATE TABLE %s.%s (%s)", TEST_SCHEMA, tableName, columnDefinitions.toString());
        log.info("Creating table %s using definition '%s'", tableName, createTableStatement);
        server.execute(createTableStatement);
        String importStatement = format("""
                IMPORT INTO %s.%s
                FROM LOCAL CSV FILE '%s'
                ROW SEPARATOR = 'LF'
                COLUMN SEPARATOR = ','""", TEST_SCHEMA, tableName, tempFile.toAbsolutePath());
        server.execute(importStatement);
    }

    private static String convertType(String tableName, String columnName, Type type)
    {
        if (tableName.equals(TpchTable.ORDERS.getTableName())) {
            if (("o_" + columnName).equals(OrderColumn.TOTAL_PRICE.getColumnName())) {
                // Column type is double but tests expect precision 2
                return "decimal(10,2)";
            }
        }
        switch (type.getDisplayName()) {
            case "bigint": // Exasol interprets bigint as decimal(36,0), but tests expect decimal(19,0)
                return "decimal(19,0)";
            case "integer":
                return "decimal(10,0)";
            default:
                return type.getDisplayName();
        }
    }

    private static Path createCsvFile(MaterializedResult rows, String tableName)
    {
        try {
            Path tempFile = Files.createTempFile("exasol_" + tableName, ".csv");
            writeDataAsCsv(rows, tempFile);
            return tempFile;
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void writeDataAsCsv(MaterializedResult rows, Path dataFile)
            throws IOException
    {
        try (BufferedWriter bw = Files.newBufferedWriter(dataFile, UTF_8)) {
            for (MaterializedRow row : rows.getMaterializedRows()) {
                bw.write(convertToCsv(row.getFields()));
                bw.write("\n");
            }
        }
    }

    private static String convertToCsv(List<Object> data)
    {
        return data.stream()
                .map(String::valueOf)
                .map(value -> "\"" + value + "\"")
                .collect(Collectors.joining(","));
    }
}
