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

import io.trino.plugin.jdbc.JdbcClient;
import io.trino.spi.connector.SourcePage;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

final class TestExasolParallelPageSource
{
    @Test
    void testUnusedSubconnectionsCloseAfterMainConnection()
    {
        List<String> events = new ArrayList<>();
        try (var executor = Executors.newSingleThreadExecutor()) {
            ExasolParallelPageSource source = new ExasolParallelPageSource(
                    List.of(),
                    executor,
                    connection(events, "main connection"),
                    proxy(PreparedStatement.class, (_, method, _) -> {
                        if (method.getName().equals("cancel") || method.getName().equals("close")) {
                            events.add("statement " + method.getName());
                        }
                        return null;
                    }),
                    proxy(ResultSet.class, (_, method, _) -> {
                        if (method.getName().equals("close")) {
                            events.add("result set");
                        }
                        return null;
                    }),
                    List.of(connection(events, "worker 1"), connection(events, "worker 2")));

            source.close();
            source.close();
        }

        assertThat(events).containsExactly("statement cancel", "result set", "statement close", "main connection", "worker 1", "worker 2");
    }

    @Test
    void testEmptyWorkerFinishesAlongsideNonemptyWorker()
            throws Exception
    {
        List<String> events = new ArrayList<>();
        AtomicInteger nonemptyWorkerRows = new AtomicInteger();
        ResultSet emptyResultSet = resultSet(() -> false);
        ResultSet nonemptyResultSet = resultSet(() -> nonemptyWorkerRows.getAndIncrement() == 0);

        try (var executor = Executors.newFixedThreadPool(4)) {
            JdbcClient jdbcClient = proxy(JdbcClient.class, (_, _, _) -> null);
            ExasolResultHandlePageSource emptyWorker = new ExasolResultHandlePageSource(jdbcClient, executor, null, connection(events, "empty worker"), 42, List.of(), () -> emptyResultSet);
            ExasolResultHandlePageSource nonemptyWorker = new ExasolResultHandlePageSource(jdbcClient, executor, null, connection(events, "nonempty worker"), 42, List.of(), () -> nonemptyResultSet);
            ExasolParallelPageSource source = new ExasolParallelPageSource(
                    List.of(emptyWorker, nonemptyWorker),
                    executor,
                    connection(events, "main connection"),
                    proxy(PreparedStatement.class, (_, _, _) -> null),
                    resultSet(() -> false),
                    List.of());
            try {
                source.startReading();
                int positions = 0;
                while (!source.isFinished()) {
                    source.isBlocked().get(5, TimeUnit.SECONDS);
                    SourcePage page = source.getNextSourcePage();
                    if (page != null) {
                        positions += page.getPositionCount();
                    }
                }
                assertThat(positions).isEqualTo(1);
            }
            finally {
                source.close();
            }
        }

        assertThat(events).contains("empty worker", "nonempty worker");
    }

    private static ResultSet resultSet(java.util.function.BooleanSupplier next)
    {
        return proxy(ResultSet.class, (_, method, _) -> {
            if (method.getName().equals("next")) {
                return next.getAsBoolean();
            }
            return null;
        });
    }

    private static Connection connection(List<String> events, String name)
    {
        return proxy(Connection.class, (_, method, _) -> {
            if (method.getName().equals("close")) {
                events.add(name);
            }
            return null;
        });
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type, java.lang.reflect.InvocationHandler handler)
    {
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, handler);
    }
}
