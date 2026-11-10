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
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

final class TestExasolParallelPageSource
{
    @Test
    void testUnusedSubconnectionsCloseBeforeMainConnection()
    {
        List<String> events = new CopyOnWriteArrayList<>();
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
                    List.of(connection(events, "worker 1"), connection(events, "worker 2")),
                    false,
                    "test-query",
                    1);

            source.close();
            source.close();
        }

        assertThat(events).contains("worker 1", "worker 2", "statement cancel", "result set", "statement close", "main connection");
        assertThat(events.indexOf("worker 1")).isLessThan(events.indexOf("main connection"));
        assertThat(events.indexOf("worker 2")).isLessThan(events.indexOf("main connection"));
        assertThat(events).containsOnlyOnce("worker 1", "worker 2", "main connection");
    }

    @Test
    void testWorkerConnectionsCloseConcurrently()
    {
        CountDownLatch bothStarted = new CountDownLatch(2);
        AtomicBoolean workersClosedTogether = new AtomicBoolean(true);
        Runnable closeWorker = () -> {
            bothStarted.countDown();
            try {
                workersClosedTogether.compareAndSet(true, bothStarted.await(1, TimeUnit.SECONDS));
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                workersClosedTogether.set(false);
            }
        };

        assertThat(ExasolParallelPageSource.closeWorkerConnections(List.of(closeWorker, closeWorker), 2, TimeUnit.SECONDS)).isTrue();
        assertThat(workersClosedTogether).isTrue();
    }

    @Test
    void testWorkerCloseTimeout()
    {
        CountDownLatch release = new CountDownLatch(1);
        Runnable closeWorker = () -> {
            try {
                release.await();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };

        assertThat(ExasolParallelPageSource.closeWorkerConnections(List.of(closeWorker), 100, TimeUnit.MILLISECONDS)).isFalse();
    }

    @Test
    void testActiveWorkerClosesAfterMainConnection()
    {
        List<String> events = new CopyOnWriteArrayList<>();
        try (var executor = Executors.newSingleThreadExecutor()) {
            JdbcClient jdbcClient = proxy(JdbcClient.class, (_, _, _) -> null);
            ExasolResultHandlePageSource worker = new ExasolResultHandlePageSource(jdbcClient, executor, null, connection(events, "worker"), 42, List.of(), () -> resultSet(() -> false));
            ExasolParallelPageSource source = new ExasolParallelPageSource(
                    List.of(worker),
                    executor,
                    connection(events, "main connection"),
                    proxy(PreparedStatement.class, (_, _, _) -> null),
                    resultSet(() -> false),
                    List.of(),
                    false,
                    "test-query",
                    1);

            source.close();
        }

        assertThat(events).contains("main connection", "worker");
        assertThat(events.indexOf("main connection")).isLessThan(events.indexOf("worker"));
    }

    @Test
    void testEmptyWorkerFinishesAlongsideNonemptyWorker()
            throws Exception
    {
        List<String> events = new CopyOnWriteArrayList<>();
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
                    List.of(),
                    false,
                    "test-query",
                    1);
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

        assertThat(events).contains("empty worker", "nonempty worker", "main connection");
        assertThat(events.indexOf("empty worker")).isLessThan(events.indexOf("main connection"));
        assertThat(events.indexOf("nonempty worker")).isLessThan(events.indexOf("main connection"));
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
