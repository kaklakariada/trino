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
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

final class TestExasolResultHandlePageSource
{
    @Test
    void testCloseResultSetAfterDescribeCompletes()
            throws Exception
    {
        AtomicInteger resultSetCloses = new AtomicInteger();
        AtomicInteger connectionCloses = new AtomicInteger();
        CountDownLatch describeStarted = new CountDownLatch(1);
        CountDownLatch completeDescribe = new CountDownLatch(1);
        CountDownLatch resultSetClosed = new CountDownLatch(1);
        ResultSet resultSet = proxy(ResultSet.class, (_, method, _) -> {
            if (method.getName().equals("close")) {
                resultSetCloses.incrementAndGet();
                resultSetClosed.countDown();
            }
            return null;
        });
        Connection connection = connection(connectionCloses);

        try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
            ExasolResultHandlePageSource source = new ExasolResultHandlePageSource(
                    jdbcClient(), executor, null, connection, 42, List.of(), () -> {
                        describeStarted.countDown();
                        try {
                            if (!completeDescribe.await(5, TimeUnit.SECONDS)) {
                                throw new AssertionError("DescribeResult did not resume");
                            }
                        }
                        catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new SQLException("DescribeResult interrupted", e);
                        }
                        return resultSet;
                    });
            try {
                source.getNextSourcePage();
                assertThat(describeStarted.await(5, TimeUnit.SECONDS)).isTrue();

                source.close();
                assertThat(connectionCloses).hasValue(1);
                completeDescribe.countDown();
                assertThat(resultSetClosed.await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(resultSetCloses).hasValue(1);
            }
            finally {
                completeDescribe.countDown();
                source.close();
            }
        }
    }

    @Test
    void testResultSetClosesBeforeSubconnection()
            throws Exception
    {
        AtomicInteger resultSetCloses = new AtomicInteger();
        AtomicInteger connectionCloses = new AtomicInteger();
        ResultSet resultSet = proxy(ResultSet.class, (_, method, _) -> {
            if (method.getName().equals("close")) {
                resultSetCloses.incrementAndGet();
            }
            return null;
        });

        try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
            ExasolResultHandlePageSource source = new ExasolResultHandlePageSource(
                    jdbcClient(), executor, null, connection(connectionCloses), 42, List.of(), () -> resultSet);
            try {
                source.getNextSourcePage();
                source.isBlocked().get(5, TimeUnit.SECONDS);
                source.closeResultSet();

                assertThat(resultSetCloses).hasValue(1);
                assertThat(connectionCloses).hasValue(0);
            }
            finally {
                source.close();
            }
            assertThat(resultSetCloses).hasValue(1);
            assertThat(connectionCloses).hasValue(1);
        }
    }

    private static JdbcClient jdbcClient()
    {
        return proxy(JdbcClient.class, (_, _, _) -> null);
    }

    private static Connection connection(AtomicInteger closes)
    {
        return proxy(Connection.class, (_, method, _) -> {
            if (method.getName().equals("close")) {
                closes.incrementAndGet();
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
