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

import com.exasol.jdbc.EXAResultSet;
import io.trino.plugin.jdbc.BaseJdbcConnectorTableHandle;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcPageSource;
import io.trino.plugin.jdbc.JdbcProcedureHandle;
import io.trino.plugin.jdbc.JdbcSplit;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class ExasolParallelConnectionPageSource
        implements ConnectorPageSource
{
    private static final Logger log = LoggerFactory.getLogger(ExasolParallelConnectionPageSource.class);
    private final List<SubConnectionPageSource> subConnectionPageSources;
    private final ExecutorService executor;
    private final BlockingQueue<SourcePage> queue = new LinkedBlockingQueue<>();
    private final AtomicInteger completedSubConnections;
    private final Connection mainConnection;
    private final PreparedStatement mainStatement;
    private final ResultSet mainResultSet;
    private final AtomicReference<Throwable> subConnectionFailure = new AtomicReference<>();
    private boolean closed;
    private CompletableFuture<Void> allConsumersFuture;

    private ExasolParallelConnectionPageSource(List<SubConnectionPageSource> subConnectionPageSources, ExecutorService executor, Connection mainConnection, PreparedStatement mainStatement, ResultSet mainResultSet)
    {
        this.subConnectionPageSources = subConnectionPageSources;
        this.executor = executor;
        this.completedSubConnections = new AtomicInteger(subConnectionPageSources.size());
        this.mainConnection = mainConnection;
        this.mainStatement = mainStatement;
        this.mainResultSet = mainResultSet;
    }

    public static ConnectorPageSource create(JdbcClient jdbcClient, ExecutorService executor, ParallelConnectionFactory parallelConnectionFactory, ConnectorSession session, JdbcSplit jdbcSplit, BaseJdbcConnectorTableHandle table, List<JdbcColumnHandle> columnHandles)
    {
        int parallelImportWorkerCount = ExasolSessionProperties.getParallelImportWorkerCount(session);
        if (parallelImportWorkerCount == 0) {
            return new JdbcPageSource(jdbcClient, executor, session, jdbcSplit, table, columnHandles);
        }
        return createParallelConnectionPageSource(jdbcClient, executor, parallelConnectionFactory, session, jdbcSplit, table, columnHandles);
    }

    private static ExasolParallelConnectionPageSource createParallelConnectionPageSource(JdbcClient jdbcClient, ExecutorService executor, ParallelConnectionFactory parallelConnectionFactory, ConnectorSession session, JdbcSplit jdbcSplit, BaseJdbcConnectorTableHandle table, List<JdbcColumnHandle> columnHandles)
    {
        try {
            Connection mainConnection = createExaConnection(jdbcClient, session, table);
            PreparedStatement mainStatement = prepareStatement(jdbcClient, session, mainConnection, table, jdbcSplit, columnHandles);
            List<Connection> subConnections = parallelConnectionFactory.createConnections(session, mainConnection);
            log.info("Connected to {} Exasol nodes. Preparing statement...", subConnections.size());

            ResultSet mainResultSet = mainStatement.executeQuery();
            int resultSetHandle = mainResultSet.unwrap(EXAResultSet.class).GetHandle();
            log.debug("Statement executed, got result set handle {}. Reading result from subconnections...", resultSetHandle);
            List<SubConnectionPageSource> subConnectionPageSources = subConnections.stream().map(con -> new SubConnectionPageSource(jdbcClient, executor, session, con, resultSetHandle, columnHandles)).toList();
            ExasolParallelConnectionPageSource pageSource = new ExasolParallelConnectionPageSource(subConnectionPageSources, executor, mainConnection, mainStatement, mainResultSet);
            pageSource.startReading();
            return pageSource;
        }
        catch (SQLException e) {
            log.error("Error creating parallel connection page source: {}", e.getMessage(), e);
            throw new RuntimeException("Error creating parallel connection page source: " + e.getMessage(), e);
        }
    }

    private void startReading()
    {
        CompletableFuture<?>[] consumers = subConnectionPageSources.stream().map(source ->
                CompletableFuture.supplyAsync(() -> consumePageSource(source), executor)
        ).toArray(CompletableFuture[]::new);
        allConsumersFuture = CompletableFuture.allOf(consumers).thenRun(() -> log.info("All consumers have finished reading"));
    }

    private Void consumePageSource(SubConnectionPageSource source)
    {
        try {
            log.debug("Reading all pages from {}...", source);
            while (!source.isFinished()) {
                SourcePage page = source.getNextSourcePage();
                if (page != null) {
                    queue.add(page);
                    log.debug("Found page {} from source {}, queue size: {}", page, source, queue.size());
                }
            }
            return null;
        }
        catch (Throwable e) {
            log.error("Error reading from source {}: {}", source, e.getMessage(), e);
            subConnectionFailure.set(e);
            return null;
        }
        finally {
            int remainingConnections = completedSubConnections.decrementAndGet();
            log.debug("All pages read from {}, remaining connections: {}, queue size: {}", source, remainingConnections, queue.size());
        }
    }

    private static PreparedStatement prepareStatement(JdbcClient jdbcClient, ConnectorSession session, Connection mainConnection, BaseJdbcConnectorTableHandle table, JdbcSplit jdbcSplit, List<JdbcColumnHandle> columnHandles)
            throws SQLException
    {
        if (table instanceof JdbcProcedureHandle procedureHandle) {
            return jdbcClient.buildProcedure(session, mainConnection, jdbcSplit, procedureHandle);
        }
        else {
            return jdbcClient.buildSql(session, mainConnection, jdbcSplit, (JdbcTableHandle) table, columnHandles);
        }
    }

    private static Connection createExaConnection(JdbcClient jdbcClient, ConnectorSession session, BaseJdbcConnectorTableHandle table)
            throws SQLException
    {
        if (table instanceof JdbcProcedureHandle procedureHandle) {
            return jdbcClient.getConnection(session, null, procedureHandle);
        }
        else {
            return jdbcClient.getConnection(session, null, (JdbcTableHandle) table);
        }
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        Throwable throwable = subConnectionFailure.get();
        if (throwable != null) {
            throw new RuntimeException("Sub connection failure: " + throwable.getMessage(), throwable);
        }
        SourcePage page = queue.poll();
        log.debug("Got next source page {}, remaining queue size: {}", page, queue.size());
        return page;
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        //log.debug("Checking if blocked...");
        // TODO: combine futures of subconnections
        //return CompletableFuture.anyOf(subConnectionPageSources.stream().map(ConnectorPageSource::isBlocked).toArray(CompletableFuture[]::new));
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public long getCompletedBytes()
    {
        return subConnectionPageSources.stream().mapToLong(ConnectorPageSource::getCompletedBytes).sum();
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        return subConnectionPageSources.stream()
                .map(ConnectorPageSource::getCompletedPositions)
                .filter(OptionalLong::isPresent)
                .mapToLong(OptionalLong::getAsLong)
                .reduce(Long::sum);
    }

    @Override
    public long getReadTimeNanos()
    {
        return subConnectionPageSources.stream().mapToLong(ConnectorPageSource::getReadTimeNanos).sum();
    }

    @Override
    public boolean isFinished()
    {
        boolean subConnectionsFinished = subConnectionsFinished();
        boolean queueEmpty = queue.isEmpty();
        boolean subConnectionFailure = this.subConnectionFailure.get() == null;
        boolean finished = queueEmpty && subConnectionFailure && subConnectionsFinished;
        log.debug("Checking isFinished: {}, queue empty: {}, subconnections ok: {}, subconnections finished: {}", finished, queueEmpty, subConnectionFailure, subConnectionsFinished);
        return finished;
    }

    private boolean subConnectionsFinished()
    {
        return allConsumersFuture != null && allConsumersFuture.isDone();
    }

    @Override
    public long getMemoryUsage()
    {
        return subConnectionPageSources.stream().mapToLong(ConnectorPageSource::getMemoryUsage).sum();
    }

    @Override
    public void close()
    {
        if (closed) {
            log.debug("Page sources already closed");
            return;
        }

        log.debug("Closing main connection...");
        // use try with resources to close everything properly
        try (Connection connection = this.mainConnection;
                Statement statement = this.mainStatement;
                ResultSet resultSet = this.mainResultSet) {
            if (statement != null) {
                try {
                    log.debug("Cancelling statement...");
                    // Trying to cancel running statement as close() may not do it
                    statement.cancel();
                }
                catch (SQLException e) {
                    log.debug("Cancel failed", e);
                    // statement already closed or cancel is not supported
                }
            }
        }
        catch (SQLException | RuntimeException e) {
            // ignore exception from close
            log.debug("Closing failed", e);
        }

        // Subconnections must be closed after the main connection.
        // When main connection is still open, closing of subconnections will block.
        log.debug("Closing {} subconnection page sources of {}...", subConnectionPageSources.size(), this);
        subConnectionPageSources.forEach(SubConnectionPageSource::close);

        closed = true;
        log.debug("Closed page source {} successfully", this);
    }
}
