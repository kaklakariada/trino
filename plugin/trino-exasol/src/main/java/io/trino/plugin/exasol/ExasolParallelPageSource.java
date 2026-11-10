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

import com.exasol.jdbc.EXAConnection;
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
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.connector.SourcePage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class ExasolParallelPageSource
        implements ConnectorPageSource
{
    private static final Logger log = LoggerFactory.getLogger(ExasolParallelPageSource.class);
    private static final long WORKER_CLOSE_TIMEOUT_SECONDS = 5;
    private final List<ExasolResultHandlePageSource> subConnectionPageSources;
    private final ExecutorService executor;
    private final BufferedSourcePageQueue pageQueue;
    private final Connection mainConnection;
    private final PreparedStatement mainStatement;
    private final ResultSet mainResultSet;
    private final List<Connection> unusedSubConnections;
    private final boolean readFromMainConnection;
    private final String queryId;
    private final long sessionId;
    private volatile List<? extends Future<?>> readerTasks = List.of();
    private volatile boolean closed;

    ExasolParallelPageSource(List<ExasolResultHandlePageSource> subConnectionPageSources, ExecutorService executor, Connection mainConnection, PreparedStatement mainStatement, ResultSet mainResultSet, List<Connection> unusedSubConnections, boolean readFromMainConnection, String queryId, long sessionId)
    {
        this.subConnectionPageSources = subConnectionPageSources;
        this.executor = executor;
        this.pageQueue = new BufferedSourcePageQueue(subConnectionPageSources.size());
        this.mainConnection = mainConnection;
        this.mainStatement = mainStatement;
        this.mainResultSet = mainResultSet;
        this.unusedSubConnections = unusedSubConnections;
        this.readFromMainConnection = readFromMainConnection;
        this.queryId = queryId;
        this.sessionId = sessionId;
    }

    public static ConnectorPageSource create(JdbcClient jdbcClient, ExecutorService executor, ParallelConnectionFactory parallelConnectionFactory, ConnectorSession session, JdbcSplit jdbcSplit, BaseJdbcConnectorTableHandle table, List<JdbcColumnHandle> columnHandles, MemoryContext memoryContext)
    {
        int parallelImportWorkerCount = ExasolSessionProperties.getParallelImportWorkerCount(session);
        if (parallelImportWorkerCount == 0) {
            return new JdbcPageSource(jdbcClient, executor, session, jdbcSplit, table, columnHandles, memoryContext);
        }
        return createParallelConnectionPageSource(jdbcClient, executor, parallelConnectionFactory, session, jdbcSplit, table, columnHandles);
    }

    private static ConnectorPageSource createParallelConnectionPageSource(JdbcClient jdbcClient, ExecutorService executor, ParallelConnectionFactory parallelConnectionFactory, ConnectorSession session, JdbcSplit jdbcSplit, BaseJdbcConnectorTableHandle table, List<JdbcColumnHandle> columnHandles)
    {
        Connection mainConnection = null;
        PreparedStatement mainStatement = null;
        ResultSet mainResultSet = null;
        List<Connection> subConnections = List.of();
        long sessionId = -1;
        try {
            mainConnection = createExaConnection(jdbcClient, session, table);
            sessionId = mainConnection.unwrap(EXAConnection.class).getSessionID();
            log.debug("Starting Exasol parallel read: queryId={}, sessionId={}, requestedWorkers={}", session.getQueryId(), sessionId, ExasolSessionProperties.getParallelImportWorkerCount(session));
            subConnections = parallelConnectionFactory.createConnections(session, mainConnection);
            log.debug("Connected Exasol parallel workers: queryId={}, sessionId={}, workers={}", session.getQueryId(), sessionId, subConnections.size());
            mainStatement = prepareStatement(jdbcClient, session, mainConnection, table, jdbcSplit, columnHandles);
            mainResultSet = mainStatement.executeQuery();
            EXAResultSet exaResultSet = mainResultSet.unwrap(EXAResultSet.class);
            if (exaResultSet.getNumberOfRows() == 0) {
                log.trace("Query returned no rows; skipping parallel result reading");
                log.debug("Closing empty Exasol parallel read: queryId={}, sessionId={}, workers={}", session.getQueryId(), sessionId, subConnections.size());
                closeResources(session.getQueryId(), sessionId, mainConnection, mainStatement, mainResultSet, subConnections);
                return new EmptyPageSource();
            }
            int resultSetHandle = exaResultSet.GetHandle();
            log.debug("Executed Exasol parallel read: queryId={}, sessionId={}, rows={}, resultSetHandle={}", session.getQueryId(), sessionId, exaResultSet.getNumberOfRows(), resultSetHandle);
            if (resultSetHandle < 0) {
                log.trace("Result set handle {} is unavailable for {} rows; reading from the main connection", resultSetHandle, exaResultSet.getNumberOfRows());
                ResultSet directResultSet = mainResultSet;
                ExasolResultHandlePageSource directSource = new ExasolResultHandlePageSource(jdbcClient, executor, session, mainConnection, resultSetHandle, columnHandles, () -> directResultSet);
                ExasolParallelPageSource pageSource = new ExasolParallelPageSource(List.of(directSource), executor, mainConnection, mainStatement, mainResultSet, subConnections, true, session.getQueryId(), sessionId);
                pageSource.startReading();
                return pageSource;
            }
            List<ExasolResultHandlePageSource> subConnectionPageSources = subConnections.stream()
                    .map(subConnection -> new ExasolResultHandlePageSource(jdbcClient, executor, session, subConnection, resultSetHandle, columnHandles))
                    .toList();
            ExasolParallelPageSource pageSource = new ExasolParallelPageSource(subConnectionPageSources, executor, mainConnection, mainStatement, mainResultSet, List.of(), false, session.getQueryId(), sessionId);
            pageSource.startReading();
            return pageSource;
        }
        catch (SQLException | RuntimeException e) {
            log.debug("Closing failed Exasol parallel read: queryId={}, sessionId={}, workers={}", session.getQueryId(), sessionId, subConnections.size());
            closeResources(session.getQueryId(), sessionId, mainConnection, mainStatement, mainResultSet, subConnections);
            log.error("Error creating Exasol parallel page source: queryId={}, sessionId={}, error={}", session.getQueryId(), sessionId, e.getMessage(), e);
            throw new RuntimeException("Error creating parallel connection page source: " + e.getMessage(), e);
        }
    }

    private static void closeResources(String queryId, long sessionId, Connection mainConnection, PreparedStatement mainStatement, ResultSet mainResultSet, List<Connection> subConnections)
    {
        log.debug("Closing Exasol workers during setup cleanup: queryId={}, sessionId={}, workers={}", queryId, sessionId, subConnections.size());
        List<Runnable> closeActions = new ArrayList<>();
        for (int workerId = 0; workerId < subConnections.size(); workerId++) {
            int worker = workerId;
            closeActions.add(() -> {
                try {
                    subConnections.get(worker).close();
                }
                catch (SQLException | RuntimeException e) {
                    log.debug("Failed to close Exasol worker during setup cleanup: queryId={}, sessionId={}, worker={}", queryId, sessionId, worker, e);
                }
            });
        }
        if (!closeWorkerConnections(closeActions, WORKER_CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
            log.debug("Timed out closing Exasol workers during setup cleanup; closing main connection: queryId={}, sessionId={}", queryId, sessionId);
        }

        log.debug("Closing Exasol main connection during setup cleanup: queryId={}, sessionId={}", queryId, sessionId);
        try (Connection _ = mainConnection;
                Statement statement = mainStatement;
                ResultSet _ = mainResultSet) {
            if (statement != null) {
                try {
                    statement.cancel();
                }
                catch (SQLException e) {
                    log.debug("Cancel failed during cleanup", e);
                }
            }
        }
        catch (SQLException | RuntimeException e) {
            log.debug("Exasol main connection setup cleanup failed: queryId={}, sessionId={}", queryId, sessionId, e);
        }
        log.debug("Finished Exasol parallel setup cleanup: queryId={}, sessionId={}", queryId, sessionId);
    }

    static boolean closeWorkerConnections(List<Runnable> closeActions, long timeout, TimeUnit unit)
    {
        if (closeActions.isEmpty()) {
            return true;
        }
        ExecutorService closeExecutor = Executors.newVirtualThreadPerTaskExecutor();
        closeActions.forEach(closeExecutor::submit);
        closeExecutor.shutdown();
        try {
            if (closeExecutor.awaitTermination(timeout, unit)) {
                return true;
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        closeExecutor.shutdownNow();
        return false;
    }

    void startReading()
    {
        readerTasks = IntStream.range(0, subConnectionPageSources.size())
                .mapToObj(producerId -> executor.submit(() -> consumePageSource(producerId, subConnectionPageSources.get(producerId))))
                .toList();
    }

    private void consumePageSource(int producerId, ExasolResultHandlePageSource source)
    {
        String outcome = "completed";
        try {
            log.debug("Starting Exasol worker read: queryId={}, sessionId={}, worker={}", queryId, sessionId, producerId);
            while (!source.isFinished()) {
                SourcePage page = source.getNextSourcePage();
                if (page != null) {
                    pageQueue.add(page);
                    log.trace("Found page {} with {} rows from source {}, queue size: {}", page, page.getPositionCount(), source, pageQueue.size());
                }
            }
        }
        catch (InterruptedException e) {
            outcome = "interrupted";
            Thread.currentThread().interrupt();
            log.debug("Exasol worker read interrupted: queryId={}, sessionId={}, worker={}", queryId, sessionId, producerId, e);
        }
        catch (Throwable e) {
            outcome = "failed";
            log.error("Exasol worker read failed: queryId={}, sessionId={}, worker={}, error={}", queryId, sessionId, producerId, e.getMessage(), e);
            pageQueue.fail(producerId, e);
        }
        finally {
            source.closeResultSet();
            pageQueue.finish(producerId);
            log.debug("Finished Exasol worker read: queryId={}, sessionId={}, worker={}, outcome={}, rows={}", queryId, sessionId, producerId, outcome, source.getCompletedPositions().orElse(0));
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
        Throwable throwable = pageQueue.getFailure();
        if (throwable != null) {
            throw new RuntimeException("Sub connection failure: " + throwable.getMessage(), throwable);
        }
        SourcePage page = pageQueue.poll();
        log.trace("Got next source page {}, remaining queue size: {}", page, pageQueue.size());
        return page;
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        return pageQueue.isBlocked();
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
        return pageQueue.isFinished();
    }

    @Override
    public void close()
    {
        if (closed) {
            log.debug("Exasol parallel page source already closed: queryId={}, sessionId={}", queryId, sessionId);
            return;
        }
        boolean workersActive = pageQueue.activeProducerCount() > 0 || pageQueue.getFailure() != null;
        log.debug("Closing Exasol parallel page source: queryId={}, sessionId={}, finished={}, workerFailure={}, activeWorkers={}, queuedPages={}", queryId, sessionId, pageQueue.isFinished(), pageQueue.getFailure() != null, pageQueue.activeProducerCount(), pageQueue.size());
        readerTasks.forEach(task -> task.cancel(true));
        if (!readFromMainConnection) {
            subConnectionPageSources.forEach(ExasolResultHandlePageSource::closeResultSet);
        }

        if (!workersActive) {
            closeWorkers();
        }

        log.debug("Closing Exasol main connection: queryId={}, sessionId={}", queryId, sessionId);
        // use try with resources to close everything properly
        try (Connection _ = this.mainConnection;
                Statement statement = this.mainStatement;
                ResultSet _ = this.mainResultSet) {
            if (statement != null) {
                try {
                    log.debug("Cancelling Exasol main statement: queryId={}, sessionId={}", queryId, sessionId);
                    // Trying to cancel running statement as close() may not do it
                    statement.cancel();
                }
                catch (SQLException e) {
                    log.debug("Exasol main statement cancel failed: queryId={}, sessionId={}", queryId, sessionId, e);
                    // statement already closed or cancel is not supported
                }
            }
        }
        catch (SQLException | RuntimeException e) {
            // ignore exception from close
            log.debug("Exasol main connection cleanup failed: queryId={}, sessionId={}", queryId, sessionId, e);
        }
        log.debug("Finished Exasol main connection cleanup: queryId={}, sessionId={}", queryId, sessionId);

        if (workersActive) {
            closeWorkers();
        }
        if (readFromMainConnection) {
            subConnectionPageSources.forEach(ExasolResultHandlePageSource::close);
        }

        closed = true;
        log.debug("Closed Exasol parallel page source: queryId={}, sessionId={}", queryId, sessionId);
    }

    private void closeWorkers()
    {
        log.debug("Closing Exasol worker connections: queryId={}, sessionId={}, workers={}, unusedWorkers={}", queryId, sessionId, subConnectionPageSources.size(), unusedSubConnections.size());
        List<Runnable> closeActions = new ArrayList<>();
        if (!readFromMainConnection) {
            for (int workerId = 0; workerId < subConnectionPageSources.size(); workerId++) {
                int worker = workerId;
                closeActions.add(() -> {
                    log.debug("Closing Exasol worker connection: queryId={}, sessionId={}, worker={}", queryId, sessionId, worker);
                    subConnectionPageSources.get(worker).close();
                    log.debug("Finished Exasol worker connection cleanup: queryId={}, sessionId={}, worker={}", queryId, sessionId, worker);
                });
            }
        }
        for (int workerId = 0; workerId < unusedSubConnections.size(); workerId++) {
            int worker = workerId;
            closeActions.add(() -> {
                try {
                    unusedSubConnections.get(worker).close();
                }
                catch (SQLException | RuntimeException e) {
                    log.debug("Failed to close unused Exasol subconnection: queryId={}, sessionId={}, worker={}", queryId, sessionId, worker, e);
                }
            });
        }
        if (!closeWorkerConnections(closeActions, WORKER_CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
            log.debug("Timed out closing Exasol workers: queryId={}, sessionId={}", queryId, sessionId);
        }
    }
}
