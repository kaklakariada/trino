package io.trino.plugin.exasol;

import com.exasol.jdbc.EXAResultSet;
import io.trino.plugin.jdbc.BaseJdbcConnectorTableHandle;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcColumnHandle;
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

public class ExasolParallelConnectionPageSource implements ConnectorPageSource
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
    private boolean closed = false;

    private ExasolParallelConnectionPageSource(List<SubConnectionPageSource> subConnectionPageSources, ExecutorService executor, Connection mainConnection, PreparedStatement mainStatement, ResultSet mainResultSet) {
        this.subConnectionPageSources = subConnectionPageSources;
        this.executor = executor;
        this.completedSubConnections=new AtomicInteger(subConnectionPageSources.size());
        this.mainConnection = mainConnection;
        this.mainStatement = mainStatement;
        this.mainResultSet = mainResultSet;
    }

    public static ExasolParallelConnectionPageSource create(JdbcClient jdbcClient, ExecutorService executor, ParallelConnectionFactory parallelConnectionFactory, ConnectorSession session, JdbcSplit jdbcSplit, BaseJdbcConnectorTableHandle table, List<JdbcColumnHandle> columnHandles) {
        try {
            Connection mainConnection = createExaConnection(jdbcClient, session, table);
            List<Connection> subConnections = parallelConnectionFactory.createConnections(session, mainConnection);
            log.info("Connected to {} Exasol nodes. Preparing statement...", subConnections.size());
            PreparedStatement mainStatement = prepareStatement(jdbcClient, session, mainConnection, table, jdbcSplit, columnHandles);
            log.info("Executing prepared statement...");
            ResultSet mainResultSet = mainStatement.executeQuery();
            int resultSetHandle = mainResultSet.unwrap(EXAResultSet.class).GetHandle();
            log.info("Statement executed, got result set handle {}. Reading result from subconnections...", resultSetHandle);
            List<SubConnectionPageSource> subConnectionPageSources = subConnections.stream().map(con -> new SubConnectionPageSource(jdbcClient, executor, session, con, resultSetHandle, columnHandles)).toList();
            ExasolParallelConnectionPageSource pageSource = new ExasolParallelConnectionPageSource(subConnectionPageSources, executor, mainConnection, mainStatement, mainResultSet);
            pageSource.startReading();
            return pageSource;
        }
        catch (SQLException e) {
            throw new RuntimeException("Error creating parallel connection page source: "+e.getMessage(), e);
        }
    }

    private void startReading() {

        CompletableFuture<?>[] consumers = subConnectionPageSources.stream().map(source ->
                CompletableFuture.supplyAsync(() -> consumePageSource(source), executor)
        ).toArray(CompletableFuture[]::new);
        CompletableFuture.allOf(consumers).thenRun(()-> log.info("All consumers have finished reading"));
    }

    private Void consumePageSource(SubConnectionPageSource source)
    {
        try {
            log.info("Reading all pages from {}...", source);
            while (!source.isFinished()) {
                SourcePage page = source.getNextSourcePage();
                if (page != null) {
                    log.info("Found page {} from source {}", page, source);
                    queue.add(page);
                }
            }
        } catch(Throwable e) {
            log.error("Error reading from source {}: {}", source, e.getMessage(), e);
            subConnectionFailure.set(e);
        } finally {
            int remainingConnections = completedSubConnections.decrementAndGet();
            log.info("All pages read from {}, remaining connections: {}", source, remainingConnections);
            return null;
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
        if(throwable != null) {
            throw new RuntimeException("Sub connection failure: "+throwable.getMessage(), throwable);
        }
        return queue.poll();
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
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
        return queue.isEmpty() && subConnectionFailure.get() != null && subConnectionsFinished();
    }

    private boolean subConnectionsFinished()
    {
        return subConnectionPageSources.stream().allMatch(source -> source.isFinished());
    }

    @Override
    public long getMemoryUsage()
    {
        return subConnectionPageSources.stream().mapToLong(ConnectorPageSource::getMemoryUsage).sum();
    }

    @Override
    public void close()
    {
        subConnectionPageSources.forEach(SubConnectionPageSource::close);
        if (closed) {
            return;
        }
        closed = true;

        // use try with resources to close everything properly
        try (Connection connection = this.mainConnection;
                Statement statement = this.mainStatement;
                ResultSet resultSet = this.mainResultSet) {
            if (statement != null) {
                try {
                    // Trying to cancel running statement as close() may not do it
                    statement.cancel();
                }
                catch (SQLException _) {
                    // statement already closed or cancel is not supported
                }
            }
        }
        catch (SQLException | RuntimeException e) {
            // ignore exception from close
        }
    }
}
