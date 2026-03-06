package io.trino.plugin.exasol;

import com.exasol.jdbc.EXAConnection;
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
import io.trino.spi.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;

public class ExasolParallelConnectionPageSource implements ConnectorPageSource
{
    private static final Logger log = LoggerFactory.getLogger(ExasolParallelConnectionPageSource.class);
    private final List<SubConnectionPageSource> subConnectionPageSources;
    private final ExecutorService executor;
    private final BlockingQueue<SourcePage> queue = new LinkedBlockingQueue<>();
    private final CountDownLatch completedSubConnections;
    private final EXAConnection mainConnection;

    private ExasolParallelConnectionPageSource(List<SubConnectionPageSource> subConnectionPageSources, ExecutorService executor, EXAConnection mainConnection) {
        this.subConnectionPageSources = subConnectionPageSources;
        this.executor = executor;
        this.completedSubConnections=new CountDownLatch(subConnectionPageSources.size());
        this.mainConnection = mainConnection;
    }

    public static ExasolParallelConnectionPageSource create(JdbcClient jdbcClient, ExecutorService executor, ParallelConnectionFactory parallelConnectionFactory, ConnectorSession session, JdbcSplit jdbcSplit, BaseJdbcConnectorTableHandle table, List<JdbcColumnHandle> columnHandles) {

        try {
            EXAConnection exaCon = createExaConnection(jdbcClient, session, table);
            List<EXAConnection> subConnections = parallelConnectionFactory.createConnections(session, exaCon);
            PreparedStatement stmt = prepareStatement(jdbcClient, session, exaCon, table, jdbcSplit, columnHandles);
            EXAResultSet resultSet = stmt.executeQuery().unwrap(EXAResultSet.class);
            int resultSetHandle=resultSet.GetHandle();
            List<SubConnectionPageSource> subConnectionPageSources = subConnections.stream().map(con -> new SubConnectionPageSource(jdbcClient, executor, session, con, resultSetHandle, columnHandles)).toList();

            ExasolParallelConnectionPageSource pageSource = new ExasolParallelConnectionPageSource(subConnectionPageSources, executor, exaCon);
            pageSource.startReading();
            return pageSource;
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void startReading() {
        subConnectionPageSources.forEach(source -> {
            executor.submit(()->{
                SourcePage page = null;
                try {
                    log.info("Reading all pages from "+source);
                    while ((page = source.getNextSourcePage()) != null) {
                        log.info("Found page "+page +" from source "+source);
                        queue.add(page);
                    }
                } catch(Exception e) {
                    log.error("Error reading from source "+source+": "+e.getMessage(), e);
                } finally {
                    log.info("All pages read from "+source);
                    completedSubConnections.countDown();
                }
            });
        });
    }

    private static PreparedStatement prepareStatement(JdbcClient jdbcClient, ConnectorSession session, EXAConnection exaCon, BaseJdbcConnectorTableHandle table, JdbcSplit jdbcSplit, List<JdbcColumnHandle> columnHandles)
            throws SQLException
    {
        if (table instanceof JdbcProcedureHandle procedureHandle) {
            return jdbcClient.buildProcedure(session, exaCon, jdbcSplit, procedureHandle);
        }
        else {
            return jdbcClient.buildSql(session, exaCon, jdbcSplit, (JdbcTableHandle) table, columnHandles);
        }
    }

    private static EXAConnection createExaConnection(JdbcClient jdbcClient, ConnectorSession session, BaseJdbcConnectorTableHandle table)
            throws SQLException
    {
        if (table instanceof JdbcProcedureHandle procedureHandle) {
            return jdbcClient.getConnection(session, null, procedureHandle).unwrap(EXAConnection.class);
        }
        else {
            return jdbcClient.getConnection(session, null, (JdbcTableHandle) table).unwrap(EXAConnection.class);
        }
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        if(completedSubConnections.getCount()==0){
            log.info("All subconnections are finished, poll next page");
            return queue.poll();
        }
        try {
            log.info("Subconnections are still running, block waiting for next page...");
            return queue.take();
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        // TODO
        return ConnectorPageSource.super.isBlocked();
    }

    @Override
    public Metrics getMetrics()
    {
        // TODO
        return ConnectorPageSource.super.getMetrics();
    }

    @Override
    public long getCompletedBytes()
    {
        return subConnectionPageSources.stream().mapToLong(SubConnectionPageSource::getCompletedBytes).sum();
    }

    @Override
    public long getReadTimeNanos()
    {
        return subConnectionPageSources.stream().mapToLong(SubConnectionPageSource::getReadTimeNanos).sum();
    }

    @Override
    public boolean isFinished()
    {
        return subConnectionPageSources.stream().allMatch(subConnectionPageSource -> subConnectionPageSource.isFinished());
    }

    @Override
    public long getMemoryUsage()
    {
        return subConnectionPageSources.stream().mapToLong(SubConnectionPageSource::getMemoryUsage).sum();
    }

    @Override
    public void close()
    {
        subConnectionPageSources.forEach(SubConnectionPageSource::close);
        try {
            mainConnection.close();
        }
        catch (SQLException e) {
            throw new RuntimeException("Error closing main connection", e);
        }
    }
}
