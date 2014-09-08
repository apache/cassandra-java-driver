package com.datastax.driver.core;

import com.codahale.metrics.Gauge;
import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.fail;

public class HostConnectionPoolTest extends CCMBridge.PerClassSingleNodeCluster {
    @Override
    protected Collection<String> getTableDefinitions() {
        StringBuilder sb = new StringBuilder("CREATE TABLE Java349 (mykey INT primary key");
        for (int i = 0; i < 1000; i++) {
            sb.append(", column").append(i).append(" INT");
        }
        sb.append(")");
        return Lists.newArrayList(sb.toString());
    }


    /**
     * Test for #JAVA-349 - Negative openConnection count results in broken connection release.
     * <p/>
     * This test uses a table named "Java349" with 1000 column and performs asynchronously 100k insertions. While the
     * insertions are being executed, the number of opened connection is monitored.
     * <p/>
     * If at anytime, the number of opened connections is negative, this test will fail.
     *
     * @throws InterruptedException
     */
    @Test(groups = "long")
    public void open_connections_metric_should_always_be_positive() throws InterruptedException {
        // Track progress in a dedicated thread
        int numberOfInserts = 100 * 1000;
        final CountDownLatch pendingInserts = new CountDownLatch(numberOfInserts);
        ExecutorService progressReportExecutor = Executors.newSingleThreadExecutor();
        final Runnable progressReporter = new Runnable() {
            @Override
            public void run() {
                pendingInserts.countDown();
            }
        };

        // Track opened connections in a dedicated thread every one second
        final AtomicBoolean negativeOpenConnectionCountSpotted = new AtomicBoolean(false);
        final Gauge<Integer> openConnections = cluster.getMetrics().getOpenConnections();
        ScheduledExecutorService openConnectionsWatcherExecutor = Executors.newScheduledThreadPool(1);
        final Runnable openConnectionsWatcher = new Runnable() {
            @Override
            public void run() {
                Integer value = openConnections.getValue();
                if (value < 0) {
                    System.err.println("Negative value spotted for openConnection metric: " + value);
                    negativeOpenConnectionCountSpotted.set(true);
                }
            }
        };
        openConnectionsWatcherExecutor.scheduleAtFixedRate(openConnectionsWatcher, 1, 1, SECONDS);

        // Insert 100k lines in a newly created 1k columns table
        PreparedStatement insertStatement = session.prepare(generateJava349InsertStatement());
        for (int key = 0; key < numberOfInserts; key++) {
            session.executeAsync(insertStatement.bind(key)).addListener(progressReporter, progressReportExecutor);
        }

        // Wait for all inserts to happen and stop connections and progress tracking
        pendingInserts.await();
        openConnectionsWatcherExecutor.shutdownNow();
        progressReportExecutor.shutdownNow();

        if (negativeOpenConnectionCountSpotted.get()) {
            fail("Negative value spotted for open connection count");
        }
    }

    private String generateJava349InsertStatement() {
        StringBuilder sb = new StringBuilder("INSERT INTO Java349 (mykey");
        for (int i = 0; i < 1000; i++) {
            sb.append(", column").append(i);
        }
        sb.append(") VALUES (?");
        for (int i = 0; i < 1000; i++) {
            sb.append(", ").append(i);
        }
        sb.append(");");
        return sb.toString();
    }
}
