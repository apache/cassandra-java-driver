package com.datastax.driver.core.pool;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.transport.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HostConnectionPool {

    private static final Logger logger = LoggerFactory.getLogger(HostConnectionPool.class);

    private final Host host;
    private final Host.HealthMonitor.Signaler failureSignaler;

    private final Connection.Factory factory;

    private final AtomicBoolean isShutdown = new AtomicBoolean();

    private final BlockingQueue<Connection> available = new LinkedBlockingQueue<Connection>();

    private final AtomicInteger open = new AtomicInteger(0);
    private final AtomicInteger borrowed = new AtomicInteger(0);
    private final AtomicInteger waitingThreads = new AtomicInteger(0);

    private final Configuration configuration;

    // TODO: We could share that executor across pools
    private final ExecutorService openExecutor = Executors.newCachedThreadPool();
    private final Runnable newConnectionTask;

    public HostConnectionPool(Host host, Host.HealthMonitor.Signaler signaler, Connection.Factory factory, Configuration configuration) {
        this.host = host;
        this.failureSignaler = signaler;
        this.factory = factory;
        this.configuration = configuration;

        this.newConnectionTask = new Runnable() {
            public void run() {
                // If when we execute there is still some waiting threads, create a connection
                if (waitingThreads.get() > 0)
                    addConnectionIfUnderMaximum();
            }
        };

        // Create initial core connections
        for (int i = 0; i < configuration.coreConnections; i++)
            if (!addConnection())
                break;
    }

    public Connection borrowConnection(long timeout, TimeUnit unit) throws ConnectionException {
        if (isShutdown.get())
            // TODO: have a specific exception
            throw new ConnectionException(host.getAddress(), "Pool is shutdown");

        Connection connection = available.poll();
        if (connection == null)
        {
            // Request the opening of a connection, unless we already know there is too much
            if (open.get() < configuration.maxConnections)
                openExecutor.submit(newConnectionTask);

            connection = waitForConnection(timeout, unit);
        }

        borrowed.incrementAndGet();
        connection.setKeyspace(configuration.keyspace);
        return connection;
    }

    private boolean addConnectionIfUnderMaximum() {

        // First, make sure we don't cross the allowed limit of open connections
        for(;;) {
            int opened = open.get();
            if (opened >= configuration.maxConnections)
                return false;

            if (open.compareAndSet(opened, opened + 1))
                break;
        }
        return addConnection();
    }

    private boolean addConnection() {
        try {
            available.offer(factory.open(host));

            if (isShutdown.get()) {
                discardAvailableConnections();
                return false;
            } else {
                return true;
            }
        } catch (ConnectionException e) {
            logger.debug("Connection error to " + host + ", signaling monitor");
            if (failureSignaler.signalConnectionFailure(e))
                shutdown();
            return false;
        }
    }

    // This is guaranteed to either return a connection or throw an exception
    private Connection waitForConnection(long timeout, TimeUnit unit) throws ConnectionException {
        waitingThreads.incrementAndGet();
        try {
            Connection connection = available.poll(timeout, unit);
            if (connection == null)
                // TODO: maybe create a special exception for that
                throw new ConnectionException(host.getAddress(), "No free connection available");
            return connection;
        } catch (InterruptedException e) {
            throw new RuntimeException();
        } finally {
            waitingThreads.decrementAndGet();
        }
    }

    public void returnConnection(Connection connection) {
        borrowed.decrementAndGet();

        if (connection.isDefunct()) {
            if (failureSignaler.signalConnectionFailure(connection.lastException()))
                shutdown();
            // TODO: make the close async
            connection.close();
            open.decrementAndGet();
            return;
        }

        // Return the connection as available if we have <= core connections opened, or if we have waiting threads.
        // Otherwise, close it (but if some other thread beats us at closing, keep available)
        if (waitingThreads.get() > 0 || open.get() <= configuration.coreConnections || !closeConnectionIfIdle(connection)) {
            available.offer(connection);

            // Sanity check
            if (isShutdown.get())
                discardAvailableConnections();
        }
    }

    public boolean closeConnectionIfIdle(Connection connection) {
        for (;;) {
            int opened = open.get();
            if (opened <= configuration.coreConnections) {
                return false;
            }
            assert opened > 0;
            if (open.compareAndSet(opened, opened - 1))
                break;
        }
        // TODO: maybe we should do the close asynchronously?
        connection.close();
        open.decrementAndGet();
        return true;
    }

    // Open connections if there is < core and close some if there is > core and some are idle
    public void ensureCoreSize() {
        int opened = open.get();
        if (opened < configuration.coreConnections) {
            while (addConnectionIfUnderMaximum());
        } else {
            Connection connection = available.poll();
            while (connection != null && closeConnectionIfIdle(connection))
                connection = available.poll();
        }
    }

    public boolean isShutdown() {
        return isShutdown.get();
    }

    public void shutdown() {
        if (!isShutdown.compareAndSet(false, true))
            return;

        logger.debug("Shutting down pool");

        // TODO: we can have threads waiting for connection on the queue.
        // It would be nice to be able to wake them up here (otherwise they
        // will have to wait for the timeout). One option would be to feed some
        // fake connections object to available that borrow would recognize

        discardAvailableConnections();
    }

    private void discardAvailableConnections() {
        while (!available.isEmpty()) {
            // TODO: If we make the close async, wait for it here
            available.poll().close();
            open.decrementAndGet();
        }
    }

    public static class Configuration {

        private volatile String keyspace;

        private volatile int coreConnections = 2;
        private volatile int maxConnections = 100;

        public void setKeyspace(String keyspace) {
            this.keyspace = keyspace;
        }

        public void setCoreConnections(int value) {
            coreConnections = value;
        }

        public int getCoreConnections() {
            return coreConnections;
        }

        public void setMaxConnections(int value) {
            maxConnections = value;
        }

        public int getMaxConnections() {
            return maxConnections;
        }
    }
}
