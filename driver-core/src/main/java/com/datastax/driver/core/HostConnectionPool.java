package com.datastax.driver.core;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.configuration.*;

// TODO: We should allow changing the core pool size (i.e. have a method that
// adds new connection or trash existing one)
class HostConnectionPool {
    private static final Logger logger = LoggerFactory.getLogger(HostConnectionPool.class);

    public final Host host;
    public volatile HostDistance hostDistance;
    private final Session.Manager manager;

    private final List<Connection> connections;
    private final AtomicInteger open;
    private final AtomicBoolean isShutdown = new AtomicBoolean();
    private final Set<Connection> trash = new CopyOnWriteArraySet();

    private final Lock waitLock = new ReentrantLock(true);
    private final Condition hasAvailableConnection = waitLock.newCondition();

    private final Runnable newConnectionTask;

    public HostConnectionPool(Host host, HostDistance hostDistance, Session.Manager manager) throws ConnectionException {
        this.host = host;
        this.hostDistance = hostDistance;
        this.manager = manager;

        this.newConnectionTask = new Runnable() {
            public void run() {
                addConnectionIfUnderMaximum();
            }
        };

        // Create initial core connections
        List<Connection> l = new ArrayList<Connection>(options().getCoreConnectionsPerHost(hostDistance));
        for (int i = 0; i < options().getCoreConnectionsPerHost(hostDistance); i++)
            l.add(manager.connectionFactory().open(host));
        this.connections = new CopyOnWriteArrayList(l);
        this.open = new AtomicInteger(connections.size());

        logger.trace(String.format("Created connection pool to host %s", host));
    }

    private PoolingOptions options() {
        return manager.configuration().getConnectionsConfiguration().getPoolingOptions();
    }

    public Connection borrowConnection(long timeout, TimeUnit unit) throws ConnectionException, TimeoutException {
        if (isShutdown.get())
            // TODO: have a specific exception
            throw new ConnectionException(host.getAddress(), "Pool is shutdown");

        if (connections.isEmpty()) {
            for (int i = 0; i < options().getCoreConnectionsPerHost(hostDistance); i++)
                spawnNewConnection();
            return waitForConnection(timeout, unit);
        }

        int minInFlight = Integer.MAX_VALUE;
        Connection leastBusy = null;
        for (Connection connection : connections) {
            int inFlight = connection.inFlight.get();
            if (inFlight < minInFlight) {
                minInFlight = inFlight;
                leastBusy = connection;
            }
        }

        if (minInFlight >= options().getMaxSimultaneousRequestsPerConnectionTreshold(hostDistance) && connections.size() < options().getMaxConnectionPerHost(hostDistance))
            spawnNewConnection();

        while (true) {
            int inFlight = leastBusy.inFlight.get();

            if (inFlight >= Connection.MAX_STREAM_PER_CONNECTION) {
                leastBusy = waitForConnection(timeout, unit);
                break;
            }

            if (leastBusy.inFlight.compareAndSet(inFlight, inFlight + 1))
                break;
        }
        leastBusy.setKeyspace(manager.poolsState.keyspace);
        return leastBusy;
    }

    private static long elapsed(long start, TimeUnit unit) {
        return unit.convert(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
    }

    private void awaitAvailableConnection(long timeout, TimeUnit unit) {
        waitLock.lock();
        try {
            hasAvailableConnection.await(timeout, unit);
        } catch (InterruptedException e) {
            // TODO: Do we want to stop ignoring that?
        } finally {
            waitLock.unlock();
        }
    }

    private void signalAvailableConnection() {
        waitLock.lock();
        try {
            hasAvailableConnection.signal();
        } finally {
            waitLock.unlock();
        }
    }

    private void signalAllAvailableConnection() {
        waitLock.lock();
        try {
            hasAvailableConnection.signal();
        } finally {
            waitLock.unlock();
        }
    }


    private Connection waitForConnection(long timeout, TimeUnit unit) throws ConnectionException, TimeoutException {
        long start = System.currentTimeMillis();
        long remaining = timeout;
        do {
            awaitAvailableConnection(remaining, unit);

            if (isShutdown())
                throw new ConnectionException(host.getAddress(), "Pool is shutdown");

            int minInFlight = Integer.MAX_VALUE;
            Connection leastBusy = null;
            for (Connection connection : connections) {
                int inFlight = connection.inFlight.get();
                if (inFlight < minInFlight) {
                    minInFlight = inFlight;
                    leastBusy = connection;
                }
            }

            while (true) {
                int inFlight = leastBusy.inFlight.get();

                if (inFlight >= Connection.MAX_STREAM_PER_CONNECTION)
                    break;

                if (leastBusy.inFlight.compareAndSet(inFlight, inFlight + 1))
                    return leastBusy;
            }

            remaining = timeout - elapsed(start, unit);
        } while (remaining > 0);

        throw new TimeoutException();
    }

    public void returnConnection(Connection connection) {
        int inFlight = connection.inFlight.decrementAndGet();

        if (connection.isDefunct()) {
            if (host.getMonitor().signalConnectionFailure(connection.lastException()))
                shutdown();
            else
                replace(connection);
        } else {

            if (trash.contains(connection) && inFlight == 0) {
                if (trash.remove(connection))
                    close(connection);
                return;
            }

            if (connections.size() > options().getCoreConnectionsPerHost(hostDistance) && inFlight <= options().getMinSimultaneousRequestsPerConnectionTreshold(hostDistance)) {
                trashConnection(connection);
            } else {
                signalAvailableConnection();
            }
        }
    }

    private boolean trashConnection(Connection connection) {
        // First, make sure we don't go below core connections
        for(;;) {
            int opened = open.get();
            if (opened <= options().getCoreConnectionsPerHost(hostDistance))
                return false;

            if (open.compareAndSet(opened, opened - 1))
                break;
        }
        trash.add(connection);
        connections.remove(connection);

        if (connection.inFlight.get() == 0 && trash.remove(connection))
            close(connection);
        return true;
    }

    private boolean addConnectionIfUnderMaximum() {

        // First, make sure we don't cross the allowed limit of open connections
        for(;;) {
            int opened = open.get();
            if (opened >= options().getMaxConnectionPerHost(hostDistance))
                return false;

            if (open.compareAndSet(opened, opened + 1))
                break;
        }

        if (isShutdown()) {
            open.decrementAndGet();
            return false;
        }

        // Now really open the connection
        try {
            connections.add(manager.connectionFactory().open(host));
            signalAvailableConnection();
            return true;
        } catch (ConnectionException e) {
            open.decrementAndGet();
            logger.debug("Connection error to " + host + " while creating additional connection");
            if (host.getMonitor().signalConnectionFailure(e))
                shutdown();
            return false;
        }
    }

    private void spawnNewConnection() {
        manager.executor().submit(newConnectionTask);
    }

    private void replace(final Connection connection) {
        connections.remove(connection);

        manager.executor().submit(new Runnable() {
            public void run() {
                connection.close();
                addConnectionIfUnderMaximum();
            }
        });
    }

    private void close(final Connection connection) {
        manager.executor().submit(new Runnable() {
            public void run() {
                connection.close();
            }
        });
    }

    public boolean isShutdown() {
        return isShutdown.get();
    }

    public void shutdown() {
        if (!isShutdown.compareAndSet(false, true))
            return;

        logger.debug("Shutting down pool");

        // Wake up all threads that waits
        signalAllAvailableConnection();
        discardAvailableConnections();
    }

    private void discardAvailableConnections() {
        for (Connection connection : connections) {
            connection.close();
            open.decrementAndGet();
        }
    }

    static class PoolState {

        private volatile String keyspace;

        public void setKeyspace(String keyspace) {
            this.keyspace = keyspace;
        }
    }
}
