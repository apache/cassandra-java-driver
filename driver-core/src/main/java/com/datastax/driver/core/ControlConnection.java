package com.datastax.driver.core;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.*;

import com.datastax.driver.core.transport.Connection;
import com.datastax.driver.core.transport.ConnectionException;
import com.datastax.driver.core.utils.RoundRobinPolicy;

import org.apache.cassandra.transport.Event;
import org.apache.cassandra.transport.messages.RegisterMessage;
import org.apache.cassandra.transport.messages.QueryMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ControlConnection implements Host.StateListener {

    private static final Logger logger = LoggerFactory.getLogger(ControlConnection.class);

    private static final String SELECT_KEYSPACES = "SELECT * FROM system.schema_keyspaces";
    private static final String SELECT_COLUMN_FAMILIES = "SELECT * FROM system.schema_columnfamilies";
    private static final String SELECT_COLUMNS = "SELECT * FROM system.schema_columns";

    private final AtomicReference<Connection> connectionRef = new AtomicReference<Connection>();

    private final Cluster.Manager cluster;
    private final LoadBalancingPolicy balancingPolicy;

    private final ReconnectionPolicy.Factory reconnectionPolicyFactory = ReconnectionPolicy.Exponential.makeFactory(2 * 1000, 5 * 60 * 1000);
    private final AtomicReference<ScheduledFuture> reconnectionAttempt = new AtomicReference<ScheduledFuture>();

    public ControlConnection(Cluster.Manager cluster) {
        this.cluster = cluster;
        this.balancingPolicy = RoundRobinPolicy.Factory.INSTANCE.create(cluster.metadata.allHosts());
    }

    public void reconnect() {
        try {
            setNewConnection(reconnectInternal());
        } catch (ConnectionException e) {
            logger.error("[Control connection] Cannot connect to any host, scheduling retry");
            new AbstractReconnectionHandler(cluster.scheduledExecutor, reconnectionPolicyFactory.create(), reconnectionAttempt) {
                protected Connection tryReconnect() throws ConnectionException {
                    return reconnectInternal();
                }

                protected void onReconnection(Connection connection) {
                    setNewConnection(connection);
                }

                protected boolean onConnectionException(ConnectionException e, long nextDelayMs) {
                    logger.error(String.format("[Control connection] Cannot connect to any host, scheduling retry in %d milliseconds", nextDelayMs));
                    return true;
                }

                protected boolean onUnknownException(Exception e, long nextDelayMs) {
                    logger.error(String.format("[Control connection ]Unknown error during reconnection, scheduling retry in %d milliseconds", nextDelayMs), e);
                    return true;
                }
            }.start();
        }
    }

    private void setNewConnection(Connection newConnection) {
        logger.debug(String.format("[Control connection] Successfully connected to %s", newConnection.address));
        Connection old = connectionRef.getAndSet(newConnection);
        if (old != null && !old.isClosed())
            old.close();
    }

    private Connection reconnectInternal() throws ConnectionException {

        Iterator<Host> iter = balancingPolicy.newQueryPlan();
        while (iter.hasNext()) {
            Host host = iter.next();
            try {
                return tryConnect(host);
            } catch (ConnectionException e) {
                if (iter.hasNext()) {
                    logger.debug(String.format("[Control connection] Failed connecting to %s, trying next host", host));
                } else {
                    logger.debug(String.format("[Control connection] Failed connecting to %s, no more host to try", host));
                    throw e;
                }
            }
        }
        throw new ConnectionException(null, "Cannot connect to any host");
    }

    private Connection tryConnect(Host host) throws ConnectionException {
        Connection connection = cluster.connectionFactory.open(host);

        logger.trace("[Control connection] Registering for events");
        List<Event.Type> evs = Arrays.asList(new Event.Type[]{
            Event.Type.TOPOLOGY_CHANGE,
            Event.Type.STATUS_CHANGE,
            //Event.Type.SCHEMA_CHANGE,
        });
        connection.write(new RegisterMessage(evs));

        logger.trace("[Control connection] Refreshing schema");
        refreshSchema(connection);
        // TODO: also catch up on potentially missed nodes (and node that happens to be up but not known to us)
        return connection;
    }

    private void refreshSchema(Connection connection) {
        // Make sure we're up to date on metadata
        try {
            ResultSet.Future ksFuture = new ResultSet.Future(null, new QueryMessage(SELECT_KEYSPACES));
            ResultSet.Future cfFuture = new ResultSet.Future(null, new QueryMessage(SELECT_COLUMN_FAMILIES));
            ResultSet.Future colsFuture = new ResultSet.Future(null, new QueryMessage(SELECT_COLUMNS));
            connection.write(ksFuture);
            connection.write(cfFuture);
            connection.write(colsFuture);

            // TODO: we should probably do something more fancy, like check if the schema changed and notify whoever wants to be notified
            cluster.metadata.rebuildSchema(ksFuture.get(), cfFuture.get(), colsFuture.get());
        } catch (ConnectionException e) {
            logger.debug(String.format("[Control connection] Connection error when refeshing schema (%s)", e.getMessage()));
            reconnect();
        } catch (ExecutionException e) {
            logger.error("[Control connection] Unexpected error while refeshing schema", e);
            reconnect();
        } catch (InterruptedException e) {
            // TODO: it's bad to do that but at the same time it's annoying to be interrupted
            throw new RuntimeException(e);
        }
    }

    public void onUp(Host host) {
        balancingPolicy.onUp(host);
    }

    public void onDown(Host host) {
        balancingPolicy.onDown(host);

        // If that's the host we're connected to, and we haven't yet schedul a reconnection, pre-emptively start one
        Connection current = connectionRef.get();
        logger.trace(String.format("[Control connection] %s is down, currently connected to %s", host, current == null ? "nobody" : current.address));
        if (current != null && current.address.equals(host.getAddress()) && reconnectionAttempt.get() == null)
            reconnect();
    }

    public void onAdd(Host host) {
        balancingPolicy.onAdd(host);
    }

    public void onRemove(Host host) {
        balancingPolicy.onRemove(host);
    }
}
