package com.datastax.driver.core;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;

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

    private Connection connection;

    private final Cluster.Manager cluster;
    private final LoadBalancingPolicy balancingPolicy;

    public ControlConnection(Cluster.Manager cluster) {
        this.cluster = cluster;
        this.balancingPolicy = RoundRobinPolicy.Factory.INSTANCE.create(cluster.metadata.allHosts());
    }

    public void reconnect() {

        if (connection != null)
            connection.close();

        Iterator<Host> iter = balancingPolicy.newQueryPlan();
        while (iter.hasNext()) {
            Host host = iter.next();
            try {
                tryConnect(host);
                return;
            } catch (ConnectionException e) {
                // TODO: log something
                // Just try next node
            }
        }

        // TODO: we should log an error but reschedule for later
        throw new RuntimeException();
    }

    public void tryConnect(Host host) throws ConnectionException {
        connection = cluster.connectionFactory.open(host);
        logger.trace(String.format("Control connection connected to %s", host));

        List<Event.Type> evs = Arrays.asList(new Event.Type[]{
            Event.Type.TOPOLOGY_CHANGE,
            Event.Type.STATUS_CHANGE,
            //Event.Type.SCHEMA_CHANGE,
        });
        connection.write(new RegisterMessage(evs));

        refreshSchema();
        // TODO: also catch up on potentially missed nodes (and node that happens to be up but not known to us)
    }

    public void refreshSchema() {
        // Shouldn't happen unless we have bigger problems, but no reason to NPE
        if (connection == null || connection.isClosed()) {
            reconnect();
        }

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
            // TODO: log
            reconnect();
        } catch (ExecutionException e) {
            // TODO: log and decide what to do since in theory that shouldn't be a cassandra exception
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

        // TODO: we should look if that's the host we're connected with and
        // attempt a reconnect. However we need to make that thread safe
        // somehow.
    }

    public void onAdd(Host host) {
        balancingPolicy.onAdd(host);
    }

    public void onRemove(Host host) {
        balancingPolicy.onRemove(host);
    }
}
