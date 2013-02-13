package com.datastax.driver.core;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.*;

import org.apache.cassandra.transport.Event;
import org.apache.cassandra.transport.messages.RegisterMessage;
import org.apache.cassandra.transport.messages.QueryMessage;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.policies.*;
import com.datastax.driver.core.exceptions.DriverInternalError;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

class ControlConnection implements Host.StateListener {

    private static final Logger logger = LoggerFactory.getLogger(ControlConnection.class);

    // TODO: we might want to make that configurable
    private static final long MAX_SCHEMA_AGREEMENT_WAIT_MS = 10000;

    private static final InetAddress bindAllAddress;
    static
    {
        try {
            bindAllAddress = InetAddress.getByAddress(new byte[4]);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    private static final String SELECT_KEYSPACES = "SELECT * FROM system.schema_keyspaces";
    private static final String SELECT_COLUMN_FAMILIES = "SELECT * FROM system.schema_columnfamilies";
    private static final String SELECT_COLUMNS = "SELECT * FROM system.schema_columns";

    private static final String SELECT_PEERS = "SELECT peer, data_center, rack, tokens, rpc_address FROM system.peers";
    private static final String SELECT_LOCAL = "SELECT cluster_name, data_center, rack, tokens, partitioner FROM system.local WHERE key='local'";

    private static final String SELECT_SCHEMA_PEERS = "SELECT rpc_address, schema_version FROM system.peers";
    private static final String SELECT_SCHEMA_LOCAL = "SELECT schema_version FROM system.local WHERE key='local'";

    private final AtomicReference<Connection> connectionRef = new AtomicReference<Connection>();

    private final Cluster.Manager cluster;
    private final LoadBalancingPolicy balancingPolicy;

    private final ReconnectionPolicy reconnectionPolicy = new ExponentialReconnectionPolicy(2 * 1000, 5 * 60 * 1000);
    private final AtomicReference<ScheduledFuture> reconnectionAttempt = new AtomicReference<ScheduledFuture>();

    private volatile boolean isShutdown;

    public ControlConnection(Cluster.Manager manager, Metadata metadata) {
        this.cluster = manager;
        this.balancingPolicy = new RoundRobinPolicy();
        this.balancingPolicy.init(manager.getCluster(), metadata.allHosts());
    }

    // Only for the initial connection. Does not schedule retries if it fails
    public void connect() throws NoHostAvailableException {
        if (isShutdown)
            return;

        setNewConnection(reconnectInternal());
    }

    public void shutdown() {
        isShutdown = true;
        Connection connection = connectionRef.get();
        if (connection != null)
            connection.close();
    }

    private void reconnect() {
        if (isShutdown)
            return;

        try {
            setNewConnection(reconnectInternal());
        } catch (NoHostAvailableException e) {
            logger.error("[Control connection] Cannot connect to any host, scheduling retry");
            new AbstractReconnectionHandler(cluster.reconnectionExecutor, reconnectionPolicy.newSchedule(), reconnectionAttempt) {
                protected Connection tryReconnect() throws ConnectionException {
                    try {
                        return reconnectInternal();
                    } catch (NoHostAvailableException e) {
                        throw new ConnectionException(null, e.getMessage());
                    }
                }

                protected void onReconnection(Connection connection) {
                    setNewConnection(connection);
                }

                protected boolean onConnectionException(ConnectionException e, long nextDelayMs) {
                    logger.error("[Control connection] Cannot connect to any host, scheduling retry in {} milliseconds", nextDelayMs);
                    return true;
                }

                protected boolean onUnknownException(Exception e, long nextDelayMs) {
                    logger.error(String.format("[Control connection] Unknown error during reconnection, scheduling retry in %d milliseconds", nextDelayMs), e);
                    return true;
                }
            }.start();
        }
    }

    private void setNewConnection(Connection newConnection) {
        logger.debug("[Control connection] Successfully connected to {}", newConnection.address);
        Connection old = connectionRef.getAndSet(newConnection);
        if (old != null && !old.isClosed())
            old.close();
    }

    private Connection reconnectInternal() throws NoHostAvailableException {

        Iterator<Host> iter = balancingPolicy.newQueryPlan(Query.DEFAULT);
        Map<InetAddress, String> errors = null;

        Host host = null;
        try {
            while (iter.hasNext()) {
                host = iter.next();
                try {
                    return tryConnect(host);
                } catch (ConnectionException e) {
                    errors = logError(host, e.getMessage(), errors, iter);
                } catch (ExecutionException e) {
                    errors = logError(host, e.getMessage(), errors, iter);
                }
            }
        } catch (InterruptedException e) {
            // Sets interrupted status
            Thread.currentThread().interrupt();

            // Indicates that all remaining hosts are skipped due to the interruption
            if (host != null)
                errors = logError(host, "Connection thread interrupted", errors, iter);
            while (iter.hasNext())
                errors = logError(iter.next(), "Connection thread interrupted", errors, iter);
        }
        throw new NoHostAvailableException(errors == null ? Collections.<InetAddress, String>emptyMap() : errors);
    }

    private static Map<InetAddress, String> logError(Host host, String msg, Map<InetAddress, String> errors, Iterator<Host> iter) {
        if (errors == null)
            errors = new HashMap<InetAddress, String>();
        errors.put(host.getAddress(), msg);

        if (logger.isDebugEnabled()) {
            if (iter.hasNext()) {
                logger.debug("[Control connection] error on {} connection ({}), trying next host", host, msg);
            } else {
                logger.debug("[Control connection] error on {} connection ({}), no more host to try", host, msg);
            }
        }
        return errors;
    }

    private Connection tryConnect(Host host) throws ConnectionException, ExecutionException, InterruptedException {
        Connection connection = cluster.connectionFactory.open(host);

        try {
            logger.trace("[Control connection] Registering for events");
            List<Event.Type> evs = Arrays.asList(new Event.Type[]{
                Event.Type.TOPOLOGY_CHANGE,
                Event.Type.STATUS_CHANGE,
                Event.Type.SCHEMA_CHANGE,
            });
            connection.write(new RegisterMessage(evs));

            logger.debug(String.format("[Control connection] Refreshing node list and token map"));
            refreshNodeListAndTokenMap(connection);

            logger.debug("[Control connection] Refreshing schema");
            refreshSchema(connection, null, null, cluster);
            return connection;
        } catch (BusyConnectionException e) {
            throw new DriverInternalError("Newly created connection should not be busy");
        }
    }

    public void refreshSchema(String keyspace, String table) throws InterruptedException {
        logger.debug("[Control connection] Refreshing schema for {}.{}", keyspace, table);
        try {
            refreshSchema(connectionRef.get(), keyspace, table, cluster);
        } catch (ConnectionException e) {
            logger.debug("[Control connection] Connection error while refeshing schema ({})", e.getMessage());
            reconnect();
        } catch (ExecutionException e) {
            logger.error("[Control connection] Unexpected error while refeshing schema", e);
            reconnect();
        } catch (BusyConnectionException e) {
            logger.debug("[Control connection] Connection is busy, reconnecting");
            reconnect();
        }
    }

    static void refreshSchema(Connection connection, String keyspace, String table, Cluster.Manager cluster) throws ConnectionException, BusyConnectionException, ExecutionException, InterruptedException {
        // Make sure we're up to date on schema
        String whereClause = "";
        if (keyspace != null) {
            whereClause = " WHERE keyspace_name = '" + keyspace + "'";
            if (table != null)
                whereClause += " AND columnfamily_name = '" + table + "'";
        }

        ResultSetFuture ksFuture = table == null
                                 ? new ResultSetFuture(null, new QueryMessage(SELECT_KEYSPACES + whereClause, ConsistencyLevel.DEFAULT_CASSANDRA_CL))
                                 : null;
        ResultSetFuture cfFuture = new ResultSetFuture(null, new QueryMessage(SELECT_COLUMN_FAMILIES + whereClause, ConsistencyLevel.DEFAULT_CASSANDRA_CL));
        ResultSetFuture colsFuture = new ResultSetFuture(null, new QueryMessage(SELECT_COLUMNS + whereClause, ConsistencyLevel.DEFAULT_CASSANDRA_CL));

        if (ksFuture != null)
            connection.write(ksFuture.callback);
        connection.write(cfFuture.callback);
        connection.write(colsFuture.callback);

        cluster.metadata.rebuildSchema(keyspace, table, ksFuture == null ? null : ksFuture.get(), cfFuture.get(), colsFuture.get());
    }

    public void refreshNodeListAndTokenMap() {
        Connection c = connectionRef.get();
        // At startup, when we add the initial nodes, this will be null, which is ok
        if (c == null)
            return;

        logger.debug(String.format("[Control connection] Refreshing node list and token map"));
        try {
            refreshNodeListAndTokenMap(connectionRef.get());
        } catch (ConnectionException e) {
            logger.debug("[Control connection] Connection error while refeshing node list and token map ({})", e.getMessage());
            reconnect();
        } catch (ExecutionException e) {
            logger.error("[Control connection] Unexpected error while refeshing node list and token map", e);
            reconnect();
        } catch (BusyConnectionException e) {
            logger.debug("[Control connection] Connection is busy, reconnecting");
            reconnect();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.debug("[Control connection] Interrupted while refreshing node list and token map, skipping it.");
        }
    }

    private void refreshNodeListAndTokenMap(Connection connection) throws ConnectionException, BusyConnectionException, ExecutionException, InterruptedException {
        // Make sure we're up to date on nodes and tokens

        ResultSetFuture peersFuture = new ResultSetFuture(null, new QueryMessage(SELECT_PEERS, ConsistencyLevel.DEFAULT_CASSANDRA_CL));
        ResultSetFuture localFuture = new ResultSetFuture(null, new QueryMessage(SELECT_LOCAL, ConsistencyLevel.DEFAULT_CASSANDRA_CL));
        connection.write(peersFuture.callback);
        connection.write(localFuture.callback);

        String partitioner = null;
        Map<Host, Collection<String>> tokenMap = new HashMap<Host, Collection<String>>();

        // Update cluster name, DC and rack for the one node we are connected to
        Row localRow = localFuture.get().one();
        if (localRow != null) {
            String clusterName = localRow.getString("cluster_name");
            if (clusterName != null)
                cluster.metadata.clusterName = clusterName;

            Host host = cluster.metadata.getHost(connection.address);
            // In theory host can't be null. However there is no point in risking a NPE in case we
            // have a race between a node removal and this.
            if (host != null)
                host.setLocationInfo(localRow.getString("data_center"), localRow.getString("rack"));

            partitioner = localRow.getString("partitioner");
            Set<String> tokens = localRow.getSet("tokens", String.class);
            if (partitioner != null && !tokens.isEmpty())
                tokenMap.put(host, tokens);
        }

        List<InetAddress> foundHosts = new ArrayList<InetAddress>();
        List<String> dcs = new ArrayList<String>();
        List<String> racks = new ArrayList<String>();
        List<Set<String>> allTokens = new ArrayList<Set<String>>();

        for (Row row : peersFuture.get()) {

            InetAddress addr = row.getInet("rpc_address");
            if (addr == null) {
                addr = row.getInet("peer");
                logger.error("No rpc_address found for host {} in {}'s peers system table. That should not happen but using address {} instead", addr, connection.address, addr);
            } else if (addr.equals(bindAllAddress)) {
                addr = row.getInet("peer");
            }

            foundHosts.add(addr);
            dcs.add(row.getString("data_center"));
            racks.add(row.getString("rack"));
            allTokens.add(row.getSet("tokens", String.class));
        }

        for (int i = 0; i < foundHosts.size(); i++) {
            Host host = cluster.metadata.getHost(foundHosts.get(i));
            if (host == null) {
                // We don't know that node, add it.
                host = cluster.addHost(foundHosts.get(i), true);
            }
            host.setLocationInfo(dcs.get(i), racks.get(i));

            if (partitioner != null && !allTokens.get(i).isEmpty())
                tokenMap.put(host, allTokens.get(i));
        }

        // Removes all those that seems to have been removed (since we lost the control connection)
        Set<InetAddress> foundHostsSet = new HashSet<InetAddress>(foundHosts);
        for (Host host : cluster.metadata.allHosts())
            if (!host.getAddress().equals(connection.address) && !foundHostsSet.contains(host.getAddress()))
                cluster.removeHost(host);

        if (partitioner != null)
            cluster.metadata.rebuildTokenMap(partitioner, tokenMap);
    }

    static boolean waitForSchemaAgreement(Connection connection, Metadata metadata) throws ConnectionException, BusyConnectionException, ExecutionException, InterruptedException {

        long start = System.currentTimeMillis();
        long elapsed = 0;
        while (elapsed < MAX_SCHEMA_AGREEMENT_WAIT_MS) {
            ResultSetFuture peersFuture = new ResultSetFuture(null, new QueryMessage(SELECT_SCHEMA_PEERS, ConsistencyLevel.DEFAULT_CASSANDRA_CL));
            ResultSetFuture localFuture = new ResultSetFuture(null, new QueryMessage(SELECT_SCHEMA_LOCAL, ConsistencyLevel.DEFAULT_CASSANDRA_CL));
            connection.write(peersFuture.callback);
            connection.write(localFuture.callback);

            Set<UUID> versions = new HashSet<UUID>();

            Row localRow = localFuture.get().one();
            if (localRow != null && !localRow.isNull("schema_version"))
                versions.add(localRow.getUUID("schema_version"));

            for (Row row : peersFuture.get()) {

                if (row.isNull("rpc_address") || row.isNull("schema_version"))
                    continue;

                InetAddress rpc = row.getInet("rpc_address");
                if (rpc.equals(bindAllAddress))
                    rpc = row.getInet("peer");

                Host peer = metadata.getHost(rpc);
                if (peer != null && peer.getMonitor().isUp())
                    versions.add(row.getUUID("schema_version"));
            }

            if (versions.size() <= 1)
                return true;

            // let's not flood the node too much
            Thread.sleep(200);

            elapsed = System.currentTimeMillis() - start;
        }

        return false;
    }

    boolean isOpen() {
        Connection c = connectionRef.get();
        return c != null && !c.isClosed();
    }

    public void onUp(Host host) {
        balancingPolicy.onUp(host);
    }

    public void onDown(Host host) {
        balancingPolicy.onDown(host);

        // If that's the host we're connected to, and we haven't yet schedul a reconnection, pre-emptively start one
        Connection current = connectionRef.get();
        if (logger.isTraceEnabled())
            logger.trace("[Control connection] %s is down, currently connected to {}", host, current == null ? "nobody" : current.address);
        if (current != null && current.address.equals(host.getAddress()) && reconnectionAttempt.get() == null)
            reconnect();
    }

    public void onAdd(Host host) {
        balancingPolicy.onAdd(host);
        refreshNodeListAndTokenMap();
    }

    public void onRemove(Host host) {
        balancingPolicy.onRemove(host);
        refreshNodeListAndTokenMap();
    }
}
