/*
 *      Copyright (C) 2012 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.*;

import com.google.common.base.Objects;

import org.apache.cassandra.transport.Event;
import org.apache.cassandra.transport.messages.RegisterMessage;
import org.apache.cassandra.transport.messages.QueryMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.policies.*;
import com.datastax.driver.core.exceptions.DriverInternalError;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

class ControlConnection implements Host.StateListener {

    private static final Logger logger = LoggerFactory.getLogger(ControlConnection.class);

    // TODO: we might want to make that configurable
    static final long MAX_SCHEMA_AGREEMENT_WAIT_MS = 10000;

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

    private static final String SELECT_SCHEMA_PEERS = "SELECT peer, rpc_address, schema_version FROM system.peers";
    private static final String SELECT_SCHEMA_LOCAL = "SELECT schema_version FROM system.local WHERE key='local'";

    private final AtomicReference<Connection> connectionRef = new AtomicReference<Connection>();

    private final Cluster.Manager cluster;

    private final AtomicReference<ScheduledFuture<?>> reconnectionAttempt = new AtomicReference<ScheduledFuture<?>>();

    private volatile boolean isShutdown;

    public ControlConnection(Cluster.Manager manager) {
        this.cluster = manager;
    }

    // Only for the initial connection. Does not schedule retries if it fails
    public void connect() {
        if (isShutdown)
            return;

        setNewConnection(reconnectInternal());
    }

    public boolean shutdown(long timeout, TimeUnit unit) throws InterruptedException {
        isShutdown = true;
        Connection connection = connectionRef.get();
        return connection != null ? connection.close(timeout, unit) : true;
    }

    private void reconnect() {
        if (isShutdown)
            return;

        try {
            setNewConnection(reconnectInternal());
        } catch (NoHostAvailableException e) {
            logger.error("[Control connection] Cannot connect to any host, scheduling retry");
            new AbstractReconnectionHandler(cluster.reconnectionExecutor, cluster.reconnectionPolicy().newSchedule(), reconnectionAttempt) {
                @Override
                protected Connection tryReconnect() throws ConnectionException {
                    try {
                        return reconnectInternal();
                    } catch (NoHostAvailableException e) {
                        throw new ConnectionException(null, e.getMessage());
                    }
                }

                @Override
                protected void onReconnection(Connection connection) {
                    setNewConnection(connection);
                }

                @Override
                protected boolean onConnectionException(ConnectionException e, long nextDelayMs) {
                    logger.error("[Control connection] Cannot connect to any host, scheduling retry in {} milliseconds", nextDelayMs);
                    return true;
                }

                @Override
                protected boolean onUnknownException(Exception e, long nextDelayMs) {
                    logger.error(String.format("[Control connection] Unknown error during reconnection, scheduling retry in %d milliseconds", nextDelayMs), e);
                    return true;
                }
            }.start();
        }
    }

    private void signalError() {

        // Try just signaling the host monitor, as this will trigger a reconnect as part to marking the host down.
        Connection connection = connectionRef.get();
        if (connection != null && connection.isDefunct()) {
            Host host = cluster.metadata.getHost(connection.address);
            // Host might be null in the case the host has been removed, but it means this has
            // been reported already so it's fine.
            if (host != null) {
                cluster.signalConnectionFailure(host, connection.lastException());
                return;
            }
        }

        // If the connection is not defunct, or the host has left, just reconnect manually
        reconnect();
    }

    private void setNewConnection(Connection newConnection) {
        logger.debug("[Control connection] Successfully connected to {}", newConnection.address);
        Connection old = connectionRef.getAndSet(newConnection);
        if (old != null && !old.isClosed()) {
            try {
                old.close(0, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private Connection reconnectInternal() {

        Iterator<Host> iter = cluster.loadBalancingPolicy().newQueryPlan(Query.DEFAULT);
        Map<InetAddress, String> errors = null;

        Host host = null;
        try {
            while (iter.hasNext()) {
                host = iter.next();
                try {
                    return tryConnect(host);
                } catch (ConnectionException e) {
                    errors = logError(host, e.getMessage(), errors, iter);
                    cluster.signalConnectionFailure(host, e);
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
        logger.debug("[Control connection] Refreshing schema for {}{}", keyspace == null ? "" : keyspace, table == null ? "" : "." + table);
        try {
            refreshSchema(connectionRef.get(), keyspace, table, cluster);
        } catch (ConnectionException e) {
            logger.debug("[Control connection] Connection error while refeshing schema ({})", e.getMessage());
            signalError();
        } catch (ExecutionException e) {
            // If we're being shutdown during schema refresh, this can happen. That's fine so don't scare the user.
            if (!isShutdown)
                logger.error("[Control connection] Unexpected error while refeshing schema", e);
            signalError();
        } catch (BusyConnectionException e) {
            logger.debug("[Control connection] Connection is busy, reconnecting");
            signalError();
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
            refreshNodeListAndTokenMap(c);
        } catch (ConnectionException e) {
            logger.debug("[Control connection] Connection error while refeshing node list and token map ({})", e.getMessage());
            signalError();
        } catch (ExecutionException e) {
            // If we're being shutdown during refresh, this can happen. That's fine so don't scare the user.
            if (!isShutdown)
                logger.error("[Control connection] Unexpected error while refeshing node list and token map", e);
            signalError();
        } catch (BusyConnectionException e) {
            logger.debug("[Control connection] Connection is busy, reconnecting");
            signalError();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.debug("[Control connection] Interrupted while refreshing node list and token map, skipping it.");
        }
    }

    private void updateLocationInfo(Host host, String datacenter, String rack) {
        if (Objects.equal(host.getDatacenter(), datacenter) && Objects.equal(host.getRack(), rack))
            return;

        // If the dc/rack information changes, we need to update the load balancing policy.
        // For what, we remove and re-add the node against the policy. Not the most elegant, and assumes
        // that the policy will update correctly, but in practice this should work.
        cluster.loadBalancingPolicy().onDown(host);
        host.setLocationInfo(datacenter, rack);
        cluster.loadBalancingPolicy().onAdd(host);
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

            partitioner = localRow.getString("partitioner");

            Host host = cluster.metadata.getHost(connection.address);
            // In theory host can't be null. However there is no point in risking a NPE in case we
            // have a race between a node removal and this.
            if (host == null) {
                logger.debug("Host in local system table ({}) unknown to us (ok if said host just got removed)", connection.address);
            } else {
                updateLocationInfo(host, localRow.getString("data_center"), localRow.getString("rack"));

                Set<String> tokens = localRow.getSet("tokens", String.class);
                if (partitioner != null && !tokens.isEmpty())
                    tokenMap.put(host, tokens);
            }
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
            updateLocationInfo(host, dcs.get(i), racks.get(i));

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

        long start = System.nanoTime();
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
                if (peer != null && peer.isUp())
                    versions.add(row.getUUID("schema_version"));
            }

            logger.debug("Checking for schema agreement: versions are {}", versions);

            if (versions.size() <= 1)
                return true;

            // let's not flood the node too much
            Thread.sleep(200);

            elapsed = Cluster.timeSince(start, TimeUnit.MILLISECONDS);
        }

        return false;
    }

    boolean isOpen() {
        Connection c = connectionRef.get();
        return c != null && !c.isClosed();
    }

    @Override
    public void onUp(Host host) {
    }

    @Override
    public void onDown(Host host) {
        // If that's the host we're connected to, and we haven't yet schedule a reconnection, preemptively start one
        Connection current = connectionRef.get();
        if (logger.isTraceEnabled())
            logger.trace("[Control connection] {} is down, currently connected to {}", host, current == null ? "nobody" : current.address);
        if (current != null && current.address.equals(host.getAddress()) && reconnectionAttempt.get() == null) {
            // We might very be on an I/O thread when we reach this so we should not do that on this thread.
            // Besides, there is no reason to block the onDown method while we try to reconnect.
            cluster.executor.submit(new Runnable() {
                @Override
                public void run() {
                    reconnect();
                }
            });
        }
    }

    @Override
    public void onAdd(Host host) {
        refreshNodeListAndTokenMap();
    }

    @Override
    public void onRemove(Host host) {
        refreshNodeListAndTokenMap();
    }
}
