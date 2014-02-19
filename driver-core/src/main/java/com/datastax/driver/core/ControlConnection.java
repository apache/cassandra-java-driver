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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.exceptions.DriverException;
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

    private static final String SELECT_PEERS = "SELECT * FROM system.peers";
    private static final String SELECT_LOCAL = "SELECT * FROM system.local WHERE key='local'";

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
    public void connect() throws UnsupportedProtocolVersionException {
        if (isShutdown)
            return;

        setNewConnection(reconnectInternal());
    }

    public CloseFuture closeAsync() {
        // We don't have to be fancy here. We just set a flag so that we stop trying to reconnect (and thus change the
        // connection used) and shutdown the current one.
        isShutdown = true;
        Connection connection = connectionRef.get();
        return connection == null ? CloseFuture.immediateFuture() : connection.closeAsync();
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
                    } catch (UnsupportedProtocolVersionException e) {
                        // reconnectInternal only propagate those if we've not decided on the protocol version yet,
                        // which should only happen on the initial connection and thus in connect() but never here.
                        throw new AssertionError();
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
        } catch (UnsupportedProtocolVersionException e) {
            // reconnectInternal only propagate those if we've not decided on the protocol version yet,
            // which should only happen on the initial connection and thus in connect() but never here.
            throw new AssertionError();
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
                cluster.signalConnectionFailure(host, connection.lastException(), false);
                return;
            }
        }

        // If the connection is not defunct, or the host has left, just reconnect manually
        reconnect();
    }

    private void setNewConnection(Connection newConnection) {
        logger.debug("[Control connection] Successfully connected to {}", newConnection.address);
        Connection old = connectionRef.getAndSet(newConnection);
        if (old != null && !old.isClosed())
            old.closeAsync();
    }

    private Connection reconnectInternal() throws UnsupportedProtocolVersionException {

        Iterator<Host> iter = cluster.loadBalancingPolicy().newQueryPlan(null, Statement.DEFAULT);
        Map<InetAddress, Throwable> errors = null;

        Host host = null;
        try {
            while (iter.hasNext()) {
                host = iter.next();
                try {
                    return tryConnect(host);
                } catch (ConnectionException e) {
                    errors = logError(host, e, errors, iter);
                    cluster.signalConnectionFailure(host, e, false);
                } catch (ExecutionException e) {
                    errors = logError(host, e.getCause(), errors, iter);
                } catch (UnsupportedProtocolVersionException e) {
                    // If it's the very first node we've connected to, rethrow the exception and
                    // Cluster.init() will handle it. Otherwise, just mark this node in error.
                    if (cluster.protocolVersion() < 1)
                        throw e;
                    logger.debug("Ignoring host {}: {}", host, e.getMessage());
                    errors = logError(host, e.getCause(), errors, iter);
                }
            }
        } catch (InterruptedException e) {
            // Sets interrupted status
            Thread.currentThread().interrupt();

            // Indicates that all remaining hosts are skipped due to the interruption
            if (host != null)
                errors = logError(host, new DriverException("Connection thread interrupted"), errors, iter);
            while (iter.hasNext())
                errors = logError(iter.next(), new DriverException("Connection thread interrupted"), errors, iter);
        }
        throw new NoHostAvailableException(errors == null ? Collections.<InetAddress, Throwable>emptyMap() : errors);
    }

    private static Map<InetAddress, Throwable> logError(Host host, Throwable exception, Map<InetAddress, Throwable> errors, Iterator<Host> iter) {
        if (errors == null)
            errors = new HashMap<InetAddress, Throwable>();

        errors.put(host.getAddress(), exception);

        if (logger.isDebugEnabled()) {
            if (iter.hasNext()) {
                logger.debug("[Control connection] error on {} connection ({}), trying next host", host, exception.getMessage());
            } else {
                logger.debug("[Control connection] error on {} connection ({}), no more host to try", host, exception.getMessage());
            }
        }
        return errors;
    }

    private Connection tryConnect(Host host) throws ConnectionException, ExecutionException, InterruptedException, UnsupportedProtocolVersionException {
        Connection connection = cluster.connectionFactory.open(host);

        try {
            logger.trace("[Control connection] Registering for events");
            List<ProtocolEvent.Type> evs = Arrays.asList(new ProtocolEvent.Type[]{
                ProtocolEvent.Type.TOPOLOGY_CHANGE,
                ProtocolEvent.Type.STATUS_CHANGE,
                ProtocolEvent.Type.SCHEMA_CHANGE,
            });
            connection.write(new Requests.Register(evs));

            logger.debug("[Control connection] Refreshing node list and token map");
            refreshNodeListAndTokenMap(connection, cluster);

            logger.debug("[Control connection] Refreshing schema");
            refreshSchema(connection, null, null, cluster);
            return connection;
        } catch (BusyConnectionException e) {
            throw new DriverInternalError("Newly created connection should not be busy");
        }
    }

    public void refreshSchema(String keyspace, String table) throws InterruptedException {
        logger.debug("[Control connection] Refreshing schema for {}{}", keyspace == null ? "" : keyspace, table == null ? "" : '.' + table);
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
            whereClause = " WHERE keyspace_name = '" + keyspace + '\'';
            if (table != null)
                whereClause += " AND columnfamily_name = '" + table + '\'';
        }

        DefaultResultSetFuture ksFuture = table == null
                                        ? new DefaultResultSetFuture(null, new Requests.Query(SELECT_KEYSPACES + whereClause))
                                        : null;
        DefaultResultSetFuture cfFuture = new DefaultResultSetFuture(null, new Requests.Query(SELECT_COLUMN_FAMILIES + whereClause));
        DefaultResultSetFuture colsFuture = new DefaultResultSetFuture(null, new Requests.Query(SELECT_COLUMNS + whereClause));

        if (ksFuture != null)
            connection.write(ksFuture);
        connection.write(cfFuture);
        connection.write(colsFuture);

        Host host = cluster.metadata.getHost(connection.address);
        // Neither host, nor it's version should be null. But instead of dying if there is a race or something, we can kind of try to infer
        // a Cassandra version from the protocol version (this is not full proof, we can have the protocol 1 against C* 2.0+, but it's worth
        // a shot, and since we log in this case, it should be relatively easy to debug when if this ever fail).
        VersionNumber cassandraVersion;
        if (host == null || host.getCassandraVersion() == null) {
            cassandraVersion = cluster.protocolVersion() == 1 ? VersionNumber.parse("1.2.0") : VersionNumber.parse("2.0.0");
            logger.warn("Cannot find Cassandra version for host {} to parse the schema, using {} based on protocol version in use. "
                      + "If parsing the schema fails, this could be the cause", connection.address, cassandraVersion);
        } else {
            cassandraVersion = host.getCassandraVersion();
        }
        cluster.metadata.rebuildSchema(keyspace, table, ksFuture == null ? null : ksFuture.get(), cfFuture.get(), colsFuture.get(), cassandraVersion);
        // If the table is null, we either rebuild all from scratch or have an updated keyspace. In both case, rebuild the token map
        // since some replication on some keyspace may have changed
        if (table == null)
            refreshNodeListAndTokenMap(connection, cluster);
    }

    public void refreshNodeListAndTokenMap() {
        Connection c = connectionRef.get();
        // At startup, when we add the initial nodes, this will be null, which is ok
        if (c == null)
            return;

        logger.debug("[Control connection] Refreshing node list and token map");
        try {
            refreshNodeListAndTokenMap(c, cluster);
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

    public void refreshNodeInfo(Host host) {

        Connection c = connectionRef.get();
        // At startup, when we add the initial nodes, this will be null, which is ok
        if (c == null)
            return;

        logger.debug("[Control connection] Refreshing node info on {}", host);
        try {
            DefaultResultSetFuture future = c.address.equals(host.getAddress())
                                          ? new DefaultResultSetFuture(null, new Requests.Query(SELECT_LOCAL))
                                          : new DefaultResultSetFuture(null, new Requests.Query(SELECT_PEERS + " WHERE peer='" + host.getAddress().getHostAddress() + '\''));
            c.write(future);
            ResultSet rs = future.get();
            // It's possible our peers selection returns nothing, but that's fine, this method is best effort really.
            if (rs.isExhausted()) {
                logger.debug("[control connection] Asked to refresh node info for {} but host not found in {} system table (this can happen)", host.getAddress(), c.address);
                return;
            }

            Row row = rs.one();
            updateLocationInfo(host, row.getString("data_center"), row.getString("rack"), cluster);
            host.setVersion(row.getString("release_version"));
        } catch (ConnectionException e) {
            logger.debug("[Control connection] Connection error while refeshing node info ({})", e.getMessage());
            signalError();
        } catch (ExecutionException e) {
            // If we're being shutdown during refresh, this can happen. That's fine so don't scare the user.
            if (!isShutdown)
                logger.debug("[Control connection] Unexpected error while refeshing node info", e);
            signalError();
        } catch (BusyConnectionException e) {
            logger.debug("[Control connection] Connection is busy, reconnecting");
            signalError();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.debug("[Control connection] Interrupted while refreshing node list and token map, skipping it.");
        }
    }

    private static void updateLocationInfo(Host host, String datacenter, String rack, Cluster.Manager cluster) {
        if (Objects.equal(host.getDatacenter(), datacenter) && Objects.equal(host.getRack(), rack))
            return;

        // If the dc/rack information changes, we need to update the load balancing policy.
        // For that, we remove and re-add the node against the policy. Not the most elegant, and assumes
        // that the policy will update correctly, but in practice this should work.
        cluster.loadBalancingPolicy().onDown(host);
        host.setLocationInfo(datacenter, rack);
        cluster.loadBalancingPolicy().onAdd(host);
    }

    private static void refreshNodeListAndTokenMap(Connection connection, Cluster.Manager cluster) throws ConnectionException, BusyConnectionException, ExecutionException, InterruptedException {
        // Make sure we're up to date on nodes and tokens

        DefaultResultSetFuture peersFuture = new DefaultResultSetFuture(null, new Requests.Query(SELECT_PEERS));
        DefaultResultSetFuture localFuture = new DefaultResultSetFuture(null, new Requests.Query(SELECT_LOCAL));
        connection.write(peersFuture);
        connection.write(localFuture);

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
                updateLocationInfo(host, localRow.getString("data_center"), localRow.getString("rack"), cluster);
                host.setVersion(localRow.getString("release_version"));

                Set<String> tokens = localRow.getSet("tokens", String.class);
                if (partitioner != null && !tokens.isEmpty())
                    tokenMap.put(host, tokens);
            }
        }

        List<InetAddress> foundHosts = new ArrayList<InetAddress>();
        List<String> dcs = new ArrayList<String>();
        List<String> racks = new ArrayList<String>();
        List<String> cassandraVersions = new ArrayList<String>();
        List<Set<String>> allTokens = new ArrayList<Set<String>>();

        for (Row row : peersFuture.get()) {

            InetAddress peer = row.getInet("peer");
            InetAddress addr = row.getInet("rpc_address");

            if (peer.equals(connection.address) || (addr != null && addr.equals(connection.address))) {
                // Some DSE versions were inserting a line for the local node in peers (with mostly null values). This has been fixed, but if we
                // detect that's the case, ignore it as it's not really a big deal.
                logger.debug("System.peers on node {} has a line for itself. This is not normal but is a known problem of some DSE version. Ignoring the entry.", connection.address);
                continue;
            } else if (addr == null) {
                logger.error("No rpc_address found for host {} in {}'s peers system table. That should not happen but using address {} instead", peer, connection.address, peer);
                addr = peer;
            } else if (addr.equals(bindAllAddress)) {
                logger.warn("Found host with 0.0.0.0 as rpc_address, using listen_address ({}) to contact it instead. If this is incorrect you should avoid the use of 0.0.0.0 server side.", peer);
                addr = peer;
            }

            foundHosts.add(addr);
            dcs.add(row.getString("data_center"));
            racks.add(row.getString("rack"));
            cassandraVersions.add(row.getString("release_version"));
            allTokens.add(row.getSet("tokens", String.class));
        }

        for (int i = 0; i < foundHosts.size(); i++) {
            Host host = cluster.metadata.getHost(foundHosts.get(i));
            boolean isNew = false;
            if (host == null) {
                // We don't know that node, create the Host object but wait until we've set the known
                // info before signaling the addition.
                host = cluster.metadata.add(foundHosts.get(i));
                isNew = true;
            }
            updateLocationInfo(host, dcs.get(i), racks.get(i), cluster);
            host.setVersion(cassandraVersions.get(i));

            if (partitioner != null && !allTokens.get(i).isEmpty())
                tokenMap.put(host, allTokens.get(i));

            if (isNew)
                cluster.onAdd(host);
        }

        // Removes all those that seems to have been removed (since we lost the control connection)
        Set<InetAddress> foundHostsSet = new HashSet<InetAddress>(foundHosts);
        for (Host host : cluster.metadata.allHosts())
            if (!host.getAddress().equals(connection.address) && !foundHostsSet.contains(host.getAddress()))
                cluster.removeHost(host);

        cluster.metadata.rebuildTokenMap(partitioner, tokenMap);
    }

    static boolean waitForSchemaAgreement(Connection connection, Metadata metadata) throws ConnectionException, BusyConnectionException, ExecutionException, InterruptedException {

        long start = System.nanoTime();
        long elapsed = 0;
        while (elapsed < MAX_SCHEMA_AGREEMENT_WAIT_MS) {

            DefaultResultSetFuture peersFuture = new DefaultResultSetFuture(null, new Requests.Query(SELECT_SCHEMA_PEERS));
            DefaultResultSetFuture localFuture = new DefaultResultSetFuture(null, new Requests.Query(SELECT_SCHEMA_LOCAL));
            connection.write(peersFuture);
            connection.write(localFuture);

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
            cluster.blockingTasksExecutor.submit(new Runnable() {
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
