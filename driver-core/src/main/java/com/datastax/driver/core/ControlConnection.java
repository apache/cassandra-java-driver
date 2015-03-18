/*
 *      Copyright (C) 2012-2014 DataStax Inc.
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
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Objects;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.DriverInternalError;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

class ControlConnection implements Host.StateListener {

    private static final Logger logger = LoggerFactory.getLogger(ControlConnection.class);

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
    private static final String SELECT_USERTYPES = "SELECT * FROM system.schema_usertypes";

    private static final String SELECT_PEERS = "SELECT * FROM system.peers";
    private static final String SELECT_LOCAL = "SELECT * FROM system.local WHERE key='local'";

    private static final String SELECT_SCHEMA_PEERS = "SELECT peer, rpc_address, schema_version FROM system.peers";
    private static final String SELECT_SCHEMA_LOCAL = "SELECT schema_version FROM system.local WHERE key='local'";

    private final AtomicReference<Connection> connectionRef = new AtomicReference<Connection>();

    private final Cluster.Manager cluster;

    private final AtomicReference<ListenableFuture<?>> reconnectionAttempt = new AtomicReference<ListenableFuture<?>>();

    private volatile boolean isShutdown;

    public ControlConnection(Cluster.Manager manager) {
        this.cluster = manager;
    }

    // Only for the initial connection. Does not schedule retries if it fails
    public void connect() throws UnsupportedProtocolVersionException {
        if (isShutdown)
            return;

        // NB: at this stage, allHosts() only contains the initial contact points
        setNewConnection(reconnectInternal(cluster.metadata.allHosts().iterator(), true));
    }

    public CloseFuture closeAsync() {
        // We don't have to be fancy here. We just set a flag so that we stop trying to reconnect (and thus change the
        // connection used) and shutdown the current one.
        isShutdown = true;

        // Cancel any reconnection attempt in progress
        ListenableFuture<?> r = reconnectionAttempt.get();
        if (r != null)
            r.cancel(false);

        Connection connection = connectionRef.get();
        return connection == null ? CloseFuture.immediateFuture() : connection.closeAsync();
    }

    Host connectedHost() {
        Connection current = connectionRef.get();
        return cluster.metadata.getHost(current.address);
    }

    void reconnect() {
        if (isShutdown)
            return;

        try {
            setNewConnection(reconnectInternal(queryPlan(), false));
        } catch (NoHostAvailableException e) {
            logger.error("[Control connection] Cannot connect to any host, scheduling retry");
            backgroundReconnect(-1);
        } catch (UnsupportedProtocolVersionException e) {
            // reconnectInternal only propagate those if we've not decided on the protocol version yet,
            // which should only happen on the initial connection and thus in connect() but never here.
            throw new AssertionError();
        } catch (Exception e) {
            logger.error("[Control connection] Unknown error during reconnection, scheduling retry", e);
            backgroundReconnect(-1);
        }
    }

    /**
     * @param initialDelayMs if >=0, bypass the schedule and use this for the first call
     */
    private void backgroundReconnect(long initialDelayMs) {
        if (isShutdown)
            return;

        new AbstractReconnectionHandler(cluster.reconnectionExecutor, cluster.reconnectionPolicy().newSchedule(), reconnectionAttempt, initialDelayMs) {
            @Override
            protected Connection tryReconnect() throws ConnectionException {
                try {
                    return reconnectInternal(queryPlan(), false);
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
    }

    private Iterator<Host> queryPlan() {
        return cluster.loadBalancingPolicy().newQueryPlan(null, Statement.DEFAULT);
    }

    private void signalError() {
        Connection connection = connectionRef.get();
        if (connection != null && connection.isDefunct() && cluster.metadata.getHost(connection.address) != null) {
            // If the connection was marked as defunct and the host hadn't left, this already reported the
            // host down, which will trigger a reconnect.
            return;
        }
        // If the connection is not defunct, or the host has left, just reconnect manually
        backgroundReconnect(0);
    }

    private void setNewConnection(Connection newConnection) {
        logger.debug("[Control connection] Successfully connected to {}", newConnection.address);
        Connection old = connectionRef.getAndSet(newConnection);
        if (old != null && !old.isClosed())
            old.closeAsync();
    }

    private Connection reconnectInternal(Iterator<Host> iter, boolean isInitialConnection) throws UnsupportedProtocolVersionException {

        Map<InetSocketAddress, Throwable> errors = null;

        Host host = null;
        try {
            while (iter.hasNext()) {
                host = iter.next();
                try {
                    return tryConnect(host, isInitialConnection);
                } catch (ConnectionException e) {
                    errors = logError(host, e, errors, iter);
                    if (isInitialConnection) {
                        // Mark the host down right away so that we don't try it again during the initialization process.
                        // We don't call cluster.triggerOnDown because it does a bunch of other things we don't want to do here (notify LBP, etc.)
                        host.setDown();
                        cluster.startPeriodicReconnectionAttempt(host, true);
                    }
                } catch (ExecutionException e) {
                    errors = logError(host, e.getCause(), errors, iter);
                } catch (UnsupportedProtocolVersionException e) {
                    // If it's the very first node we've connected to, rethrow the exception and
                    // Cluster.init() will handle it. Otherwise, just mark this node in error.
                    if (cluster.protocolVersion() == null)
                        throw e;
                    logger.debug("Ignoring host {}: {}", host, e.getMessage());
                    errors = logError(host, e, errors, iter);
                } catch (ClusterNameMismatchException e) {
                    logger.debug("Ignoring host {}: {}", host, e.getMessage());
                    errors = logError(host, e, errors, iter);
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
        throw new NoHostAvailableException(errors == null ? Collections.<InetSocketAddress, Throwable>emptyMap() : errors);
    }

    private static Map<InetSocketAddress, Throwable> logError(Host host, Throwable exception, Map<InetSocketAddress, Throwable> errors, Iterator<Host> iter) {
        if (errors == null)
            errors = new HashMap<InetSocketAddress, Throwable>();

        errors.put(host.getSocketAddress(), exception);

        if (logger.isDebugEnabled()) {
            if (iter.hasNext()) {
                logger.debug(String.format("[Control connection] error on %s connection, trying next host", host), exception);
            } else {
                logger.debug(String.format("[Control connection] error on %s connection, no more host to try", host), exception);
            }
        }
        return errors;
    }

    private Connection tryConnect(Host host, boolean isInitialConnection) throws ConnectionException, ExecutionException, InterruptedException, UnsupportedProtocolVersionException, ClusterNameMismatchException
    {
        Connection connection = cluster.connectionFactory.open(host);

        // If no protocol version was specified, set the default as soon as a connection succeeds (it's needed to parse UDTs in refreshSchema)
        if (cluster.connectionFactory.protocolVersion == null)
            cluster.connectionFactory.protocolVersion = ProtocolVersion.NEWEST_SUPPORTED;

        try {
            logger.trace("[Control connection] Registering for events");
            List<ProtocolEvent.Type> evs = Arrays.asList(
                ProtocolEvent.Type.TOPOLOGY_CHANGE,
                ProtocolEvent.Type.STATUS_CHANGE,
                ProtocolEvent.Type.SCHEMA_CHANGE
            );
            connection.write(new Requests.Register(evs));

            // We need to refresh the node list first so we know about the cassandra version of
            // the node we're connecting to.
            refreshNodeListAndTokenMap(connection, cluster, isInitialConnection, true);

            // Note that refreshing the schema will trigger refreshNodeListAndTokenMap since table == null
            // We want that because the token map was not properly initialized by the first call above, since it requires the list of keyspaces
            // to be loaded.
            logger.debug("[Control connection] Refreshing schema");
            refreshSchema(connection, null, null, null, cluster, isInitialConnection);
            return connection;
        } catch (BusyConnectionException e) {
            connection.closeAsync().force();
            throw new DriverInternalError("Newly created connection should not be busy");
        } catch (InterruptedException e) {
            connection.closeAsync().force();
            throw e;
        } catch (ConnectionException e) {
            connection.closeAsync().force();
            throw e;
        } catch (ExecutionException e) {
            connection.closeAsync().force();
            throw e;
        } catch (RuntimeException e) {
            connection.closeAsync().force();
            throw e;
        }
    }

    public void refreshSchema(String keyspace, String table, String udt) throws InterruptedException {
        logger.debug("[Control connection] Refreshing schema for {}{}",
            keyspace == null ? "everything" : keyspace,
            table == null ? (udt == null ? "" : '.' + udt + " (UDT)") : '.' + table + " (table)");
        try {
            Connection c = connectionRef.get();
            // At startup, when we add the initial nodes, this will be null, which is ok
            if (c == null)
                return;
            refreshSchema(c, keyspace, table, udt, cluster, false);
        } catch (ConnectionException e) {
            logger.debug("[Control connection] Connection error while refreshing schema ({})", e.getMessage());
            signalError();
        } catch (ExecutionException e) {
            // If we're being shutdown during schema refresh, this can happen. That's fine so don't scare the user.
            if (!isShutdown)
                logger.error("[Control connection] Unexpected error while refreshing schema", e);
            signalError();
        } catch (BusyConnectionException e) {
            logger.debug("[Control connection] Connection is busy, reconnecting");
            signalError();
        }
    }

    static void refreshSchema(Connection connection, String keyspace, String table, String udt, Cluster.Manager cluster, boolean isInitialConnection) throws ConnectionException, BusyConnectionException, ExecutionException, InterruptedException {
        Host host = cluster.metadata.getHost(connection.address);
        // Neither host, nor it's version should be null. But instead of dying if there is a race or something, we can kind of try to infer
        // a Cassandra version from the protocol version (this is not full proof, we can have the protocol 1 against C* 2.0+, but it's worth
        // a shot, and since we log in this case, it should be relatively easy to debug when if this ever fail).
        VersionNumber cassandraVersion;
        if (host == null || host.getCassandraVersion() == null) {
            cassandraVersion = cluster.protocolVersion().minCassandraVersion();
            logger.warn("Cannot find Cassandra version for host {} to parse the schema, using {} based on protocol version in use. "
                      + "If parsing the schema fails, this could be the cause", connection.address, cassandraVersion);
        } else {
            cassandraVersion = host.getCassandraVersion();
        }

        // Make sure we're up to date on schema
        String whereClause = "";
        if (keyspace != null) {
            whereClause = " WHERE keyspace_name = '" + keyspace + '\'';
            if (table != null)
                whereClause += " AND columnfamily_name = '" + table + '\'';
            else if (udt != null)
                whereClause += " AND type_name = '" + udt + '\'';
        }

        DefaultResultSetFuture ksFuture = (table == null && udt == null)
                                        ? new DefaultResultSetFuture(null, cluster.protocolVersion(), new Requests.Query(SELECT_KEYSPACES + whereClause))
                                        : null;
        DefaultResultSetFuture udtFuture = table == null && (cassandraVersion.getMajor() > 2 || (cassandraVersion.getMajor() == 2 && cassandraVersion.getMinor() >= 1))
                                         ? new DefaultResultSetFuture(null, cluster.protocolVersion(), new Requests.Query(SELECT_USERTYPES + whereClause))
                                         : null;
        DefaultResultSetFuture cfFuture = udt == null
                                        ? new DefaultResultSetFuture(null, cluster.protocolVersion(), new Requests.Query(SELECT_COLUMN_FAMILIES + whereClause))
                                        : null;
        DefaultResultSetFuture colsFuture = udt == null
                                          ? new DefaultResultSetFuture(null, cluster.protocolVersion(), new Requests.Query(SELECT_COLUMNS + whereClause))
                                          : null;

        if (ksFuture != null)
            connection.write(ksFuture);
        if (udtFuture != null)
            connection.write(udtFuture);
        if (cfFuture != null)
            connection.write(cfFuture);
        if (colsFuture != null)
            connection.write(colsFuture);

        try {
            cluster.metadata.rebuildSchema(keyspace, table, udt,
                                           ksFuture == null ? null : ksFuture.get(),
                                           udtFuture == null ? null : udtFuture.get(),
                                           cfFuture == null ? null : cfFuture.get(),
                                           colsFuture == null ? null : colsFuture.get(),
                                           cassandraVersion);
        } catch (RuntimeException e) {
            // Failure to parse the schema is definitively wrong so log a full-on error, but this won't generally prevent queries to
            // work and this can happen when new Cassandra versions modify stuff in the schema and the driver hasn't yet be modified.
            // So log, but let things go otherwise.
            logger.error("Error parsing schema from Cassandra system tables: the schema in Cluster#getMetadata() will appear incomplete or stale", e);
        }

        // If the table is null, we either rebuild all from scratch or have an updated keyspace. In both case, rebuild the token map
        // since some replication on some keyspace may have changed
        if (table == null)
            refreshNodeListAndTokenMap(connection, cluster, false, false);
    }

    public void refreshNodeListAndTokenMap() {
        Connection c = connectionRef.get();
        // At startup, when we add the initial nodes, this will be null, which is ok
        if (c == null)
            return;

        logger.debug("[Control connection] Refreshing node list and token map");
        try {
            refreshNodeListAndTokenMap(c, cluster, false, true);
        } catch (ConnectionException e) {
            logger.debug("[Control connection] Connection error while refreshing node list and token map ({})", e.getMessage());
            signalError();
        } catch (ExecutionException e) {
            // If we're being shutdown during refresh, this can happen. That's fine so don't scare the user.
            if (!isShutdown)
                logger.error("[Control connection] Unexpected error while refreshing node list and token map", e);
            signalError();
        } catch (BusyConnectionException e) {
            logger.debug("[Control connection] Connection is busy, reconnecting");
            signalError();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.debug("[Control connection] Interrupted while refreshing node list and token map, skipping it.");
        }
    }

    private static InetSocketAddress addressToUseForPeerHost(Row peersRow, InetSocketAddress connectedHost, Cluster.Manager cluster, boolean logMissingRpcAddresses) {
        InetAddress peer = peersRow.getInet("peer");
        InetAddress addr = peersRow.getInet("rpc_address");

        if (peer.equals(connectedHost.getAddress()) || (addr != null && addr.equals(connectedHost.getAddress()))) {
            // Some DSE versions were inserting a line for the local node in peers (with mostly null values). This has been fixed, but if we
            // detect that's the case, ignore it as it's not really a big deal.
            logger.debug("System.peers on node {} has a line for itself. This is not normal but is a known problem of some DSE version. Ignoring the entry.", connectedHost);
            return null;
        } else if (addr == null) {
            if (logMissingRpcAddresses)
                logger.warn("No rpc_address found for host {} in {}'s peers system table. {} will be ignored.", peer, connectedHost, peer);
            return null;
        } else if (addr.equals(bindAllAddress)) {
            logger.warn("Found host with 0.0.0.0 as rpc_address, using listen_address ({}) to contact it instead. If this is incorrect you should avoid the use of 0.0.0.0 server side.", peer);
            addr = peer;
        }
        return cluster.translateAddress(addr);
    }

    private Row fetchNodeInfo(Host host, Connection c) throws ConnectionException, BusyConnectionException, ExecutionException, InterruptedException {
        boolean isConnectedHost = c.address.equals(host.getSocketAddress());
        if (isConnectedHost || host.listenAddress != null) {
            DefaultResultSetFuture future = isConnectedHost
                ? new DefaultResultSetFuture(null, cluster.protocolVersion(), new Requests.Query(SELECT_LOCAL))
                : new DefaultResultSetFuture(null, cluster.protocolVersion(), new Requests.Query(SELECT_PEERS + " WHERE peer='" + host.listenAddress.getHostAddress() + '\''));
            c.write(future);
            return future.get().one();
        }

        // We have to fetch the whole peers table and find the host we're looking for
        DefaultResultSetFuture future = new DefaultResultSetFuture(null, cluster.protocolVersion(), new Requests.Query(SELECT_PEERS));
        c.write(future);
        for (Row row : future.get()) {
            InetSocketAddress addr = addressToUseForPeerHost(row, c.address, cluster, true);
            if (addr != null && addr.equals(host.getSocketAddress()))
                return row;
        }
        return null;
    }

    /**
     * @return whether we have enough information to bring the node back up
     */
    public boolean refreshNodeInfo(Host host) {

        Connection c = connectionRef.get();
        // At startup, when we add the initial nodes, this will be null, which is ok
        if (c == null)
            return true;

        logger.debug("[Control connection] Refreshing node info on {}", host);
        try {
            Row row = fetchNodeInfo(host, c);
            if (row == null) {
                if (c.isDefunct()) {
                    logger.debug("Control connection is down, could not refresh node info");
                    // Keep going with what we currently know about the node, otherwise we will ignore all nodes
                    // until the control connection is back up (which leads to a catch-22 if there is only one)
                    return true;
                } else {
                    logger.warn("No row found for host {} in {}'s peers system table. {} will be ignored.", host.getAddress(), c.address, host.getAddress());
                    return false;
                }
                // Ignore hosts with a null rpc_address, as this is most likely a phantom row in system.peers (JAVA-428).
                // Don't test this for the control host since we're already connected to it anyway, and we read the info from system.local
                // which doesn't have an rpc_address column (JAVA-546).
            } else if (!c.address.equals(host.getSocketAddress()) && row.getInet("rpc_address") == null) {
                logger.warn("No rpc_address found for host {} in {}'s peers system table. {} will be ignored.", host.getAddress(), c.address, host.getAddress());
                return false;
            }

            updateInfo(host, row, cluster, false);
            return true;

        } catch (ConnectionException e) {
            logger.debug("[Control connection] Connection error while refreshing node info ({})", e.getMessage());
            signalError();
        } catch (ExecutionException e) {
            // If we're being shutdown during refresh, this can happen. That's fine so don't scare the user.
            if (!isShutdown)
                logger.debug("[Control connection] Unexpected error while refreshing node info", e);
            signalError();
        } catch (BusyConnectionException e) {
            logger.debug("[Control connection] Connection is busy, reconnecting");
            signalError();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.debug("[Control connection] Interrupted while refreshing node info, skipping it.");
        } catch (Exception e) {
            logger.debug("[Control connection] Unexpected error while refreshing node info", e);
            signalError();
        }
        // If we got an exception, always return true. Otherwise a faulty control connection would cause
        // reconnected hosts to be ignored permanently.
        return true;
    }

    // row can come either from the 'local' table or the 'peers' one
    private static void updateInfo(Host host, Row row, Cluster.Manager cluster, boolean isInitialConnection) {
        if (!row.isNull("data_center") || !row.isNull("rack"))
            updateLocationInfo(host, row.getString("data_center"), row.getString("rack"), isInitialConnection, cluster);

        String version = row.getString("release_version");
        // We don't know if it's a 'local' or a 'peers' row, and only 'peers' rows have the 'peer' field.
        InetAddress listenAddress = row.getColumnDefinitions().contains("peer")
                                  ? row.getInet("peer")
                                  : null;

        host.setVersionAndListenAdress(version, listenAddress);
    }

    private static void updateLocationInfo(Host host, String datacenter, String rack, boolean isInitialConnection, Cluster.Manager cluster) {
        if (Objects.equal(host.getDatacenter(), datacenter) && Objects.equal(host.getRack(), rack))
            return;

        // If the dc/rack information changes for an existing node, we need to update the load balancing policy.
        // For that, we remove and re-add the node against the policy. Not the most elegant, and assumes
        // that the policy will update correctly, but in practice this should work.
        if (!isInitialConnection)
            cluster.loadBalancingPolicy().onDown(host);
        host.setLocationInfo(datacenter, rack);
        if (!isInitialConnection)
            cluster.loadBalancingPolicy().onAdd(host);
    }

    private static void refreshNodeListAndTokenMap(Connection connection, Cluster.Manager cluster, boolean isInitialConnection, boolean logMissingRpcAddresses) throws ConnectionException, BusyConnectionException, ExecutionException, InterruptedException {
        logger.debug("[Control connection] Refreshing node list and token map");

        // Make sure we're up to date on nodes and tokens

        DefaultResultSetFuture localFuture = new DefaultResultSetFuture(null, cluster.protocolVersion(), new Requests.Query(SELECT_LOCAL));
        DefaultResultSetFuture peersFuture = new DefaultResultSetFuture(null, cluster.protocolVersion(), new Requests.Query(SELECT_PEERS));
        connection.write(localFuture);
        connection.write(peersFuture);

        String partitioner = null;
        Map<Host, Collection<String>> tokenMap = new HashMap<Host, Collection<String>>();

        // Update cluster name, DC and rack for the one node we are connected to
        Row localRow = localFuture.get().one();
        if (localRow != null) {
            String clusterName = localRow.getString("cluster_name");
            if (clusterName != null)
                cluster.metadata.clusterName = clusterName;

            partitioner = localRow.getString("partitioner");
            if (partitioner != null)
                cluster.metadata.partitioner = partitioner;

            Host host = cluster.metadata.getHost(connection.address);
            // In theory host can't be null. However there is no point in risking a NPE in case we
            // have a race between a node removal and this.
            if (host == null) {
                logger.debug("Host in local system table ({}) unknown to us (ok if said host just got removed)", connection.address);
            } else {
                updateInfo(host, localRow, cluster, isInitialConnection);
                Set<String> tokens = localRow.getSet("tokens", String.class);
                if (partitioner != null && !tokens.isEmpty())
                    tokenMap.put(host, tokens);
            }
        }

        List<InetSocketAddress> foundHosts = new ArrayList<InetSocketAddress>();
        List<String> dcs = new ArrayList<String>();
        List<String> racks = new ArrayList<String>();
        List<String> cassandraVersions = new ArrayList<String>();
        List<InetAddress> listenAddresses = new ArrayList<InetAddress>();
        List<Set<String>> allTokens = new ArrayList<Set<String>>();

        for (Row row : peersFuture.get()) {
            InetSocketAddress addr = addressToUseForPeerHost(row, connection.address, cluster, logMissingRpcAddresses);
            if (addr == null)
                continue;

            foundHosts.add(addr);
            dcs.add(row.getString("data_center"));
            racks.add(row.getString("rack"));
            cassandraVersions.add(row.getString("release_version"));
            listenAddresses.add(row.getInet("peer"));
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
            if (dcs.get(i) != null || racks.get(i) != null)
                updateLocationInfo(host, dcs.get(i), racks.get(i), isInitialConnection, cluster);
            if (cassandraVersions.get(i) != null)
                host.setVersionAndListenAdress(cassandraVersions.get(i), listenAddresses.get(i));

            if (partitioner != null && !allTokens.get(i).isEmpty())
                tokenMap.put(host, allTokens.get(i));

            if (isNew && !isInitialConnection)
                cluster.triggerOnAdd(host);
        }

        // Removes all those that seems to have been removed (since we lost the control connection)
        Set<InetSocketAddress> foundHostsSet = new HashSet<InetSocketAddress>(foundHosts);
        for (Host host : cluster.metadata.allHosts())
            if (!host.getSocketAddress().equals(connection.address) && !foundHostsSet.contains(host.getSocketAddress()))
                cluster.removeHost(host, isInitialConnection);

        cluster.metadata.rebuildTokenMap(partitioner, tokenMap);
    }

    static boolean waitForSchemaAgreement(Connection connection, Cluster.Manager cluster) throws ConnectionException, BusyConnectionException, ExecutionException, InterruptedException {

        long start = System.nanoTime();
        long elapsed = 0;
        int maxSchemaAgreementWaitSeconds = cluster.configuration.getProtocolOptions().getMaxSchemaAgreementWaitSeconds();
        while (elapsed < maxSchemaAgreementWaitSeconds * 1000) {

            DefaultResultSetFuture peersFuture = new DefaultResultSetFuture(null, cluster.protocolVersion(), new Requests.Query(SELECT_SCHEMA_PEERS));
            DefaultResultSetFuture localFuture = new DefaultResultSetFuture(null, cluster.protocolVersion(), new Requests.Query(SELECT_SCHEMA_LOCAL));
            connection.write(peersFuture);
            connection.write(localFuture);

            Set<UUID> versions = new HashSet<UUID>();

            Row localRow = localFuture.get().one();
            if (localRow != null && !localRow.isNull("schema_version"))
                versions.add(localRow.getUUID("schema_version"));

            for (Row row : peersFuture.get()) {

                InetSocketAddress addr = addressToUseForPeerHost(row, connection.address, cluster, true);
                if (addr == null || row.isNull("schema_version"))
                    continue;

                Host peer = cluster.metadata.getHost(addr);
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
        if (logger.isDebugEnabled())
            logger.debug("[Control connection] {} is down, currently connected to {}", host, current == null ? "nobody" : current.address);

        if (current != null && current.address.equals(host.getSocketAddress())) {
            // This starts an AbstractReconnectionHandler, which will take care of checking if another reconnection is
            // already in progress
            backgroundReconnect(0);
        }
    }

    @Override
    public void onSuspected(Host host) {
    }

    @Override
    public void onAdd(Host host) {
        // Refresh infos and token map if we didn't knew about that host, i.e. if we either don't have basic infos on it,
        // or it's not part of our computed token map
        Metadata.TokenMap tkmap = cluster.metadata.tokenMap;
        if (host.getCassandraVersion() == null || tkmap == null || !tkmap.hosts.contains(host))
            refreshNodeListAndTokenMap();
    }

    @Override
    public void onRemove(Host host) {
        Connection current = connectionRef.get();
        if (logger.isDebugEnabled())
            logger.debug("[Control connection] {} has been removed, currently connected to {}", host, current == null ? "nobody" : current.address);

        // Schedule a reconnection if that was our control host
        if (current != null && current.address.equals(host.getSocketAddress())) {
            backgroundReconnect(0);
        }

        refreshNodeListAndTokenMap();
    }
}
