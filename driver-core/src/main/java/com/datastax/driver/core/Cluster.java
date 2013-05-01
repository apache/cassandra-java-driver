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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.cassandra.utils.MD5Digest;
import org.apache.cassandra.transport.Event;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.messages.EventMessage;
import org.apache.cassandra.transport.messages.PrepareMessage;

import com.datastax.driver.core.exceptions.*;
import com.datastax.driver.core.policies.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * information and known state of a Cassandra cluster.
 * <p>
 * This is the main entry point of the driver. A simple example of access to a
 * Cassandra cluster would be:
 * <pre>
 *   Cluster cluster = Cluster.builder().addContactPoint("192.168.0.1").build();
 *   Session session = cluster.connect("db1");
 *
 *   for (Row row : session.execute("SELECT * FROM table1"))
 *       // do something ...
 * </pre>
 * <p>
 * A cluster object maintains a permanent connection to one of the cluster nodes
 * which it uses solely to maintain information on the state and current
 * topology of the cluster. Using the connection, the driver will discover all
 * the nodes currently in the cluster as well as new nodes joining the cluster
 * subsequently.
 */
public class Cluster {

    private static final Logger logger = LoggerFactory.getLogger(Cluster.class);

    final Manager manager;

    private Cluster(List<InetAddress> contactPoints, Configuration configuration) {
        this.manager = new Manager(contactPoints, configuration);
        this.manager.init();
    }

    /**
     * Build a new cluster based on the provided initializer.
     * <p>
     * Note that for building a cluster programmatically, Cluster.Builder
     * provides a slightly less verbose shortcut with {@link Builder#build}.
     * <p>
     * Also note that that all the contact points provided by {@code
     * initializer} must share the same port.
     *
     * @param initializer the Cluster.Initializer to use
     * @return the newly created Cluster instance
     *
     * @throws NoHostAvailableException if no host amongst the contact points
     * can be reached.
     * @throws IllegalArgumentException if the list of contact points provided
     * by {@code initializer} is empty or if not all those contact points have the same port.
     * @throws AuthenticationException if an authentication error occurs
     * while contacting the initial contact points.
     */
    public static Cluster buildFrom(Initializer initializer) {
        List<InetAddress> contactPoints = initializer.getContactPoints();
        if (contactPoints.isEmpty())
            throw new IllegalArgumentException("Cannot build a cluster without contact points");

        return new Cluster(contactPoints, initializer.getConfiguration());
    }

    /**
     * Creates a new {@link Cluster.Builder} instance.
     * <p>
     * This is a convenenience method for {@code new Cluster.Builder()}.
     *
     * @return the new cluster builder.
     */
    public static Cluster.Builder builder() {
        return new Cluster.Builder();
    }

    /**
     * Creates a new session on this cluster.
     *
     * @return a new session on this cluster sets to no keyspace.
     */
    public Session connect() {
        return manager.newSession();
    }

    /**
     * Creates a new session on this cluster and sets the keyspace to the provided one.
     *
     * @param keyspace The name of the keyspace to use for the created
     * {@code Session}.
     * @return a new session on this cluster sets to keyspace
     * {@code keyspaceName}.
     *
     * @throws NoHostAvailableException if no host can be contacted to set the
     * {@code keyspace}.
     */
    public Session connect(String keyspace) {
        Session session = connect();
        session.manager.setKeyspace(keyspace);
        return session;
    }

    /**
     * Returns read-only metadata on the connected cluster.
     * <p>
     * This includes the known nodes with their status as seen by the driver,
     * as well as the schema definitions.
     *
     * @return the cluster metadata.
     */
    public Metadata getMetadata() {
        return manager.metadata;
    }

    /**
     * The cluster configuration.
     *
     * @return the cluster configuration.
     */
    public Configuration getConfiguration() {
        return manager.configuration;
    }

    /**
     * The cluster metrics.
     *
     * @return the cluster metrics, or {@code null} if metrics collection has
     * been disabled (see {@link Configuration#isMetricsEnabled}).
     */
    public Metrics getMetrics() {
        return manager.configuration.isMetricsEnabled()
             ? manager.metrics
             : null;
    }

    /**
     * Shuts down this cluster instance.
     *
     * This closes all connections from all the sessions of this {@code
     * Cluster} instance and reclaims all resources used by it.
     * <p>
     * This method waits indefinitely for the driver to shut down.
     * <p>
     * This method has no effect if the cluster was already shut down.
     */
    public void shutdown() {
        shutdown(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    /**
     * Shutdown this cluster instance, only waiting a definite amount of time.
     *
     * This closes all connections from all the sessions of this {@code
     * Cluster} instance and reclaim all resources used by it.
     * <p>
     * Note that this method is not thread safe in the sense that if another
     * shutdown is perform in parallel, it might return {@code true} even if
     * the instance is not yet fully shutdown.
     *
     * @param timeout how long to wait for the cluster instance to shutdown.
     * @param unit the unit for the timeout.
     * @return {@code true} if the instance has been properly shutdown within
     * the {@code timeout}, {@code false} otherwise.
     */
    public boolean shutdown(long timeout, TimeUnit unit) {
        try {
            return manager.shutdown(timeout, unit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * Initializer for {@link Cluster} instances.
     * <p>
     * If you want to create a new {@code Cluster} instance programmatically,
     * then it is advised to use {@link Cluster.Builder} which can be obtained from the
     * {@link Cluster#builder} method.
     * <p>
     * But it is also possible to implement a custom {@code Initializer} that
     * retrieves initialization from a web-service or from a configuration file.
     */
    public interface Initializer {

        /**
         * Returns the initial Cassandra hosts to connect to.
         *
         * @return the initial Cassandra contact points. See {@link Builder#addContactPoint}
         * for more details on contact points.
         */
        public List<InetAddress> getContactPoints();

        /**
         * The configuration to use for the new cluster.
         * <p>
         * Note that some configuration can be modified after the cluster
         * initialization but some others cannot. In particular, the ones that
         * cannot be changed afterwards includes:
         * <ul>
         *   <li>the port use to connect to Cassandra nodes (see {@link ProtocolOptions}).</li>
         *   <li>the policies used (see {@link Policies}).</li>
         *   <li>the authentication info provided (see {@link Configuration}).</li>
         *   <li>whether metrics are enabled (see {@link Configuration}).</li>
         * </ul>
         *
         * @return the configuration to use for the new cluster.
         */
        public Configuration getConfiguration();
    }

    /**
     * Helper class to build {@link Cluster} instances.
     */
    public static class Builder implements Initializer {

        private final List<InetAddress> addresses = new ArrayList<InetAddress>();
        private int port = ProtocolOptions.DEFAULT_PORT;
        private AuthInfoProvider authProvider = AuthInfoProvider.NONE;

        private LoadBalancingPolicy loadBalancingPolicy;
        private ReconnectionPolicy reconnectionPolicy;
        private RetryPolicy retryPolicy;

        private ProtocolOptions.Compression compression = ProtocolOptions.Compression.NONE;
        private boolean metricsEnabled = true;
        private final PoolingOptions poolingOptions = new PoolingOptions();
        private final SocketOptions socketOptions = new SocketOptions();

        public List<InetAddress> getContactPoints() {
            return addresses;
        }

        /**
         * The port to use to connect to the Cassandra host.
         *
         * If not set through this method, the default port (9042) will be used
         * instead.
         *
         * @param port the port to set.
         * @return this Builder
         */
        public Builder withPort(int port) {
            this.port = port;
            return this;
        }

        /**
         * Adds a contact point.
         *
         * Contact points are addresses of Cassandra nodes that the driver uses
         * to discover the cluster topology. Only one contact point is required
         * (the driver will retrieve the address of the other nodes
         * automatically), but it is usually a good idea to provide more than
         * one contact point, because if that single contact point is unavailable,
         * the driver cannot initialize itself correctly.
         *
         * @param address the address of the node to connect to
         * @return this Builder
         *
         * @throws IllegalArgumentException if no IP address for {@code address}
         * could be found
         * @throws SecurityException if a security manager is present and
         * permission to resolve the host name is denied.
         */
        public Builder addContactPoint(String address) {
            try {
                this.addresses.add(InetAddress.getByName(address));
                return this;
            } catch (UnknownHostException e) {
                throw new IllegalArgumentException(e.getMessage());
            }
        }

        /**
         * Adds contact points.
         *
         * See {@link Builder#addContactPoint} for more details on contact
         * points.
         *
         * @param addresses addresses of the nodes to add as contact point
         * @return this Builder
         *
         * @throws IllegalArgumentException if no IP address for at least one
         * of {@code addresses} could be found
         * @throws SecurityException if a security manager is present and
         * permission to resolve the host name is denied.
         *
         * @see Builder#addContactPoint
         */
        public Builder addContactPoints(String... addresses) {
            for (String address : addresses)
                addContactPoint(address);
            return this;
        }

        /**
         * Adds contact points.
         *
         * See {@link Builder#addContactPoint} for more details on contact
         * points.
         *
         * @param addresses addresses of the nodes to add as contact point
         * @return this Builder
         *
         * @see Builder#addContactPoint
         */
        public Builder addContactPoints(InetAddress... addresses) {
            for (InetAddress address : addresses)
                this.addresses.add(address);
            return this;
        }

        /**
         * Configures the load balancing policy to use for the new cluster.
         * <p>
         * If no load balancing policy is set through this method,
         * {@link Policies#DEFAULT_LOAD_BALANCING_POLICY} will be used instead.
         *
         * @param policy the load balancing policy to use
         * @return this Builder
         */
        public Builder withLoadBalancingPolicy(LoadBalancingPolicy policy) {
            this.loadBalancingPolicy = policy;
            return this;
        }

        /**
         * Configures the reconnection policy to use for the new cluster.
         * <p>
         * If no reconnection policy is set through this method,
         * {@link Policies#DEFAULT_RECONNECTION_POLICY} will be used instead.
         *
         * @param policy the reconnection policy to use
         * @return this Builder
         */
        public Builder withReconnectionPolicy(ReconnectionPolicy policy) {
            this.reconnectionPolicy = policy;
            return this;
        }

        /**
         * Configures the retry policy to use for the new cluster.
         * <p>
         * If no retry policy is set through this method,
         * {@link Policies#DEFAULT_RETRY_POLICY} will be used instead.
         *
         * @param policy the retry policy to use
         * @return this Builder
         */
        public Builder withRetryPolicy(RetryPolicy policy) {
            this.retryPolicy = policy;
            return this;
        }

        /**
         * Uses the provided {@code AuthInfoProvider} to connect to Cassandra hosts.
         * <p>
         * This is optional if the Cassandra cluster has been configured to not
         * require authentication (the default).
         *
         * @param authInfoProvider the authentication info provider to use
         * @return this Builder
         */
        public Builder withAuthInfoProvider(AuthInfoProvider authInfoProvider) {
            this.authProvider = authInfoProvider;
            return this;
        }

        /**
         * Sets the compression to use for the transport.
         *
         * @param compression the compression to set
         * @return this Builder
         *
         * @see ProtocolOptions.Compression
         */
        public Builder withCompression(ProtocolOptions.Compression compression) {
            this.compression = compression;
            return this;
        }

        /**
         * Disables metrics collection for the created cluster (metrics are
         * enabled by default otherwise).
         *
         * @return this builder
         */
        public Builder withoutMetrics() {
            this.metricsEnabled = false;
            return this;
        }

        /**
         * Return the pooling options used by this builder.
         *
         * @return the pooling options that will be used by this builder. You
         * can use the returned object to define the initial pooling options
         * for the built cluster.
         */
        public PoolingOptions poolingOptions() {
            return poolingOptions;
        }

        /**
         * Returns the socket options used by this builder.
         *
         * @return the socket options that will be used by this builder. You
         * can use the returned object to define the initial socket options
         * for the built cluster.
         */
        public SocketOptions socketOptions() {
            return socketOptions;
        }

        /**
         * The configuration that will be used for the new cluster.
         * <p>
         * You <b>should not</b> modify this object directly because changes made
         * to the returned object may not be used by the cluster build.
         * Instead, you should use the other methods of this {@code Builder}.
         *
         * @return the configuration to use for the new cluster.
         */
        public Configuration getConfiguration() {
            Policies policies = new Policies(
                loadBalancingPolicy == null ? Policies.DEFAULT_LOAD_BALANCING_POLICY : loadBalancingPolicy,
                reconnectionPolicy == null ? Policies.DEFAULT_RECONNECTION_POLICY : reconnectionPolicy,
                retryPolicy == null ? Policies.DEFAULT_RETRY_POLICY : retryPolicy
            );
            return new Configuration(policies,
                                     new ProtocolOptions(port).setCompression(compression),
                                     poolingOptions,
                                     socketOptions,
                                     authProvider,
                                     metricsEnabled);
        }

        /**
         * Builds the cluster with the configured set of initial contact points
         * and policies.
         *
         * This is a convenience method for {@code Cluster.buildFrom(this)}.
         *
         * @return the newly built Cluster instance.
         *
         * @throws NoHostAvailableException if none of the contact points
         * provided can be reached.
         * @throws AuthenticationException if an authentication error occurs.
         * while contacting the initial contact points
         */
        public Cluster build() {
            return Cluster.buildFrom(this);
        }
    }

    private static ThreadFactory threadFactory(String nameFormat) {
        return new ThreadFactoryBuilder().setNameFormat(nameFormat).build();
    }

    static long timeSince(long start, TimeUnit unit) {
        return unit.convert(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
    }

    /**
     * The sessions and hosts managed by this a Cluster instance.
     * <p>
     * Note: the reason we create a Manager object separate from Cluster is
     * that Manager is not publicly visible. For instance, we wouldn't want
     * user to be able to call the {@link #onUp} and {@link #onDown} methods.
     */
    class Manager implements Host.StateListener, Connection.DefaultResponseHandler {

        // Initial contacts point
        final List<InetAddress> contactPoints;
        final Set<Session> sessions = new CopyOnWriteArraySet<Session>();

        final Metadata metadata;
        final Configuration configuration;
        final Metrics metrics;

        final Connection.Factory connectionFactory;
        final ControlConnection controlConnection;

        final ConvictionPolicy.Factory convictionPolicyFactory = new ConvictionPolicy.Simple.Factory();

        final ScheduledExecutorService reconnectionExecutor = Executors.newScheduledThreadPool(2, threadFactory("Reconnection-%d"));
        // scheduledTasksExecutor is used to process C* notifications. So having it mono-threaded ensures notifications are
        // applied in the order received.
        final ScheduledExecutorService scheduledTasksExecutor = Executors.newScheduledThreadPool(1, threadFactory("Scheduled Tasks-%d"));

        final ExecutorService executor = Executors.newCachedThreadPool(threadFactory("Cassandra Java Driver worker-%d"));

        final AtomicBoolean isShutdown = new AtomicBoolean(false);

        // All the queries that have been prepared (we keep them so we can re-prepared them when a node fail or a
        // new one join the cluster).
        // Note: we could move this down to the session level, but since prepared statement are global to a node,
        // this would yield a slightly less clear behavior.
        final Map<MD5Digest, PreparedStatement> preparedQueries = new ConcurrentHashMap<MD5Digest, PreparedStatement>();

        private Manager(List<InetAddress> contactPoints, Configuration configuration) {
            this.configuration = configuration;
            this.metadata = new Metadata(this);
            this.contactPoints = contactPoints;
            this.connectionFactory = new Connection.Factory(this, configuration.getAuthInfoProvider());

            for (InetAddress address : contactPoints)
                addHost(address, false);

            this.controlConnection = new ControlConnection(this, metadata);

            this.metrics = new Metrics(this);
            this.configuration.register(this);

            try {
                this.controlConnection.connect();
            } catch (NoHostAvailableException e) {
                try {
                    shutdown(0, TimeUnit.MILLISECONDS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
                throw e;
            }
        }

        // This is separated from the constructor because this reference the
        // Cluster object, whose manager won't be properly initialized until
        // the constructor returns.
        private void init() {
            this.configuration.getPolicies().getLoadBalancingPolicy().init(Cluster.this, metadata.getAllHosts());
        }

        Cluster getCluster() {
            return Cluster.this;
        }

        private Session newSession() {
            Session session = new Session(Cluster.this, metadata.allHosts());
            sessions.add(session);
            return session;
        }

        private boolean shutdown(long timeout, TimeUnit unit) throws InterruptedException {

            if (!isShutdown.compareAndSet(false, true))
                return true;

            logger.debug("Shutting down");

            long start = System.currentTimeMillis();
            boolean success = true;

            success &= controlConnection.shutdown(timeout, unit);

            for (Session session : sessions)
                success &= session.shutdown(timeout - timeSince(start, unit), unit);

            reconnectionExecutor.shutdown();
            scheduledTasksExecutor.shutdown();
            executor.shutdown();

            success &= connectionFactory.shutdown(timeout - timeSince(start, unit), unit);

            if (metrics != null)
                metrics.shutdown();

            // Note that it's on purpose that we shutdown everything *even* if the timeout
            // is reached early
            return success
                && reconnectionExecutor.awaitTermination(timeout - timeSince(start, unit), unit)
                && scheduledTasksExecutor.awaitTermination(timeout - timeSince(start, unit), unit)
                && executor.awaitTermination(timeout - timeSince(start, unit), unit);
        }

        public void onUp(Host host) {
            logger.trace("Host {} is UP", host);

            // If there is a reconnection attempt scheduled for that node, cancel it
            ScheduledFuture scheduledAttempt = host.reconnectionAttempt.getAndSet(null);
            if (scheduledAttempt != null)
                scheduledAttempt.cancel(false);

            try {
                prepareAllQueries(host);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                // Don't propagate because we don't want to prevent other listener to run
            }

            controlConnection.onUp(host);
            for (Session s : sessions)
                s.manager.onUp(host);
        }

        public void onDown(final Host host) {
            logger.trace("Host {} is DOWN", host);
            controlConnection.onDown(host);
            for (Session s : sessions)
                s.manager.onDown(host);

            // Note: we basically waste the first successful reconnection, but it's probably not a big deal
            logger.debug("{} is down, scheduling connection retries", host);
            new AbstractReconnectionHandler(reconnectionExecutor, configuration.getPolicies().getReconnectionPolicy().newSchedule(), host.reconnectionAttempt) {

                protected Connection tryReconnect() throws ConnectionException, InterruptedException {
                    return connectionFactory.open(host);
                }

                protected void onReconnection(Connection connection) {
                    logger.debug("Successful reconnection to {}, setting host UP", host);
                    host.getMonitor().setUp();
                }

                protected boolean onConnectionException(ConnectionException e, long nextDelayMs) {
                    if (logger.isDebugEnabled())
                        logger.debug("Failed reconnection to {} ({}), scheduling retry in {} milliseconds", new Object[]{ host, e.getMessage(), nextDelayMs});
                    return true;
                }

                protected boolean onUnknownException(Exception e, long nextDelayMs) {
                    logger.error(String.format("Unknown error during control connection reconnection, scheduling retry in %d milliseconds", nextDelayMs), e);
                    return true;
                }

            }.start();
        }

        public void onAdd(Host host) {
            logger.trace("Adding new host {}", host);

            try {
                prepareAllQueries(host);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                // Don't propagate because we don't want to prevent other listener to run
            }

            controlConnection.onAdd(host);
            for (Session s : sessions)
                s.manager.onAdd(host);
        }

        public void onRemove(Host host) {
            logger.trace("Removing host {}", host);
            controlConnection.onRemove(host);
            for (Session s : sessions)
                s.manager.onRemove(host);
        }

        public Host addHost(InetAddress address, boolean signal) {
            Host newHost = metadata.add(address);
            if (newHost != null && signal) {
                logger.info("New Cassandra host {} added", newHost);
                onAdd(newHost);
            }
            return newHost;
        }

        public void removeHost(Host host) {
            if (host == null)
                return;

            if (metadata.remove(host)) {
                logger.info("Cassandra host {} removed", host);
                onRemove(host);
            }
        }

        public void ensurePoolsSizing() {
            for (Session session : sessions) {
                for (HostConnectionPool pool : session.manager.pools.values())
                    pool.ensureCoreConnections();
            }
        }

        // Prepare a query on all nodes
        // Note that this *assumes* the query is valid.
        public void prepare(MD5Digest digest, PreparedStatement stmt, InetAddress toExclude) throws InterruptedException {
            preparedQueries.put(digest, stmt);
            for (Session s : sessions)
                s.manager.prepare(stmt.getQueryString(), toExclude);
        }

        private void prepareAllQueries(Host host) throws InterruptedException {
            if (preparedQueries.isEmpty())
                return;

            logger.debug("Preparing {} prepared queries on newly up node {}", preparedQueries.size(), host);
            try {
                Connection connection = connectionFactory.open(host);

                try
                {
                    try {
                        ControlConnection.waitForSchemaAgreement(connection, metadata);
                    } catch (ExecutionException e) {
                        // As below, just move on
                    }

                    // Furthermore, along with each prepared query we keep the current keyspace at the time of preparation
                    // as we need to make it is the same when we re-prepare on new/restarted nodes. Most query will use the
                    // same keyspace so keeping it each time is slightly wasteful, but this doesn't really matter and is
                    // simpler. Besides, we do avoid in prepareAllQueries to not set the current keyspace more than needed.

                    // We need to make sure we prepared every query with the right current keyspace, i.e. the one originally
                    // used for preparing it. However, since we are likely that all prepared query belong to only a handful
                    // of different keyspace (possibly only one), and to avoid setting the current keyspace more than needed,
                    // we first sort the query per keyspace.
                    SetMultimap<String, String> perKeyspace = HashMultimap.create();
                    for (PreparedStatement ps : preparedQueries.values()) {
                        // It's possible for a query to not have a current keyspace. But since null doesn't work well as
                        // map keys, we use the empty string instead (that is not a valid keyspace name).
                        String keyspace = ps.getQueryKeyspace() == null ? "" : ps.getQueryKeyspace();
                        perKeyspace.put(keyspace, ps.getQueryString());
                    }

                    for (String keyspace : perKeyspace.keySet())
                    {
                        // Empty string mean no particular keyspace to set
                        if (!keyspace.isEmpty())
                            connection.setKeyspace(keyspace);

                        List<Connection.Future> futures = new ArrayList<Connection.Future>(preparedQueries.size());
                        for (String query : perKeyspace.get(keyspace)) {
                            futures.add(connection.write(new PrepareMessage(query)));
                        }
                        for (Connection.Future future : futures) {
                            try {
                                future.get();
                            } catch (ExecutionException e) {
                                // This "might" happen if we drop a CF but haven't removed it's prepared queries (which we don't do
                                // currently). It's not a big deal however as if it's a more serious problem it'll show up later when
                                // the query is tried for execution.
                                logger.debug("Unexpected error while preparing queries on new/newly up host", e);
                            }
                        }
                    }
                } finally {
                    connection.close(0, TimeUnit.MILLISECONDS);
                }
            } catch (ConnectionException e) {
                // Ignore, not a big deal
            } catch (AuthenticationException e) {
                // That's a bad news, but ignore at this point
            } catch (BusyConnectionException e) {
                // Ignore, not a big deal
            }
        }

        public void submitSchemaRefresh(final String keyspace, final String table) {
            logger.trace("Submitting schema refresh");
            executor.submit(new Runnable() {
                public void run() {
                    try {
                        controlConnection.refreshSchema(keyspace, table);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            });
        }

        // refresh the schema using the provided connection, and notice the future with the provided resultset once done
        public void refreshSchema(final Connection connection, final SimpleFuture future, final ResultSet rs, final String keyspace, final String table) {
            if (logger.isDebugEnabled())
                logger.debug("Refreshing schema for {}{}", keyspace == null ? "" : keyspace, table == null ? "" : "." + table);

            executor.submit(new Runnable() {
                public void run() {
                    try {
                        // Before refreshing the schema, wait for schema agreement so that querying a table just after having created it don't fail.
                        ControlConnection.waitForSchemaAgreement(connection, metadata);
                        ControlConnection.refreshSchema(connection, keyspace, table, Cluster.Manager.this);
                    } catch (Exception e) {
                        logger.error("Error during schema refresh ({}). The schema from Cluster.getMetadata() might appear stale. Asynchronously submitting job to fix.", e.getMessage());
                        submitSchemaRefresh(keyspace, table);
                    } finally {
                        // Always sets the result
                        future.set(rs);
                    }
                }
            });
        }

        // Called when some message has been received but has been initiated from the server (streamId < 0).
        public void handle(Message.Response response) {

            if (!(response instanceof EventMessage)) {
                logger.error("Received an unexpected message from the server: {}", response);
                return;
            }

            final Event event = ((EventMessage)response).event;

            logger.debug("Received event {}, scheduling delivery", response);

            // When handle is called, the current thread is a network I/O  thread, and we don't want to block
            // it (typically addHost() will create the connection pool to the new node, which can take time)
            // Besides, up events are usually sent a bit too early (since they're triggered once gossip is up,
            // but that before the client-side server is up) so adds a 1 second delay in that case.
            // TODO: this delay is honestly quite random. We should do something on the C* side to fix that.
            scheduledTasksExecutor.schedule(new Runnable() {
                public void run() {
                    switch (event.type) {
                        case TOPOLOGY_CHANGE:
                            Event.TopologyChange tpc = (Event.TopologyChange)event;
                            switch (tpc.change) {
                                case NEW_NODE:
                                    addHost(tpc.node.getAddress(), true);
                                    break;
                                case REMOVED_NODE:
                                    removeHost(metadata.getHost(tpc.node.getAddress()));
                                    break;
                                case MOVED_NODE:
                                    controlConnection.refreshNodeListAndTokenMap();
                                    break;
                            }
                            break;
                        case STATUS_CHANGE:
                            Event.StatusChange stc = (Event.StatusChange)event;
                            switch (stc.status) {
                                case UP:
                                    Host hostUp = metadata.getHost(stc.node.getAddress());
                                    if (hostUp == null) {
                                        // first time we heard about that node apparently, add it
                                        addHost(stc.node.getAddress(), true);
                                    } else {
                                        hostUp.getMonitor().setUp();
                                    }
                                    break;
                                case DOWN:
                                    // Note that there is a slight risk we can receive the event late and thus
                                    // mark the host down even though we already had reconnected successfully.
                                    // But it is unlikely, and don't have too much consequence since we'll try reconnecting
                                    // right away, so we favor the detection to make the Host.isUp method more reliable.
                                    Host hostDown = metadata.getHost(stc.node.getAddress());
                                    if (hostDown != null) {
                                        hostDown.getMonitor().setDown();
                                    }
                                    break;
                            }
                            break;
                        case SCHEMA_CHANGE:
                            Event.SchemaChange scc = (Event.SchemaChange)event;
                            switch (scc.change) {
                                case CREATED:
                                    if (scc.table.isEmpty())
                                        submitSchemaRefresh(null, null);
                                    else
                                        submitSchemaRefresh(scc.keyspace, null);
                                    break;
                                case DROPPED:
                                    if (scc.table.isEmpty())
                                        submitSchemaRefresh(null, null);
                                    else
                                        submitSchemaRefresh(scc.keyspace, null);
                                    break;
                                case UPDATED:
                                    if (scc.table.isEmpty())
                                        submitSchemaRefresh(scc.keyspace, null);
                                    else
                                        submitSchemaRefresh(scc.keyspace, scc.table);
                                    break;
                            }
                            break;
                    }
                }
            }, delayForEvent(event), TimeUnit.SECONDS);
        }

        private int delayForEvent(Event event) {
            switch (event.type) {
                case TOPOLOGY_CHANGE:
                    // Could probably be 0 for REMOVED_NODE but it's inconsequential
                    return 1;
                case STATUS_CHANGE:
                    Event.StatusChange stc = (Event.StatusChange)event;
                    if (stc.status == Event.StatusChange.Status.UP)
                        return 1;
                    break;
            }
            return 0;
        }

    }
}
