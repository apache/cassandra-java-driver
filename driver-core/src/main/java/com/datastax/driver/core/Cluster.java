package com.datastax.driver.core;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.cassandra.utils.MD5Digest;
import org.apache.cassandra.transport.Event;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.messages.EventMessage;
import org.apache.cassandra.transport.messages.PrepareMessage;
import org.apache.cassandra.transport.messages.QueryMessage;

import com.datastax.driver.core.exceptions.*;
import com.datastax.driver.core.policies.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Informations and known state of a Cassandra cluster.
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
 * A cluster object maintains a permanent connection to one of the cluster node
 * that it uses solely to maintain informations on the state and current
 * topology of the cluster. Using the connection, the driver will discover all
 * the nodes composing the cluster as well as new nodes joining the cluster.
 */
public class Cluster {

    private static final Logger logger = LoggerFactory.getLogger(Cluster.class);

    final Manager manager;

    private Cluster(List<InetAddress> contactPoints, Configuration configuration) throws NoHostAvailableException {
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
     * by {@code initiazer} is empty or if not all those contact points have the same port.
     * @throws AuthenticationException if while contacting the initial
     * contact points an authencation error occurs.
     */
    public static Cluster buildFrom(Initializer initializer) throws NoHostAvailableException {
        List<InetAddress> contactPoints = initializer.getContactPoints();
        if (contactPoints.isEmpty())
            throw new IllegalArgumentException("Cannot build a cluster without contact points");

        return new Cluster(contactPoints, initializer.getConfiguration());
    }

    /**
     * Creates a new {@link Cluster.Builder} instance.
     * <p>
     * This is a shortcut for {@code new Cluster.Builder()}.
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
     * Creates a new session on this cluster and sets a keyspace to use.
     *
     * @param keyspace The name of the keyspace to use for the created
     * {@code Session}.
     * @return a new session on this cluster sets to keyspace
     * {@code keyspaceName}.
     *
     * @throws NoHostAvailableException if no host can be contacted to set the
     * {@code keyspace}.
     */
    public Session connect(String keyspace) throws NoHostAvailableException {
        Session session = connect();
        session.manager.setKeyspace(keyspace);
        return session;
    }

    /**
     * Returns read-only metadata on the connected cluster.
     * <p>
     * This includes the know nodes (with their status as seen by the driver)
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
     * Shutdown this cluster instance.
     *
     * This closes all connections from all the sessions of this {@code
     * Cluster} instance and reclaim all resources used by it.
     * <p>
     * This method has no effect if the cluster was already shutdown.
     */
    public void shutdown() {
        manager.shutdown();
    }

    /**
     * Initializer for {@link Cluster} instances.
     * <p>
     * If you want to create a new {@code Cluster} instance programmatically,
     * then it is advised to use {@link Cluster.Builder} (obtained through the
     * {@link Cluster#builder} method).
     * <p>
     * But it is also possible to implement a custom {@code Initializer} that
     * retrieve initialization from a web-service or from a configuration file
     * for instance.
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
         * initialization but some other cannot. In particular, the ones that
         * cannot be change afterwards includes:
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
         * one contact point, as if that unique contact point is not available,
         * the driver won't be able to initialize itself correctly.
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
         * Add contact points.
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
         * Add contact points.
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
         * Configure the load balancing policy to use for the new cluster.
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
         * Configure the reconnection policy to use for the new cluster.
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
         * Configure the retry policy to use for the new cluster.
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
         * Use the provided {@code AuthInfoProvider} to connect to Cassandra hosts.
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
         * Disable metrics collection for the created cluster (metrics are
         * enabled by default otherwise).
         *
         * @return this builder
         */
        public Builder withoutMetrics() {
            this.metricsEnabled = false;
            return this;
        }

        /**
         * The pooling options used by this builder.
         *
         * @return the pooling options that will be used by this builder. You
         * can use the returned object to define the initial pooling options
         * for the built cluster.
         */
        public PoolingOptions poolingOptions() {
            return poolingOptions;
        }

        /**
         * The socket options used by this builder.
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
         * You <b>should not</b> modify this object directly as change made
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
         * Build the cluster with the configured set of initial contact points
         * and policies.
         *
         * This is a shorthand for {@code Cluster.buildFrom(this)}.
         *
         * @return the newly build Cluster instance.
         *
         * @throws NoHostAvailableException if none of the contact points
         * provided can be reached.
         * @throws AuthenticationException if while contacting the initial
         * contact points an authencation error occurs.
         */
        public Cluster build() throws NoHostAvailableException {
            return Cluster.buildFrom(this);
        }
    }

    private static ThreadFactory threadFactory(String nameFormat) {
        return new ThreadFactoryBuilder().setNameFormat(nameFormat).build();
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
        final ScheduledExecutorService scheduledTasksExecutor = Executors.newScheduledThreadPool(1, threadFactory("Scheduled Tasks-%d"));

        final ExecutorService executor = Executors.newCachedThreadPool(threadFactory("Cassandra Java Driver worker-%d"));

        final AtomicBoolean isShutdown = new AtomicBoolean(false);

        // All the queries that have been prepared (we keep them so we can
        // re-prepared them when a node fail or a new one join the cluster).
        // Note: we could move this down to the session level, but since
        // prepared statement are global to a node, this would yield a slightly
        // less clear behavior.
        final Map<MD5Digest, String> preparedQueries = new ConcurrentHashMap<MD5Digest, String>();

        private Manager(List<InetAddress> contactPoints, Configuration configuration) throws NoHostAvailableException {
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
                shutdown();
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

        private void shutdown() {
            if (!isShutdown.compareAndSet(false, true))
                return;

            logger.debug("Shutting down");

            controlConnection.shutdown();

            for (Session session : sessions)
                session.shutdown();

            reconnectionExecutor.shutdownNow();
            scheduledTasksExecutor.shutdownNow();
            executor.shutdownNow();
            connectionFactory.shutdown();

            if (metrics != null)
                metrics.shutdown();
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
                Thread.currentThread().interrupted();
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
                    host.getMonitor().reset();
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
                Thread.currentThread().interrupted();
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
        public void prepare(MD5Digest digest, String query, InetAddress toExclude) throws InterruptedException {
            preparedQueries.put(digest, query);
            for (Session s : sessions)
                s.manager.prepare(query, toExclude);
        }

        private void prepareAllQueries(Host host) throws InterruptedException {
            if (preparedQueries.isEmpty())
                return;

            try {
                Connection connection = connectionFactory.open(host);

                try {
                    ControlConnection.waitForSchemaAgreement(connection, metadata);
                } catch (ExecutionException e) {
                    // As below, just move on
                }

                List<Connection.Future> futures = new ArrayList<Connection.Future>(preparedQueries.size());
                for (String query : preparedQueries.values()) {
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
            // Besides, events are usually sent a bit too early (since they're triggered once gossip is up,
            // but that before the client-side server is up) so adds a 1 second delay.
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
                                    Host host = metadata.getHost(stc.node.getAddress());
                                    if (host == null) {
                                        // first time we heard about that node apparently, add it
                                        addHost(stc.node.getAddress(), true);
                                    } else {
                                        onUp(host);
                                    }
                                    break;
                                case DOWN:
                                    // Ignore down event. Connection will realized a node is dead quicly enough when they write to
                                    // it, and there is no point in taking the risk of marking the node down mistakenly because we
                                    // didn't received the event in a timely fashion
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
            }, 1, TimeUnit.SECONDS);
        }
    }
}
