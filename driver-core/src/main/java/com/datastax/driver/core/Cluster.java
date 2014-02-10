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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Predicates;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.MapMaker;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.*;
import org.apache.cassandra.transport.Event;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.messages.EventMessage;
import org.apache.cassandra.transport.messages.PrepareMessage;
import org.apache.cassandra.utils.MD5Digest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.policies.*;

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

    private static final int DEFAULT_THREAD_KEEP_ALIVE = 30;

    final Manager manager;

    /**
     * Constructs a new Cluster instance.
     * <p>
     * This constructor is mainly exposed so Cluster can be sub-classed as a mean to make testing/mocking
     * easier or to "intecept" it's method call. Most users shouldn't extend this class however and
     * should prefer either using the {@link #builder} or calling {@link #buildFrom} with a custom
     * Initializer.
     *
     * @param contactPoints the list of contact points to use for the new cluster.
     * @param configuration the configuration for the new cluster.
     * @param init whether or not initialization should be perform by this constructor. Passing
     * {@code false} is equivalent to using {@link Builder#withDeferredInitialization} on a
     * {@code Cluster.Builder}.
     */
    protected Cluster(List<InetAddress> contactPoints, Configuration configuration, boolean init) {
        // Note: we don't want to make init part of Configuration. In 2.0, the default is not init
        // so there is not point in breaking Configuration API for that. However, as a workaround
        // until upgrade, we still want to allow optional lazy initialization of the control
        // connection (see #JAVA-161)
        this.manager = new Manager(contactPoints, configuration);
        if (init)
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

        return new Cluster(contactPoints, initializer.getConfiguration(), true);
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
        manager.init(); // Calls init if deferInitialization was used. It's a no-op if it's already initialized.
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
        SessionManager session = (SessionManager)connect();
        try {
            session.setKeyspace(keyspace);
        } catch (RuntimeException e) {
            session.shutdown();
            throw e;
        }
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
        manager.init(); // Calls init if deferInitialization was used. It's a no-op if it's already initialized.
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
     * been disabled (that is if {@link Configuration#getMetricsOptions}
     * returns {@code null}).
     */
    public Metrics getMetrics() {
        return manager.metrics;
    }

    /**
     * Registers the provided listener to be notified on hosts
     * up/down/added/removed events.
     * <p>
     * Registering the same listener multiple times is a no-op.
     * <p>
     * Note that while {@link LoadBalancingPolicy} implements
     * {@code Host.StateListener}, the configured load balancy does not
     * need to (and should not) be registered through this  method to
     * received host related events.
     *
     * @param listener the new {@link Host.StateListener} to register.
     * @return this {@code Cluster} object;
     */
    public Cluster register(Host.StateListener listener) {
        manager.listeners.add(listener);
        return this;
    }

    /**
     * Unregisters the provided listener from being notified on hosts events.
     * <p>
     * This method is a no-op if {@code listener} hadn't previously be
     * registered against this Cluster.
     *
     * @param listener the {@link Host.StateListener} to unregister.
     * @return this {@code Cluster} object;
     */
    public Cluster unregister(Host.StateListener listener) {
        manager.listeners.remove(listener);
        return this;
    }

    /**
     * Registers the provided tracker to be updated with hosts read
     * latencies.
     * <p>
     * Registering the same listener multiple times is a no-op.
     * <p>
     * Be warry that the registered tracker {@code update} method will be call
     * very frequently (at the end of every query to a Cassandra host) and
     * should thus not be costly.
     * <p>
     * The main use case for a {@code LatencyTracker} is so
     * {@link LoadBalancingPolicy} can implement latency awareness
     * Typically, {@link LatencyAwarePolicy} registers  it's own internal
     * {@code LatencyTracker} (automatically, you don't have to call this
     * method directly).
     *
     * @param tracker the new {@link LatencyTracker} to register.
     * @return this {@code Cluster} object;
     */
    public Cluster register(LatencyTracker tracker) {
        manager.trackers.add(tracker);
        return this;
    }

    /**
     * Unregisters the provided latency tracking from being updated
     * with host read latencies.
     * <p>
     * This method is a no-op if {@code tracker} hadn't previously be
     * registered against this Cluster.
     *
     * @param tracker the {@link LatencyTracker} to unregister.
     * @return this {@code Cluster} object;
     */
    public Cluster unregister(LatencyTracker tracker) {
        manager.trackers.remove(tracker);
        return this;
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
        private SSLOptions sslOptions = null;
        private boolean metricsEnabled = true;
        private boolean jmxEnabled = true;
        private PoolingOptions poolingOptions = new PoolingOptions();
        private SocketOptions socketOptions = new SocketOptions();

        private boolean deferInitialization = false;

        @Override
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
         * {@link Policies#defaultLoadBalancingPolicy} will be used instead.
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
         * Uses the provided credentials when connecting to Cassandra hosts.
         * <p>
         * This should be used if the Cassandra cluster has been configured to
         * use the {@code PasswordAuthenticator}. If the the default {@code
         * AllowAllAuthenticator} is used instead, using this method has no
         * effect.
         *
         * @param username the username to use to login to Cassandra hosts.
         * @param password the password corresponding to {@code username}.
         * @return this Builder
         */
        public Builder withCredentials(String username, String password) {
            this.authProvider = new AuthInfoProvider.Simple(username, password);
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
         * Enables the use of SSL for the created {@code Cluster}.
         * <p>
         * Calling this method will use default SSL options (see {@link SSLOptions#SSLOptions()}).
         * This is thus a shortcut for {@code withSSL(new SSLOptions())}.
         *
         * Note that if SSL is enabled, the driver will not connect to any
         * Cassandra nodes that doesn't have SSL enabled and it is strongly
         * advised to enable SSL on every Cassandra node if you plan on using
         * SSL in the driver.
         *
         * @return this builder
         */
        public Builder withSSL() {
            this.sslOptions = new SSLOptions();
            return this;
        }

        /**
         * Enable the use of SSL for the created {@code Cluster} using the provided options.
         *
         * @param sslOptions the SSL options to use.
         *
         * @return this builder
         */
        public Builder withSSL(SSLOptions sslOptions) {
            this.sslOptions = sslOptions;
            return this;
        }

        /**
         * Disables JMX reporting of the metrics.
         * <p>
         * JMX reporting is enabled by default (see {@link Metrics}) but can be
         * disabled using this option. If metrics are disabled, this is a
         * no-op.
         *
         * @return this builder
         */
        public Builder withoutJMXReporting() {
            this.jmxEnabled = false;
            return this;
        }

        /**
         * Return the pooling options used by this builder.
         *
         * @return the pooling options that will be used by this builder. You
         * can use the returned object to define the initial pooling options
         * for the built cluster.
         *
         * @deprecated you are now encouraged to use the {@link #withPoolingOptions}
         * method. This method is deprecated and will be removed in the next major
         * version of the driver.
         */
        @Deprecated
        public PoolingOptions poolingOptions() {
            return poolingOptions;
        }

        /**
         * Set the PoolingOptions to use for the newly created Cluster.
         * <p>
         * If no pooling options are set through this method, default pooling
         * options will be used.
         *
         * @param options the pooling options to use.
         * @return this builder.
         */
        public Builder withPoolingOptions(PoolingOptions options) {
            this.poolingOptions = options;
            return this;
        }

        /**
         * Returns the socket options used by this builder.
         *
         * @return the socket options that will be used by this builder. You
         * can use the returned object to define the initial socket options
         * for the built cluster.
         *
         * @deprecated you are now encouraged to use the {@link #withPoolingOptions}
         * method. This method is deprecated and will be removed in the next major
         * version of the driver.
         */
        @Deprecated
        public SocketOptions socketOptions() {
            return socketOptions;
        }

        /**
         * Set the SocketOptions to use for the newly created Cluster.
         * <p>
         * If no socket options are set through this method, default socket
         * options will be used.
         *
         * @param options the socket options to use.
         * @return this builder.
         */
        public Builder withSocketOptions(SocketOptions options) {
            this.socketOptions = options;
            return this;
        }

        /**
         * Defer the initialization of the created cluster.
         * <p>
         * By default, building the cluster (calling the {@link #build} method of this object)
         * triggers the creation of a connection to one of the contact points.
         * That connection is then used to fetch the metadata on the Cassandra
         * cluster we are connected to (other nodes, schema, ...). If this
         * method is used, the creation of that connection will be deferred until the first
         * call to {@code connect()} or {@code getMetadata()} on the resulting {@code Cluster}
         * object.
         * <p>
         * This method is useful when it is not convenient to deal with connection problems
         * while creating the Cluster object. Note that this method only exists
         * in the 1.X branch of the driver since deferred initialization is the default on
         * the 2.X branch.
         *
         * @return this builder.
         */
        public Builder withDeferredInitialization() {
            this.deferInitialization = true;
            return this;
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
        @Override
        public Configuration getConfiguration() {
            Policies policies = new Policies(
                loadBalancingPolicy == null ? Policies.defaultLoadBalancingPolicy() : loadBalancingPolicy,
                reconnectionPolicy == null ? Policies.defaultReconnectionPolicy() : reconnectionPolicy,
                retryPolicy == null ? Policies.defaultRetryPolicy() : retryPolicy
            );
            return new Configuration(policies,
                                     new ProtocolOptions(port, sslOptions).setCompression(compression),
                                     poolingOptions,
                                     socketOptions,
                                     authProvider,
                                     metricsEnabled ? new MetricsOptions(jmxEnabled) : null);
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
            List<InetAddress> contactPoints = getContactPoints();
            if (contactPoints.isEmpty())
                throw new IllegalArgumentException("Cannot build a cluster without contact points");

            return new Cluster(contactPoints, getConfiguration(), !deferInitialization);
        }
    }

    private static ThreadFactory threadFactory(String nameFormat) {
        return new ThreadFactoryBuilder().setNameFormat(nameFormat).build();
    }

    static long timeSince(long startNanos, TimeUnit destUnit) {
        return destUnit.convert(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
    }

    private static ListeningExecutorService makeExecutor(int threads, String name) {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(threads,
                                                             threads,
                                                             DEFAULT_THREAD_KEEP_ALIVE,
                                                             TimeUnit.SECONDS,
                                                             new LinkedBlockingQueue<Runnable>(),
                                                             threadFactory(name));

        executor.allowCoreThreadTimeOut(true);
        return MoreExecutors.listeningDecorator(executor);
    }

    /**
     * The sessions and hosts managed by this a Cluster instance.
     * <p>
     * Note: the reason we create a Manager object separate from Cluster is
     * that Manager is not publicly visible. For instance, we wouldn't want
     * user to be able to call the {@link #onUp} and {@link #onDown} methods.
     */
    class Manager implements Host.StateListener, Connection.DefaultResponseHandler {

        private final AtomicBoolean isInit = new AtomicBoolean(false);

        // Initial contacts point
        final List<InetAddress> contactPoints;
        final Set<SessionManager> sessions = new CopyOnWriteArraySet<SessionManager>();

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

        // Executor used for tasks that shouldn't be executed on an IO thread. Used for short-lived, generally non-blocking tasks
        final ListeningExecutorService executor;

        // An executor for tasks that migth block some time, like creating new connection, but are generally not too critical.
        final ListeningExecutorService blockingTasksExecutor;

        final AtomicBoolean isShutdown = new AtomicBoolean(false);

        // All the queries that have been prepared (we keep them so we can re-prepared them when a node fail or a
        // new one join the cluster).
        // Note: we could move this down to the session level, but since prepared statement are global to a node,
        // this would yield a slightly less clear behavior.
        final ConcurrentMap<MD5Digest, PreparedStatement> preparedQueries = new MapMaker().weakValues().makeMap();

        final Set<Host.StateListener> listeners = new CopyOnWriteArraySet<Host.StateListener>();
        final Set<LatencyTracker> trackers = new CopyOnWriteArraySet<LatencyTracker>();

        private Manager(List<InetAddress> contactPoints, Configuration configuration) {
            logger.debug("Starting new cluster with contact points " + contactPoints);

            this.executor = makeExecutor(Runtime.getRuntime().availableProcessors(), "Cassandra Java Driver worker-%d");
            this.blockingTasksExecutor = makeExecutor(2, "Cassandra Java Driver blocking tasks worker-%d");

            this.configuration = configuration;
            this.metadata = new Metadata(this);
            this.contactPoints = contactPoints;
            this.connectionFactory = new Connection.Factory(this, configuration.getAuthInfoProvider());

            this.controlConnection = new ControlConnection(this);

            this.metrics = configuration.getMetricsOptions() == null ? null : new Metrics(this);
            this.configuration.register(this);
        }

        private void init() {
            if (!isInit.compareAndSet(false, true))
                return;

            // Note: we mark the initial contact point as UP, because we have no prior
            // notion of their state and no real way to know until we connect to them
            // (since the node status is not exposed by C* in the System tables). This
            // may not be correct.
            for (InetAddress address : contactPoints) {
                Host host = addHost(address, false);
                if (host != null)
                    host.setUp();
            }

            loadBalancingPolicy().init(Cluster.this, metadata.allHosts());

            try {
                controlConnection.connect();
            } catch (NoHostAvailableException e) {
                try {
                    shutdown(0, TimeUnit.MILLISECONDS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
                throw e;
            }
        }

        Cluster getCluster() {
            return Cluster.this;
        }

        LoadBalancingPolicy loadBalancingPolicy() {
            return configuration.getPolicies().getLoadBalancingPolicy();
        }

        ReconnectionPolicy reconnectionPolicy() {
            return configuration.getPolicies().getReconnectionPolicy();
        }

        private Session newSession() {
            init();

            SessionManager session = new SessionManager(Cluster.this, metadata.allHosts());
            sessions.add(session);
            return session;
        }

        void reportLatency(Host host, long latencyNanos) {
            for (LatencyTracker tracker : trackers) {
                tracker.update(host, latencyNanos);
            }
        }

        private boolean shutdown(long timeout, TimeUnit unit) throws InterruptedException {

            if (!isShutdown.compareAndSet(false, true))
                return true;

            logger.debug("Shutting down");

            long start = System.nanoTime();
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

        @Override
        public void onUp(final Host host) {
            logger.trace("Host {} is UP", host);

            if (isShutdown.get())
                return;

            if (host.isUp())
                return;

            // If there is a reconnection attempt scheduled for that node, cancel it
            ScheduledFuture<?> scheduledAttempt = host.reconnectionAttempt.getAndSet(null);
            if (scheduledAttempt != null) {
                logger.debug("Cancelling reconnection attempt since node is UP");
                scheduledAttempt.cancel(false);
            }

            try {
                prepareAllQueries(host);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                // Don't propagate because we don't want to prevent other listener to run
            }

            // Session#onUp() expects the load balancing policy to have been updated first, so that
            // Host distances are up to date. This mean the policy could return the node before the
            // new pool have been created. This is harmless if there is no prior pool since RequestHandler
            // will ignore the node, but we do want to make sure there is no prior pool so we don't
            // query from a pool we will shutdown right away.
            for (SessionManager s : sessions)
                s.removePool(host);
            loadBalancingPolicy().onUp(host);
            controlConnection.onUp(host);

            List<ListenableFuture<Boolean>> futures = new ArrayList<ListenableFuture<Boolean>>(sessions.size());
            for (SessionManager s : sessions)
                futures.add(s.addOrRenewPool(host, false));

            // Only mark the node up once all session have re-added their pool (if the loadbalancing
            // policy says it should), so that Host.isUp() don't return true before we're reconnected
            // to the node.
            Futures.addCallback(Futures.allAsList(futures), new FutureCallback<List<Boolean>>() {
                public void onSuccess(List<Boolean> poolCreationResults) {
                    // If any of the creation failed, they will have signaled a connection failure
                    // which will trigger a reconnection to the node. So don't bother marking UP.
                    if (Iterables.any(poolCreationResults, Predicates.equalTo(false))) {
                        logger.debug("Connection pool cannot be created, not marking {} UP", host);
                        return;
                    }

                    host.setUp();

                    for (Host.StateListener listener : listeners)
                        listener.onUp(host);

                    // Now, check if there isn't pools to create/remove following the addition.
                    // We do that now only so that it's not called before we've set the node up.
                    for (SessionManager s : sessions)
                        s.updateCreatedPools();
                }

                public void onFailure(Throwable t) {
                    // That future is not really supposed to throw unexpected exceptions
                    if (!(t instanceof InterruptedException))
                        logger.error("Unexpected error while marking node UP: while this shouldn't happen, this shouldn't be critical", t);
                }
            });
        }

        @Override
        public void onDown(final Host host) {
            onDown(host, false);
        }

        public void onDown(final Host host, final boolean isHostAddition) {
            logger.trace("Host {} is DOWN", host);

            if (isShutdown.get())
                return;

            // Note: we don't want to skip that method if !host.isUp() because we set isUp
            // late in onUp, and so we can rely on isUp if there is an error during onUp.
            // But if there is a reconnection attempt in progress already, then we know
            // we've already gone through that method since the last successful onUp(), so
            // we're good skipping it.
            if (host.reconnectionAttempt.get() != null)
                return;

            boolean wasUp = host.isUp();
            host.setDown();

            loadBalancingPolicy().onDown(host);
            controlConnection.onDown(host);
            for (SessionManager s : sessions)
                s.onDown(host);

            // Contrarily to other actions of that method, there is no reason to notify listeners
            // unless the host was UP at the beginning of this function since even if a onUp fail
            // mid-method, listeners won't  have been notified of the UP.
            if (wasUp) {
                for (Host.StateListener listener : listeners)
                    listener.onDown(host);
            }

            // Note: we basically waste the first successful reconnection, but it's probably not a big deal
            logger.debug("{} is down, scheduling connection retries", host);
            new AbstractReconnectionHandler(reconnectionExecutor, reconnectionPolicy().newSchedule(), host.reconnectionAttempt) {

                protected Connection tryReconnect() throws ConnectionException, InterruptedException {
                    return connectionFactory.open(host);
                }

                protected void onReconnection(Connection connection) {
                    logger.debug("Successful reconnection to {}, setting host UP", host);
                    if (isHostAddition)
                        onAdd(host);
                    else
                        onUp(host);
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

        @Override
        public void onAdd(final Host host) {
            logger.trace("Adding new host {}", host);

            if (isShutdown.get())
                return;

            try {
                prepareAllQueries(host);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                // Don't propagate because we don't want to prevent other listener to run
            }

            loadBalancingPolicy().onAdd(host);
            controlConnection.onAdd(host);

            List<ListenableFuture<Boolean>> futures = new ArrayList<ListenableFuture<Boolean>>(sessions.size());
            for (SessionManager s : sessions)
                futures.add(s.addOrRenewPool(host, true));

            // Only mark the node up once all session have added their pool (if the loadbalancing
            // policy says it should), so that Host.isUp() don't return true before we're reconnected
            // to the node.
            Futures.addCallback(Futures.allAsList(futures), new FutureCallback<List<Boolean>>() {
                public void onSuccess(List<Boolean> poolCreationResults) {
                    // If any of the creation failed, they will have signaled a connection failure
                    // which will trigger a reconnection to the node. So don't bother marking UP.
                    if (Iterables.any(poolCreationResults, Predicates.equalTo(false))) {
                        logger.debug("Connection pool cannot be created, not marking {} UP", host);
                        return;
                    }

                    host.setUp();

                    for (Host.StateListener listener : listeners)
                        listener.onAdd(host);

                    // Now, check if there isn't pools to create/remove following the addition.
                    // We do that now only so that it's not called before we've set the node up.
                    for (SessionManager s : sessions)
                        s.updateCreatedPools();
                }

                public void onFailure(Throwable t) {
                    // That future is not really supposed to throw unexpected exceptions
                    if (!(t instanceof InterruptedException))
                        logger.error("Unexpected error while adding node: while this shouldn't happen, this shouldn't be critical", t);
                }
            });
        }

        @Override
        public void onRemove(Host host) {
            if (isShutdown.get())
                return;

            host.setDown();

            logger.trace("Removing host {}", host);
            loadBalancingPolicy().onRemove(host);
            controlConnection.onRemove(host);
            for (SessionManager s : sessions)
                s.onRemove(host);

            for (Host.StateListener listener : listeners)
                listener.onRemove(host);
        }

        public boolean signalConnectionFailure(Host host, ConnectionException exception, boolean isHostAddition) {
            boolean isDown = host.signalConnectionFailure(exception);
            if (isDown)
                onDown(host, isHostAddition);
            return isDown;
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
            for (SessionManager session : sessions) {
                for (HostConnectionPool pool : session.pools.values())
                    pool.ensureCoreConnections();
            }
        }

        public void addPrepared(PreparedStatement stmt) {
            if (preparedQueries.putIfAbsent(stmt.id, stmt) != null)
                logger.warn("Re-preparing already prepared query {}. Please note that preparing the same query more than once is "
                          + "generally an anti-pattern and will likely affect performance. Consider preparing the statement only once.", stmt.getQueryString());
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
                @Override
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
        public void refreshSchema(final Connection connection, final SimpleFuture<ResultSet> future, final ResultSet rs, final String keyspace, final String table) {
            if (logger.isDebugEnabled())
                logger.debug("Refreshing schema for {}{}", keyspace == null ? "" : keyspace, table == null ? "" : "." + table);

            executor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        // Before refreshing the schema, wait for schema agreement so
                        // that querying a table just after having created it don't fail.
                        if (!ControlConnection.waitForSchemaAgreement(connection, metadata))
                            logger.warn("No schema agreement from live replicas after {} ms. The schema may not be up to date on some nodes.", ControlConnection.MAX_SCHEMA_AGREEMENT_WAIT_MS);
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
        @Override
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
                @Override
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
                                        onUp(hostUp);
                                    }
                                    break;
                                case DOWN:
                                    // Note that there is a slight risk we can receive the event late and thus
                                    // mark the host down even though we already had reconnected successfully.
                                    // But it is unlikely, and don't have too much consequence since we'll try reconnecting
                                    // right away, so we favor the detection to make the Host.isUp method more reliable.
                                    Host hostDown = metadata.getHost(stc.node.getAddress());
                                    if (hostDown != null) {
                                        onDown(hostDown);
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
