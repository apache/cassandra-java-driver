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

import java.io.Closeable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Predicates;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.MapMaker;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.core.exceptions.DriverInternalError;
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
public class Cluster implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(Cluster.class);

    // Some per-JVM number that allows to generate unique cluster names when
    // multiple Cluster instance are created in the same JVM.
    private static final AtomicInteger CLUSTER_ID = new AtomicInteger(0);

    private static final int DEFAULT_THREAD_KEEP_ALIVE = 30;

    final Manager manager;

    /**
     * Constructs a new Cluster instance.
     * <p>
     * This constructor is mainly exposed so Cluster can be sub-classed as a means to make testing/mocking
     * easier or to "intercept" its method call. Most users shouldn't extend this class however and
     * should prefer either using the {@link #builder} or calling {@link #buildFrom} with a custom
     * Initializer.
     *
     * @param contactPoints the list of contact points to use for the new cluster.
     * @param configuration the configuration for the new cluster.
     */
    protected Cluster(String name, List<InetAddress> contactPoints, Configuration configuration) {
        this(name, contactPoints, configuration, Collections.<Host.StateListener>emptySet());
    }

    private Cluster(String name, List<InetAddress> contactPoints, Configuration configuration, Collection<Host.StateListener> listeners) {
        this.manager = new Manager(name, contactPoints, configuration, listeners);
    }

    /**
     * Initialize this Cluster instance.
     *
     * This method creates an initial connection to one of the contact points
     * used to construct the {@code Cluster} instance. That connection is then
     * used to populate the cluster {@link Metadata}.
     * <p>
     * Calling this method is optional in the sense that any call to one of the
     * {@code connect} methods of this object will automatically trigger a call
     * to this method beforehand. It is thus only useful to call this method if
     * for some reason you want to populate the metadata (or test that at least
     * one contact point can be reached) without creating a first {@code
     * Session}.
     * <p>
     * Please note that this method only create one connection for metadata
     * gathering reasons. In particular, it doesn't create any connection pool.
     * Those are created when a new {@code Session} is created through
     * {@code connect}.
     * <p>
     * This method has no effect if the cluster is already initialized.
     *
     * @return this {@code Cluster} object.
     *
     * @throws NoHostAvailableException if no host amongst the contact points
     * can be reached.
     * @throws AuthenticationException if an authentication error occurs
     * while contacting the initial contact points.
     */
    public Cluster init() {
        this.manager.init();
        return this;
    }

    /**
     * Build a new cluster based on the provided initializer.
     * <p>
     * Note that for building a cluster pragmatically, Cluster.Builder
     * provides a slightly less verbose shortcut with {@link Builder#build}.
     * <p>
     * Also note that that all the contact points provided by {@code
     * initializer} must share the same port.
     *
     * @param initializer the Cluster.Initializer to use
     * @return the newly created Cluster instance
     *
     * @throws IllegalArgumentException if the list of contact points provided
     * by {@code initializer} is empty or if not all those contact points have the same port.
     */
    public static Cluster buildFrom(Initializer initializer) {
        List<InetAddress> contactPoints = initializer.getContactPoints();
        if (contactPoints.isEmpty())
            throw new IllegalArgumentException("Cannot build a cluster without contact points");

        return new Cluster(initializer.getClusterName(), contactPoints, initializer.getConfiguration(), initializer.getInitialListeners());
    }

    /**
     * Creates a new {@link Cluster.Builder} instance.
     * <p>
     * This is a convenience method for {@code new Cluster.Builder()}.
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
     *
     * @throws NoHostAvailableException if the Cluster has not been initialized yet
     * ({@link #init} has not be called and this is the first connect call) and
     * no host amongst the contact points can be reached.
     * @throws AuthenticationException if an authentication error occurs
     * while contacting the initial contact points.
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
     * @throws NoHostAvailableException if the Cluster has not been initialized yet
     * ({@link #init} has not be called and this is the first connect call) and
     * no host amongst the contact points can be reached, or if no host can be
     * contacted to set the {@code keyspace}.
     * @throws AuthenticationException if an authentication error occurs
     * while contacting the initial contact points.
     */
    public Session connect(String keyspace) {
        SessionManager session = (SessionManager)connect();
        try {
            session.setKeyspace(keyspace);
        } catch (RuntimeException e) {
            session.close();
            throw e;
        }
        return session;
    }

    /**
     * The name of this cluster object.
     * <p>
     * Note that this is not the Cassandra cluster name, but rather a name
     * assigned to this Cluster object. Currently, that name is only used
     * for one purpose: to distinguish exposed JMX metrics when multiple
     * Cluster instances live in the same JVM (which should be rare in the first
     * place). That name can be set at Cluster building time (through
     * {@link Builder#withClusterName} for instance) but will default to a
     * name like {@code cluster1} where each Cluster instance in the same JVM
     * will have a different number.
     *
     * @return the name for this cluster instance.
     */
    public String getClusterName() {
        return manager.clusterName;
    }

    /**
     * Returns read-only metadata on the connected cluster.
     * <p>
     * This includes the known nodes with their status as seen by the driver,
     * as well as the schema definitions. Since this return metadata on the
     * connected cluster, this method may trigger the creation of a connection
     * if none has been established yet (neither {@code init()} nor {@code connect()}
     * has been called yet).
     *
     * @return the cluster metadata.
     *
     * @throws NoHostAvailableException if the Cluster has not been initialized yet
     * and no host amongst the contact points can be reached.
     * @throws AuthenticationException if an authentication error occurs
     * while contacting the initial contact points.
     */
    public Metadata getMetadata() {
        manager.init();
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
     * {@code Host.StateListener}, the configured load balancing does not
     * need to (and should not) be registered through this method to
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
     * Be wary that the registered tracker {@code update} method will be call
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
     * Initiates a shutdown of this cluster instance.
     * <p>
     * This method is asynchronous and return a future on the completion
     * of the shutdown process. As soon a the cluster is shutdown, no
     * new request will be accepted, but already submitted queries are
     * allowed to complete. This method closes all connections from all
     * sessions and reclaims all resources used by this Cluster
     * instance.
     * <p>
     * If for some reason you wish to expedite this process, the
     * {@link CloseFuture#force} can be called on the result future.
     * <p>
     * This method has no particular effect if the cluster was already closed
     * (in which case the returned future will return immediately).
     *
     * @return a future on the completion of the shutdown process.
     */
    public CloseFuture closeAsync() {
        return manager.close();
    }

    /**
     * Initiates a shutdown of this cluster instance and blocks until
     * that shutdown completes.
     * <p>
     * This method is a shortcut for {@code closeAsync().get()}.
     */
    public void close() {
        try {
            closeAsync().get();
        } catch (ExecutionException e) {
            throw DefaultResultSetFuture.extractCauseFromExecutionException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
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
         * An optional name for the created cluster.
         * <p>
         * Such name is optional (a default name will be created otherwise) and is currently
         * only use for JMX reporting of metrics. See {@link Cluster#getClusterName} for more
         * information.
         *
         * @return the name for the created cluster or {@code null} to use an automatically
         * generated name.
         */
        public String getClusterName();

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

        /**
         * Optional listeners to register against the newly created cluster.
         * <p>
         * Note that contrary to listeners registered post Cluster creation,
         * the listeners returned by this method will see {@link Host.StateListener#onAdd}
         * events for the initial contact points.
         *
         * @return a possibly empty collection of {@code Host.StateListener} to register
         * against the newly created cluster.
         */
        public Collection<Host.StateListener> getInitialListeners();
    }

    /**
     * Helper class to build {@link Cluster} instances.
     */
    public static class Builder implements Initializer {

        private String clusterName;
        private final List<InetAddress> addresses = new ArrayList<InetAddress>();
        private int port = ProtocolOptions.DEFAULT_PORT;
        private int protocolVersion = -1;
        private AuthProvider authProvider = AuthProvider.NONE;

        private LoadBalancingPolicy loadBalancingPolicy;
        private ReconnectionPolicy reconnectionPolicy;
        private RetryPolicy retryPolicy;

        private ProtocolOptions.Compression compression = ProtocolOptions.Compression.NONE;
        private SSLOptions sslOptions = null;
        private boolean metricsEnabled = true;
        private boolean jmxEnabled = true;

        private PoolingOptions poolingOptions;
        private SocketOptions socketOptions;
        private QueryOptions queryOptions;

        private Collection<Host.StateListener> listeners;


        @Override
        public String getClusterName() {
            return clusterName;
        }

        @Override
        public List<InetAddress> getContactPoints() {
            return addresses;
        }

        /**
         * An optional name for the create cluster.
         * <p>
         * Note: this is not related to the Cassandra cluster name (though you
         * are free to provide the same name). See {@link Cluster#getClusterName} for
         * details.
         * <p>
         * If you use this method and create more than one Cluster instance in the
         * same JVM (which should be avoided unless you need to connect to multiple
         * Cassandra clusters), you should make sure each Cluster instance get a
         * unique name or you may have a problem with JMX reporting.
         *
         * @param name the cluster name to use for the created Cluster instance.
         * @return this Builder.
         */
        public Builder withClusterName(String name) {
            this.clusterName = name;
            return this;
        }

        /**
         * The port to use to connect to the Cassandra host.
         * <p>
         * If not set through this method, the default port (9042) will be used
         * instead.
         *
         * @param port the port to set.
         * @return this Builder.
         */
        public Builder withPort(int port) {
            this.port = port;
            return this;
        }

        /**
         * The native protocol version to use.
         * <p>
         * The driver supports both version 1 and 2 of the native protocol. Version 2
         * of the protocol has more features and should be preferred, but it is only
         * supported by Cassandra 2.0 and above, so you will have to use version 1 with
         * Cassandra 1.2 nodes.
         * <p>
         * By default, the driver will "auto-detect" which protocol version it can use
         * when connecting to the first node. More precisely, it will try the version
         * 2 first and will fallback to version 1 if it is not supported by that first
         * node it connects to. Please note that once the version is "auto-detected",
         * it won't change: if the first node the driver connects to is a Cassandra 1.2
         * node and auto-detection is used (the default), then the native protocol
         * version 1 will be use for the lifetime of the Cluster instance.
         * <p>
         * This method allows to force the use of a particular protocol version. Forcing
         * version 1 is always fine since all Cassandra version (at least all those
         * supporting the native protocol in the first place) so far supports it. However,
         * please note that a number of features of the driver won't be available if that
         * version of thr protocol is in use, including result set paging,
         * {@link BatchStatement}, executing a non-prepared query with binary values
         * ({@link Session#execute(String, Object...)}), ... (those methods will throw
         * an UnsupportedFeatureException). Using the protocol version 1 should thus
         * only be considered when using Cassandra 1.2, until nodes have been upgraded
         * to Cassandra 2.0.
         * <p>
         * If version 2 of the protocol is used, then Cassandra 1.2 nodes will be ignored
         * (the driver won't connect to them).
         * <p>
         * The default behavior (auto-detection) is fine in almost all case, but you may
         * want to force a particular version if you have a Cassandra cluster with mixed
         * 1.2/2.0 nodes (i.e. during a Cassandra upgrade).
         *
         * @param version the native protocol version to use. The versions supported by
         * this driver are version 1 and 2. Negative values are also supported to trigger
         * auto-detection (see above) but this is the default (so you don't have to call
         * this method for that behavior).
         * @return this Builder.
         *
         * @throws IllegalArgumentException if {@code version} is neither 1, 2 or a
         * negative value.
         */
        public Builder withProtocolVersion(int version) {
            if (version >= 0 && version != 1 && version != 2)
                throw new IllegalArgumentException(String.format("Unsupported protocol version %d; valid values are 1, 2 or negative (for auto-detect).", version));
            this.protocolVersion = version;
            return this;
        }

        /**
         * Adds a contact point.
         * <p>
         * Contact points are addresses of Cassandra nodes that the driver uses
         * to discover the cluster topology. Only one contact point is required
         * (the driver will retrieve the address of the other nodes
         * automatically), but it is usually a good idea to provide more than
         * one contact point, because if that single contact point is unavailable,
         * the driver cannot initialize itself correctly.
         *
         * @param address the address of the node to connect to
         * @return this Builder.
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
         * <p>
         * See {@link Builder#addContactPoint} for more details on contact
         * points.
         *
         * @param addresses addresses of the nodes to add as contact point.
         * @return this Builder.
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
         * <p>
         * See {@link Builder#addContactPoint} for more details on contact
         * points.
         *
         * @param addresses addresses of the nodes to add as contact point.
         * @return this Builder.
         *
         * @see Builder#addContactPoint
         */
        public Builder addContactPoints(InetAddress... addresses) {
            Collections.addAll(this.addresses, addresses);
            return this;
        }

        /**
         * Configures the load balancing policy to use for the new cluster.
         * <p>
         * If no load balancing policy is set through this method,
         * {@link Policies#defaultLoadBalancingPolicy} will be used instead.
         *
         * @param policy the load balancing policy to use.
         * @return this Builder.
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
         * @param policy the reconnection policy to use.
         * @return this Builder.
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
         * @param policy the retry policy to use.
         * @return this Builder.
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
         * @return this Builder.
         */
        public Builder withCredentials(String username, String password) {
            this.authProvider = new PlainTextAuthProvider(username, password);
            return this;
        }

        /**
         * Use the specified AuthProvider when connecting to Cassandra
         * hosts.
         * <p>
         * Use this method when a custom authentication scheme is in place.
         * You shouldn't call both this method and {@code withCredentials}
         * on the same {@code Builder} instance as one will supersede the
         * other
         *
         * @param authProvider the {@link AuthProvider} to use to login to
         * Cassandra hosts.
         * @return this Builder
         */
        public Builder withAuthProvider(AuthProvider authProvider) {
            this.authProvider = authProvider;
            return this;
        }

        /**
         * Sets the compression to use for the transport.
         *
         * @param compression the compression to set.
         * @return this Builder.
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
         * @return this builder.
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
         * <p>
         * Note that if SSL is enabled, the driver will not connect to any
         * Cassandra nodes that doesn't have SSL enabled and it is strongly
         * advised to enable SSL on every Cassandra node if you plan on using
         * SSL in the driver.
         *
         * @return this builder.
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
         * @return this builder.
         */
        public Builder withSSL(SSLOptions sslOptions) {
            this.sslOptions = sslOptions;
            return this;
        }

        /**
         * Register the provided listeners in the newly created cluster.
         * <p>
         * Note: repeated calls to this method will override the previous ones.
         *
         * @param listeners the listeners to register.
         * @return this builder.
         */
        public Builder withInitialListeners(Collection<Host.StateListener> listeners) {
            this.listeners = listeners;
            return this;
        }

        /**
         * Disables JMX reporting of the metrics.
         * <p>
         * JMX reporting is enabled by default (see {@link Metrics}) but can be
         * disabled using this option. If metrics are disabled, this is a
         * no-op.
         *
         * @return this builder.
         */
        public Builder withoutJMXReporting() {
            this.jmxEnabled = false;
            return this;
        }

        /**
         * Sets the PoolingOptions to use for the newly created Cluster.
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
         * Sets the SocketOptions to use for the newly created Cluster.
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
         * Sets the QueryOptions to use for the newly created Cluster.
         * <p>
         * If no query options are set through this method, default query
         * options will be used.
         *
         * @param options the query options to use.
         * @return this builder.
         */
        public Builder withQueryOptions(QueryOptions options) {
            this.queryOptions = options;
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
                                     new ProtocolOptions(port, protocolVersion, sslOptions, authProvider).setCompression(compression),
                                     poolingOptions == null ? new PoolingOptions() : poolingOptions,
                                     socketOptions == null ? new SocketOptions() : socketOptions,
                                     metricsEnabled ? new MetricsOptions(jmxEnabled) : null,
                                     queryOptions == null ? new QueryOptions() : queryOptions);
        }

        @Override
        public Collection<Host.StateListener> getInitialListeners() {
            return listeners == null ? Collections.<Host.StateListener>emptySet() : listeners;
        }

        /**
         * Builds the cluster with the configured set of initial contact points
         * and policies.
         * <p>
         * This is a convenience method for {@code Cluster.buildFrom(this)}.
         *
         * @return the newly built Cluster instance.
         */
        public Cluster build() {
            return Cluster.buildFrom(this);
        }
    }

    private static ThreadFactory threadFactory(String nameFormat) {
        return new ThreadFactoryBuilder().setNameFormat(nameFormat).build();
    }

    static long timeSince(long startNanos, TimeUnit destUnit) {
        return destUnit.convert(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
    }

    private static String generateClusterName() {
        return "cluster" + CLUSTER_ID.incrementAndGet();
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

        final String clusterName;
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

        // An executor for tasks that might block some time, like creating new connection, but are generally not too critical.
        final ListeningExecutorService blockingTasksExecutor;

        final AtomicReference<CloseFuture> closeFuture = new AtomicReference<CloseFuture>();

        // All the queries that have been prepared (we keep them so we can re-prepared them when a node fail or a
        // new one join the cluster).
        // Note: we could move this down to the session level, but since prepared statement are global to a node,
        // this would yield a slightly less clear behavior.
        final ConcurrentMap<MD5Digest, PreparedStatement> preparedQueries = new MapMaker().weakValues().makeMap();

        final Set<Host.StateListener> listeners;
        final Set<LatencyTracker> trackers = new CopyOnWriteArraySet<LatencyTracker>();

        private Manager(String clusterName, List<InetAddress> contactPoints, Configuration configuration, Collection<Host.StateListener> listeners) {
            logger.debug("Starting new cluster with contact points " + contactPoints);

            this.clusterName = clusterName == null ? generateClusterName() : clusterName;
            this.configuration = configuration;
            this.configuration.register(this);

            this.executor = makeExecutor(Runtime.getRuntime().availableProcessors(), "Cassandra Java Driver worker-%d");
            this.blockingTasksExecutor = makeExecutor(2, "Cassandra Java Driver blocking tasks worker-%d");

            this.metadata = new Metadata(this);
            this.contactPoints = contactPoints;
            this.connectionFactory = new Connection.Factory(this, configuration);
            this.controlConnection = new ControlConnection(this);

            this.metrics = configuration.getMetricsOptions() == null ? null : new Metrics(this);
            this.listeners = new CopyOnWriteArraySet<Host.StateListener>(listeners);
        }

        private void init() {
            if (!isInit.compareAndSet(false, true))
                return;

            for (InetAddress address : contactPoints) {
                // We don't want to signal -- call onAdd() -- because nothing is ready
                // yet (loadbalancing policy, control connection, ...). All we want is
                // create the Host object so we can initialize the loadBalancing policy.
                // But this also mean we should signal the external listeners manually.
                // Note: we mark the initial contact point as UP, because we have no prior
                // notion of their state and no real way to know until we connect to them
                // (since the node status is not exposed by C* in the System tables). This
                // may not be correct.
                Host host = metadata.add(address);
                if (host != null) {
                    host.setUp();
                    for (Host.StateListener listener : listeners)
                        listener.onAdd(host);
                }
            }

            loadBalancingPolicy().init(Cluster.this, metadata.allHosts());

            try {
                while (true) {
                    try {
                        controlConnection.connect();
                        if (connectionFactory.protocolVersion < 0)
                            connectionFactory.protocolVersion = 2;
                        return;
                    } catch (UnsupportedProtocolVersionException e) {
                        assert connectionFactory.protocolVersion < 1;
                        // For now, all C* version supports the protocol version 1
                        if (e.versionUnsupported <= 1)
                            throw new DriverInternalError("Got a node that don't even support the protocol version 1, this makes no sense", e);
                        logger.debug("{}: retrying with version {}", e.getMessage(), e.versionUnsupported - 1);
                        connectionFactory.protocolVersion = e.versionUnsupported - 1;
                    }
                }
            } catch (NoHostAvailableException e) {
                close();
                throw e;
            }
        }

        int protocolVersion() {
            return connectionFactory.protocolVersion;
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

        boolean isClosed() {
            return closeFuture.get() != null;
        }

        private CloseFuture close() {

            CloseFuture future = closeFuture.get();
            if (future != null)
                return future;

            logger.debug("Shutting down");

            // If we're shutting down, there is no point in waiting on scheduled reconnections, nor on notifications
            // delivery so we use shutdownNow
            reconnectionExecutor.shutdownNow();
            scheduledTasksExecutor.shutdownNow();

            // but for the worker executor, we want to let submitted tasks finish unless the shutdown is forced.
            executor.shutdown();

            // We also closes the metrics
            if (metrics != null)
                metrics.shutdown();

            // Then we shutdown all connections
            List<CloseFuture> futures = new ArrayList<CloseFuture>(sessions.size() + 1);
            futures.add(controlConnection.closeAsync());
            for (Session session : sessions)
                futures.add(session.closeAsync());

            future = new ClusterCloseFuture(futures);

            // The rest will happen asynchronously, when all connections are successfully closed
            return closeFuture.compareAndSet(null, future)
                 ? future
                 : closeFuture.get(); // We raced, it's ok, return the future that was actually set
        }

        void logUnsupportedVersionProtocol(Host host) {
            logger.warn("Detected added or restarted Cassandra host {} but ignoring it since it does not support the version 2 of the native "
                      + "protocol which is currently in use. If you want to force the use of the version 1 of the native protocol, use "
                      + "Cluster.Builder#usingProtocolVersion() when creating the Cluster instance.", host);
        }

        @Override
        public void onUp(final Host host) {
            logger.trace("Host {} is UP", host);

            if (isClosed())
                return;

            if (host.isUp())
                return;

            if (connectionFactory.protocolVersion == 2 && !supportsProtocolV2(host)) {
                logUnsupportedVersionProtocol(host);
                return;
            }

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
            } catch (UnsupportedProtocolVersionException e) {
                logUnsupportedVersionProtocol(host);
                return;
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

            // Only mark the node up once all session have re-added their pool (if the load-balancing
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

            if (isClosed())
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

                protected Connection tryReconnect() throws ConnectionException, InterruptedException, UnsupportedProtocolVersionException {
                    return connectionFactory.open(host);
                }

                protected void onReconnection(Connection connection) {
                    logger.debug("Successful reconnection to {}, setting host UP", host);
                    // Make sure we have up-to-date infos on that host before adding it (so we typically
                    // catch that an upgraded node uses a new cassandra version).
                    controlConnection.refreshNodeInfo(host);
                    if (isHostAddition)
                        onAdd(host);
                    else
                        onUp(host);
                }

                protected boolean onConnectionException(ConnectionException e, long nextDelayMs) {
                    if (logger.isDebugEnabled())
                        logger.debug("Failed reconnection to {} ({}), scheduling retry in {} milliseconds", host, e.getMessage(), nextDelayMs);
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
            if (isClosed())
                return;

            logger.info("New Cassandra host {} added", host);

            if (connectionFactory.protocolVersion == 2 && !supportsProtocolV2(host)) {
                logUnsupportedVersionProtocol(host);
                return;
            }

            try {
                prepareAllQueries(host);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                // Don't propagate because we don't want to prevent other listener to run
            } catch (UnsupportedProtocolVersionException e) {
                logUnsupportedVersionProtocol(host);
                return;
            }

            loadBalancingPolicy().onAdd(host);
            controlConnection.onAdd(host);

            List<ListenableFuture<Boolean>> futures = new ArrayList<ListenableFuture<Boolean>>(sessions.size());
            for (SessionManager s : sessions)
                futures.add(s.addOrRenewPool(host, true));

            // Only mark the node up once all session have added their pool (if the load-balancing
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
            if (isClosed())
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

        private boolean supportsProtocolV2(Host newHost) {
            return newHost.getCassandraVersion() == null || newHost.getCassandraVersion().getMajor() >= 2;
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
            if (preparedQueries.putIfAbsent(stmt.getPreparedId().id, stmt) != null)
                logger.warn("Re-preparing already prepared query {}. Please note that preparing the same query more than once is "
                          + "generally an anti-pattern and will likely affect performance. Consider preparing the statement only once.", stmt.getQueryString());
        }

        private void prepareAllQueries(Host host) throws InterruptedException, UnsupportedProtocolVersionException {
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
                            futures.add(connection.write(new Requests.Prepare(query)));
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
                    connection.closeAsync();
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
        public void refreshSchema(final Connection connection, final DefaultResultSetFuture future, final ResultSet rs, final String keyspace, final String table) {
            if (logger.isDebugEnabled())
                logger.debug("Refreshing schema for {}{}", keyspace == null ? "" : keyspace, table == null ? "" : '.' + table);

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
                        future.setResult(rs);
                    }
                }
            });
        }

        // Called when some message has been received but has been initiated from the server (streamId < 0).
        @Override
        public void handle(Message.Response response) {

            if (!(response instanceof Responses.Event)) {
                logger.error("Received an unexpected message from the server: {}", response);
                return;
            }

            final ProtocolEvent event = ((Responses.Event)response).event;

            logger.debug("Received event {}, scheduling delivery", response);

            // When handle is called, the current thread is a network I/O  thread, and we don't want to block
            // it (typically adding a new host will create the connection pool to the new node, which can take time)
            // Besides, up events are usually sent a bit too early (since they're triggered once gossip is up,
            // but that before the client-side server is up) so adds a 1 second delay in that case.
            // TODO: this delay is honestly quite random. We should do something on the C* side to fix that.
            scheduledTasksExecutor.schedule(new Runnable() {
                @Override
                public void run() {
                    switch (event.type) {
                        case TOPOLOGY_CHANGE:
                            ProtocolEvent.TopologyChange tpc = (ProtocolEvent.TopologyChange)event;
                            switch (tpc.change) {
                                case NEW_NODE:
                                    Host newHost = metadata.add(tpc.node.getAddress());
                                    if (newHost != null) {
                                        // Make sure we have up-to-date infos on that host before adding it (so we typically
                                        // catch that an upgraded node uses a new cassandra version).
                                        controlConnection.refreshNodeInfo(newHost);
                                        onAdd(newHost);
                                    }
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
                            ProtocolEvent.StatusChange stc = (ProtocolEvent.StatusChange)event;
                            switch (stc.status) {
                                case UP:
                                    Host hostUp = metadata.getHost(stc.node.getAddress());
                                    if (hostUp == null) {
                                        hostUp = metadata.add(stc.node.getAddress());
                                        // If hostUp is still null, it means we didn't knew about it the line before but
                                        // got beaten at adding it to the metadata by another thread. In that case, it's
                                        // fine to let the other thread win and ignore the notification here
                                        if (hostUp == null)
                                            return;

                                        // Make sure we have up-to-date infos on that host before adding it (so we typically
                                        // catch that an upgraded node uses a new cassandra version).
                                        controlConnection.refreshNodeInfo(hostUp);
                                        onAdd(hostUp);
                                    } else {
                                        // Make sure we have up-to-date infos on that host before adding it (so we typically
                                        // catch that an upgraded node uses a new cassandra version).
                                        controlConnection.refreshNodeInfo(hostUp);
                                        onUp(hostUp);
                                    }
                                    break;
                                case DOWN:
                                    // Note that there is a slight risk we can receive the event late and thus
                                    // mark the host down even though we already had reconnected successfully.
                                    // But it is unlikely, and don't have too much consequence since we'll try reconnecting
                                    // right away, so we favor the detection to make the Host.isUp method more reliable.
                                    Host hostDown = metadata.getHost(stc.node.getAddress());
                                    if (hostDown != null)
                                        onDown(hostDown);
                                    break;
                            }
                            break;
                        case SCHEMA_CHANGE:
                            ProtocolEvent.SchemaChange scc = (ProtocolEvent.SchemaChange)event;
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

        private int delayForEvent(ProtocolEvent event) {
            switch (event.type) {
                case TOPOLOGY_CHANGE:
                    // Could probably be 0 for REMOVED_NODE but it's inconsequential
                    return 1;
                case STATUS_CHANGE:
                    ProtocolEvent.StatusChange stc = (ProtocolEvent.StatusChange)event;
                    if (stc.status == ProtocolEvent.StatusChange.Status.UP)
                        return 1;
                    break;
            }
            return 0;
        }

        private class ClusterCloseFuture extends CloseFuture.Forwarding {

            ClusterCloseFuture(List<CloseFuture> futures) {
                super(futures);
            }

            @Override
            public CloseFuture force() {
                // The only ExecutorService we haven't forced yet is executor
                executor.shutdownNow();
                return super.force();
            }

            @Override
            protected void onFuturesDone() {
                /*
                 * When we reach this, all sessions should be shutdown. We've also started a shutdown
                 * of the thread pools used by this object. Remains 2 things before marking the shutdown
                 * as done:
                 *   1) we need to wait for the completion of the shutdown of the Cluster threads pools.
                 *   2) we need to shutdown the Connection.Factory, i.e. the executors used by Netty.
                 * But at least for 2), we must not do it on the current thread because that could be
                 * a netty worker, which we're going to shutdown. So creates some thread for that.
                 */
                (new Thread("Shutdown-checker") {
                    public void run() {
                        connectionFactory.shutdown();

                        // Just wait indefinitely on the the completion of the thread pools. Provided the user
                        // call force(), we'll never really block forever.
                        try {
                            reconnectionExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
                            scheduledTasksExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
                            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
                            set(null);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            setException(e);
                        }
                    }
                }).start();
            }
        }
    }
}
