/*
 *      Copyright (C) 2012-2015 DataStax Inc.
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
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Predicates;
import com.google.common.collect.*;
import com.google.common.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.core.exceptions.DriverInternalError;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.policies.*;

/**
 * Information and known state of a Cassandra cluster.
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

    @VisibleForTesting
    static final int NEW_NODE_DELAY_SECONDS = SystemProperties.getInt("com.datastax.driver.NEW_NODE_DELAY_SECONDS", 1);
    private static final int NON_BLOCKING_EXECUTOR_SIZE = SystemProperties.getInt("com.datastax.driver.NON_BLOCKING_EXECUTOR_SIZE",
                                                                                  Runtime.getRuntime().availableProcessors());

    private static final ResourceBundle driverProperties = ResourceBundle.getBundle("com.datastax.driver.core.Driver");

    // Some per-JVM number that allows to generate unique cluster names when
    // multiple Cluster instance are created in the same JVM.
    private static final AtomicInteger CLUSTER_ID = new AtomicInteger(0);

    private static final int DEFAULT_THREAD_KEEP_ALIVE = 30;

    private static final int NOTIF_LOCK_TIMEOUT_SECONDS = SystemProperties.getInt("com.datastax.driver.NOTIF_LOCK_TIMEOUT_SECONDS", 60);

    final Manager manager;

    /**
     * Constructs a new Cluster instance.
     * <p>
     * This constructor is mainly exposed so Cluster can be sub-classed as a means to make testing/mocking
     * easier or to "intercept" its method call. Most users shouldn't extend this class however and
     * should prefer either using the {@link #builder} or calling {@link #buildFrom} with a custom
     * Initializer.
     *
     * @param name the name to use for the cluster (this is not the Cassandra cluster name, see {@link #getClusterName}).
     * @param contactPoints the list of contact points to use for the new cluster.
     * @param configuration the configuration for the new cluster.
     */
    protected Cluster(String name, List<InetSocketAddress> contactPoints, Configuration configuration) {
        this(name, contactPoints, configuration, Collections.<Host.StateListener>emptySet());
    }

    /**
     * Constructs a new Cluster instance.
     * <p>
     * This constructor is mainly exposed so Cluster can be sub-classed as a means to make testing/mocking
     * easier or to "intercept" its method call. Most users shouldn't extend this class however and
     * should prefer using the {@link #builder}.
     *
     * @param initializer the initializer to use.
     * @see #buildFrom
     */
    protected Cluster(Initializer initializer) {
        this(initializer.getClusterName(),
             checkNotEmpty(initializer.getContactPoints()),
             initializer.getConfiguration(),
             initializer.getInitialListeners());
    }

    private static List<InetSocketAddress> checkNotEmpty(List<InetSocketAddress> contactPoints) {
        if (contactPoints.isEmpty())
            throw new IllegalArgumentException("Cannot build a cluster without contact points");
        return contactPoints;
    }

    private Cluster(String name, List<InetSocketAddress> contactPoints, Configuration configuration, Collection<Host.StateListener> listeners) {
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
     * Please note that this method only creates one control connection for
     * gathering cluster metadata. In particular, it doesn't create any connection pools.
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
     * @throws IllegalStateException if the Cluster was closed prior to calling
     * this method. This can occur either directly (through {@link #close()} or
     * {@link #closeAsync()}), or as a result of an error while initializing the
     * Cluster.
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
        return new Cluster(initializer);
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
     * Returns the current version of the driver.
     * <p>
     * This is intended for products that wrap or extend the driver, as a way to check
     * compatibility if end-users override the driver version in their application.
     *
     * @return the version.
     */
    public static String getDriverVersion() {
        return driverProperties.getString("driver.version");
    }

    /**
     * Creates a new session on this cluster but does not initialize it.
     * <p>
     * Because this method does not perform any initialization, it cannot fail.
     * The initialization of the session (the connection of the Session to the
     * Cassandra nodes) will occur if either the {@link Session#init} method is
     * called explicitly, or whenever the returned session object is used.
     * <p>
     * Once a session returned by this method gets initialized (see above), it
     * will be set to no keyspace. If you want to set such session to a
     * keyspace, you will have to explicitly execute a 'USE mykeyspace' query.
     * <p>
     * Note that if you do not particularly need to defer initialization, it is
     * simpler to use one of the {@code connect()} method of this class.
     *
     * @return a new, non-initialized session on this cluster.
     */
    public Session newSession() {
        checkNotClosed(manager);
        return manager.newSession();
    }

    /**
     * Creates a new session on this cluster and initialize it.
     * <p>
     * Note that this method will initialize the newly created session, trying
     * to connect to the Cassandra nodes before returning. If you only want to
     * create a Session object without initializing it right away, see
     * {@link #newSession}.
     *
     * @return a new session on this cluster sets to no keyspace.
     *
     * @throws NoHostAvailableException if the Cluster has not been initialized
     * yet ({@link #init} has not be called and this is the first connect call)
     * and no host amongst the contact points can be reached.
     * @throws AuthenticationException if an authentication error occurs while
     * contacting the initial contact points.
     * @throws IllegalStateException if the Cluster was closed prior to calling
     * this method. This can occur either directly (through {@link #close()} or
     * {@link #closeAsync()}), or as a result of an error while initializing the
     * Cluster.
     */
    public Session connect() {
        checkNotClosed(manager);
        init();
        Session session = manager.newSession();
        session.init();
        return session;
    }

    /**
     * Creates a new session on this cluster, initialize it and sets the
     * keyspace to the provided one.
     * <p>
     * Note that this method will initialize the newly created session, trying
     * to connect to the Cassandra nodes before returning. If you only want to
     * create a Session object without initializing it right away, see
     * {@link #newSession}.
     *
     * @param keyspace The name of the keyspace to use for the created
     * {@code Session}.
     * @return a new session on this cluster sets to keyspace
     * {@code keyspaceName}.
     *
     * @throws NoHostAvailableException if the Cluster has not been initialized
     * yet ({@link #init} has not be called and this is the first connect call)
     * and no host amongst the contact points can be reached, or if no host can
     * be contacted to set the {@code keyspace}.
     * @throws AuthenticationException if an authentication error occurs while
     * contacting the initial contact points.
     * @throws IllegalStateException if the Cluster was closed prior to calling
     * this method. This can occur either directly (through {@link #close()} or
     * {@link #closeAsync()}), or as a result of an error while initializing the
     * Cluster.
     */
    public Session connect(String keyspace) {
        long timeout = getConfiguration().getSocketOptions().getConnectTimeoutMillis();
        Session session = connect();
        try {
            try {
                ResultSetFuture future = session.executeAsync("USE " + keyspace);
                // Note: using the connection timeout isn't perfectly correct, we should probably change that someday
                Uninterruptibles.getUninterruptibly(future, timeout, TimeUnit.MILLISECONDS);
                return session;
            } catch (TimeoutException e) {
                throw new DriverInternalError(String.format("No responses after %d milliseconds while setting current keyspace. This should not happen, unless you have setup a very low connection timeout.", timeout));
            } catch (ExecutionException e) {
                throw DefaultResultSetFuture.extractCauseFromExecutionException(e);
            }
        } catch (RuntimeException e) {
            session.close();
            throw e;
        }
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
     * @throws IllegalStateException if the Cluster was closed prior to calling
     * this method. This can occur either directly (through {@link #close()} or
     * {@link #closeAsync()}), or as a result of an error while initializing the
     * Cluster.
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
        checkNotClosed(manager);
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
        checkNotClosed(manager);
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
        checkNotClosed(manager);
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
        checkNotClosed(manager);
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
        checkNotClosed(manager);
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
     * Whether this Cluster instance has been closed.
     * <p>
     * Note that this method returns true as soon as one of the close methods
     * ({@link #closeAsync} or {@link #close}) has been called, it does not guarantee
     * that the closing is done. If you want to guarantee that the closing is done,
     * you can call {@code close()} and wait until it returns (or call the get method
     * on {@code closeAsync()} with a very short timeout and check this doesn't timeout).
     *
     * @return {@code true} if this Cluster instance has been closed, {@code false}
     * otherwise.
     */
    public boolean isClosed() {
        return manager.closeFuture.get() != null;
    }

    private static void checkNotClosed(Manager manager) {
        if (manager.isClosed())
            throw new IllegalStateException("Can't use this cluster instance because it was previously closed");
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
        public List<InetSocketAddress> getContactPoints();

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
        private final List<InetSocketAddress> addresses = new ArrayList<InetSocketAddress>();
        private final List<InetAddress> rawAddresses = new ArrayList<InetAddress>();
        private int port = ProtocolOptions.DEFAULT_PORT;
        private int maxSchemaAgreementWaitSeconds = ProtocolOptions.DEFAULT_MAX_SCHEMA_AGREEMENT_WAIT_SECONDS;
        private int protocolVersion = -1;
        private AuthProvider authProvider = AuthProvider.NONE;

        private LoadBalancingPolicy loadBalancingPolicy;
        private ReconnectionPolicy reconnectionPolicy;
        private RetryPolicy retryPolicy;
        private AddressTranslater addressTranslater;
        private SpeculativeExecutionPolicy speculativeExecutionPolicy;

        private ProtocolOptions.Compression compression = ProtocolOptions.Compression.NONE;
        private SSLOptions sslOptions = null;
        private boolean metricsEnabled = true;
        private boolean jmxEnabled = true;

        private PoolingOptions poolingOptions;
        private SocketOptions socketOptions;
        private QueryOptions queryOptions;

        private NettyOptions nettyOptions = NettyOptions.DEFAULT_INSTANCE;

        private Collection<Host.StateListener> listeners;


        @Override
        public String getClusterName() {
            return clusterName;
        }

        @Override
        public List<InetSocketAddress> getContactPoints() {
            if (rawAddresses.isEmpty())
                return addresses;

            List<InetSocketAddress> allAddresses = new ArrayList<InetSocketAddress>(addresses);
            for (InetAddress address : rawAddresses)
                allAddresses.add(new InetSocketAddress(address, port));
            return allAddresses;
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
         * Sets the maximum time to wait for schema agreement before returning from a DDL query.
         * <p>
         * If not set through this method, the default value (10 seconds) will be used.
         *
         * @param maxSchemaAgreementWaitSeconds the new value to set.
         * @return this Builder.
         *
         * @throws IllegalStateException if the provided value is zero or less.
         */
        public Builder withMaxSchemaAgreementWaitSeconds(int maxSchemaAgreementWaitSeconds) {
            if (maxSchemaAgreementWaitSeconds <= 0)
                throw new IllegalArgumentException("Max schema agreement wait must be greater than zero");

            this.maxSchemaAgreementWaitSeconds = maxSchemaAgreementWaitSeconds;
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
         * <p>
         * Note that by default (that is, unless you use the {@link #withLoadBalancingPolicy})
         * method of this builder), the first succesfully contacted host will be use
         * to define the local data-center for the client. If follows that if you are
         * running Cassandra in a  multiple data-center setting, it is a good idea to
         * only provided contact points that are in the same datacenter than the client,
         * or to provide manually the load balancing policy that suits your need.
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
            // We explicitely check for nulls because InetAdress.getByName() will happily
            // accept it and use localhost (while a null here almost likely mean a user error,
            // not "connect to localhost")
            if (address == null)
                throw new NullPointerException();

            try {
                this.rawAddresses.add(InetAddress.getByName(address));
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
         * Adds a contact point - or many if it host resolves to multiple <code>InetAddress</code>s (A records).
         * <p>
         *
         * If the host name points to a dns records with multiple a-records, all InetAddresses
         * returned will be used. Make sure that all resulting <code>InetAddress</code>s returned
         * points to the same cluster and datacenter.
         * <p>
         * See {@link Builder#addContactPoint} for more details on contact
         * points and thrown exceptions
         *
         * @param address address of the nodes to look up InetAddresses from to add as contact points.
         * @return this Builder.
         *
         *
         * @see Builder#addContactPoint
         */
        public Builder addContactPoints(String address) {
            // We explicitely check for nulls because InetAdress.getByName() will happily
            // accept it and use localhost (while a null here almost likely mean a user error,
            // not "connect to localhost")
            if (address == null)
                throw new NullPointerException();

            try {
                addContactPoints(InetAddress.getAllByName(address));
            } catch (UnknownHostException e) {
                throw new IllegalArgumentException(e.getMessage());
            }
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
            Collections.addAll(this.rawAddresses, addresses);
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
        public Builder addContactPoints(Collection<InetAddress> addresses) {
            this.rawAddresses.addAll(addresses);
            return this;
        }

        /**
         * Adds contact points.
         * <p>
         * See {@link Builder#addContactPoint} for more details on contact
         * points. Contrarily to other {@code addContactPoints} methods, this method
         * allow to provide a different port for each contact points. Since Cassandra
         * nodes must always all listen on the same port, this is rarelly what you
         * want and most use should prefer other {@code addContactPoints} methods to
         * this one. However, this can be useful if the Cassandra nodes are behind
         * a router and are not accessed directly. Note that if you are in this
         * situtation (Cassandra nodes are behind a router, not directly accessible),
         * you almost surely want to provide a specific {@code AddressTranslater}
         * (through {@link #withAddressTranslater}) to translate actual Cassandra node
         * addresses to the addresses the driver should use, otherwise the driver
         * will not be able to auto-detect new nodes (and will generally not function
         * optimally).
         *
         * @param addresses addresses of the nodes to add as contact point
         * @return this Builder
         *
         * @see Builder#addContactPoint
         */
        public Builder addContactPointsWithPorts(Collection<InetSocketAddress> addresses) {
            this.addresses.addAll(addresses);
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
         * Configures the address translater to use for the new cluster.
         * <p>
         * See {@link AddressTranslater} for more detail on address translation,
         * but the default translater, {@link IdentityTranslater}, should be
         * correct in most cases. If unsure, stick to the default.
         *
         * @param translater the translater to use.
         * @return this Builder.
         */
        public Builder withAddressTranslater(AddressTranslater translater) {
            this.addressTranslater = translater;
            return this;
        }

        /**
         * Configures the speculative execution policy to use for the new cluster.
         * <p>
         * If no policy is set through this method, {@link Policies#defaultSpeculativeExecutionPolicy()}
         * will be used instead.
         *
         * @param policy the policy to use.
         * @return this Builder.
         */
        public Builder withSpeculativeExecutionPolicy(SpeculativeExecutionPolicy policy) {
            this.speculativeExecutionPolicy = policy;
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
         * Set the {@link NettyOptions} to use for the newly created Cluster.
         * <p>
         * If no Netty options are set through this method, {@link NettyOptions#DEFAULT_INSTANCE}
         * will be used as a default value, which means that no customization will be applied.
         *
         * @param nettyOptions the {@link NettyOptions} to use.
         * @return this builder.
         */
        public Builder withNettyOptions(NettyOptions nettyOptions) {
            this.nettyOptions = nettyOptions;
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
                Objects.firstNonNull(reconnectionPolicy, Policies.defaultReconnectionPolicy()),
                Objects.firstNonNull(retryPolicy, Policies.defaultRetryPolicy()),
                Objects.firstNonNull(addressTranslater, Policies.defaultAddressTranslater()),
                Objects.firstNonNull(speculativeExecutionPolicy, Policies.defaultSpeculativeExecutionPolicy())
            );
            return new Configuration(policies,
                                     new ProtocolOptions(port, protocolVersion, maxSchemaAgreementWaitSeconds, sslOptions, authProvider).setCompression(compression),
                                     poolingOptions == null ? new PoolingOptions() : poolingOptions,
                                     socketOptions == null ? new SocketOptions() : socketOptions,
                                     metricsEnabled ? new MetricsOptions(jmxEnabled) : null,
                                     queryOptions == null ? new QueryOptions() : queryOptions,
                                     nettyOptions);
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

    static long timeSince(long startNanos, TimeUnit destUnit) {
        return destUnit.convert(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
    }

    private static String generateClusterName() {
        return "cluster" + CLUSTER_ID.incrementAndGet();
    }

    /**
     * The sessions and hosts managed by this a Cluster instance.
     * <p>
     * Note: the reason we create a Manager object separate from Cluster is
     * that Manager is not publicly visible. For instance, we wouldn't want
     * user to be able to call the {@link #onUp} and {@link #onDown} methods.
     */
    class Manager implements Connection.DefaultResponseHandler {

        // TODO options
        private static final int MAX_PENDING_REFRESH_NODE_LIST_REQUESTS = 20;
        private static final int MAX_PENDING_REFRESH_NODE_REQUESTS = 20;
        private static final int MAX_PENDING_REFRESH_SCHEMA_REQUESTS = 20;
        private static final int REFRESH_NODE_LIST_INTERVAL_MILLIS = 1000;
        private static final int REFRESH_NODE_INTERVAL_MILLIS = 1000;
        private static final int REFRESH_SCHEMA_INTERVAL_MILLIS = 1000;

        final String clusterName;
        private boolean isInit;
        private volatile boolean isFullyInit;

        // Initial contacts point
        final List<InetSocketAddress> contactPoints;
        final Set<SessionManager> sessions = new CopyOnWriteArraySet<SessionManager>();

        Metadata metadata;
        final Configuration configuration;
        Metrics metrics;

        Connection.Factory connectionFactory;
        ControlConnection controlConnection;

        final ConvictionPolicy.Factory convictionPolicyFactory = new ConvictionPolicy.Simple.Factory();

        ScheduledThreadPoolExecutor reconnectionExecutor;
        ScheduledThreadPoolExecutor scheduledTasksExecutor;

        // Executor used for tasks that shouldn't be executed on an IO thread. Used for short-lived, generally non-blocking tasks
        ListeningExecutorService executor;

        // Work Queue used by executor.
        LinkedBlockingQueue<Runnable> executorQueue;

        // An executor for tasks that might block some time, like creating new connection, but are generally not too critical.
        ListeningExecutorService blockingExecutor;

        // Work Queue used by blockingExecutor.
        LinkedBlockingQueue<Runnable> blockingExecutorQueue;

        ConnectionReaper reaper;

        final AtomicReference<CloseFuture> closeFuture = new AtomicReference<CloseFuture>();

        // All the queries that have been prepared (we keep them so we can re-prepared them when a node fail or a
        // new one join the cluster).
        // Note: we could move this down to the session level, but since prepared statement are global to a node,
        // this would yield a slightly less clear behavior.
        ConcurrentMap<MD5Digest, PreparedStatement> preparedQueries;

        final Set<Host.StateListener> listeners;
        final Set<LatencyTracker> trackers = new CopyOnWriteArraySet<LatencyTracker>();

        // counters for pending refresh requests
        private final AtomicInteger pendingRefreshNodeListRequests = new AtomicInteger(0);
        private final AtomicInteger pendingRefreshSchemaRequests = new AtomicInteger(0);
        private final AtomicInteger pendingRefreshNodeRequests = new AtomicInteger(0);

        // pending requests to be coalesced / executed
        private final BlockingQueue<SchemaRefreshRequest> schemaRefreshRequests = new LinkedBlockingQueue<SchemaRefreshRequest>();
        private final BlockingQueue<NodeRefreshRequest> nodeRefreshRequests = new DelayQueue<NodeRefreshRequest>();

        // locks used to flush enqueued requests
        private ReadWriteLock schemaRefreshLock = new ReentrantReadWriteLock();
        private ReadWriteLock nodeListRefreshLock = new ReentrantReadWriteLock();
        private ReadWriteLock nodeRefreshLock = new ReentrantReadWriteLock();

        private Manager(String clusterName, List<InetSocketAddress> contactPoints, Configuration configuration, Collection<Host.StateListener> listeners) {
            this.clusterName = clusterName == null ? generateClusterName() : clusterName;
            this.configuration = configuration;
            this.contactPoints = contactPoints;
            this.listeners = new CopyOnWriteArraySet<Host.StateListener>(listeners);
        }

        // Initialization is not too performance intensive and in practice there shouldn't be contention
        // on it so synchronized is good enough.
        synchronized void init() {
            checkNotClosed(this);
            if (isInit)
                return;
            isInit = true;

            logger.debug("Starting new cluster with contact points " + contactPoints);

            this.configuration.register(this);

            this.executorQueue = new LinkedBlockingQueue<Runnable>();
            this.executor = makeExecutor(NON_BLOCKING_EXECUTOR_SIZE, "worker", executorQueue);
            this.blockingExecutorQueue = new LinkedBlockingQueue<Runnable>();
            this.blockingExecutor = makeExecutor(2, "blocking-task-worker", blockingExecutorQueue);
            this.reconnectionExecutor = new ScheduledThreadPoolExecutor(2, threadFactory("reconnection"));
            this.scheduledTasksExecutor = new ScheduledThreadPoolExecutor(2, threadFactory("scheduled-task-worker"));

            this.reaper = new ConnectionReaper(this);
            this.metadata = new Metadata(this);
            this.connectionFactory = new Connection.Factory(this, configuration);
            this.controlConnection = new ControlConnection(this);
            this.metrics = configuration.getMetricsOptions() == null ? null : new Metrics(this);
            this.preparedQueries = new MapMaker().weakValues().makeMap();

            this.scheduledTasksExecutor.scheduleWithFixedDelay(new CleanupIdleConnectionsTask(), 10, 10, TimeUnit.SECONDS);

            this.scheduledTasksExecutor.scheduleWithFixedDelay(new RefreshNodeListTimerTask(), REFRESH_NODE_LIST_INTERVAL_MILLIS, REFRESH_NODE_LIST_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
            this.scheduledTasksExecutor.scheduleWithFixedDelay(new RefreshSchemaTimerTask(), REFRESH_SCHEMA_INTERVAL_MILLIS, REFRESH_SCHEMA_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
            this.scheduledTasksExecutor.scheduleWithFixedDelay(new RefreshNodeTimerTask(), REFRESH_NODE_INTERVAL_MILLIS, REFRESH_NODE_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);

            for (InetSocketAddress address : contactPoints) {
                // We don't want to signal -- call onAdd() -- because nothing is ready
                // yet (loadbalancing policy, control connection, ...). All we want is
                // create the Host object so we can initialize the control connection.
                metadata.add(address);
            }

            try {
                while (true) {
                    try {
                        // At this stage, metadata.allHosts() only contains the contact points, that's what we want to pass to LBP.init().
                        // But the control connection will initialize first and discover more hosts, so make a copy.
                        Set<Host> contactPointHosts = Sets.newHashSet(metadata.allHosts());

                        controlConnection.connect();
                        if (connectionFactory.protocolVersion < 0)
                            connectionFactory.protocolVersion = 2;

                        // The control connection can mark hosts down if it failed to connect to them, separate them
                        Set<Host> downContactPointHosts = Sets.newHashSet();
                        for (Host host : contactPointHosts)
                            if (host.state == Host.State.DOWN)
                                downContactPointHosts.add(host);
                        contactPointHosts.removeAll(downContactPointHosts);

                        // Now that the control connection is ready, we have all the information we need about the nodes (datacenter,
                        // rack...) to initialize the load balancing policy
                        loadBalancingPolicy().init(Cluster.this, contactPointHosts);
                        speculativeRetryPolicy().init(Cluster.this);

                        for (Host host : downContactPointHosts) {
                            loadBalancingPolicy().onDown(host);
                            for (Host.StateListener listener : listeners)
                                listener.onDown(host);
                        }

                        for (Host host : metadata.allHosts()) {
                            // If the host is down at this stage, it's a contact point that the control connection failed to reach.
                            // Reconnection attempts are already scheduled, and the LBP and listeners have been notified above.
                            if (host.state == Host.State.DOWN) continue;

                            // Otherwise, we want to do the equivalent of onAdd(). But since we know for sure that no sessions or prepared
                            // statements exist at this point, we can skip some of the steps (plus this avoids scheduling concurrent pool
                            // creations if a session is created right after this method returns).
                            logger.info("New Cassandra host {} added", host);

                            if (connectionFactory.protocolVersion == 2 && !supportsProtocolV2(host)) {
                                logUnsupportedVersionProtocol(host);
                                continue;
                            }

                            if (!contactPointHosts.contains(host))
                                loadBalancingPolicy().onAdd(host);

                            host.setUp();

                            for (Host.StateListener listener : listeners)
                                listener.onAdd(host);
                        }
                        isFullyInit = true;

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

        ThreadFactory threadFactory(String name) {
            return new ThreadFactoryBuilder().setNameFormat(clusterName + "-" + name + "-%d").build();
        }

        private ListeningExecutorService makeExecutor(int threads, String name, LinkedBlockingQueue<Runnable> workQueue) {
            ThreadPoolExecutor executor = new ThreadPoolExecutor(threads,
                threads,
                DEFAULT_THREAD_KEEP_ALIVE,
                TimeUnit.SECONDS,
                workQueue,
                threadFactory(name));

            executor.allowCoreThreadTimeOut(true);
            return MoreExecutors.listeningDecorator(executor);
        }

        Cluster getCluster() {
            return Cluster.this;
        }

        LoadBalancingPolicy loadBalancingPolicy() {
            return configuration.getPolicies().getLoadBalancingPolicy();
        }

        SpeculativeExecutionPolicy speculativeRetryPolicy() {
            return configuration.getPolicies().getSpeculativeExecutionPolicy();
        }

        ReconnectionPolicy reconnectionPolicy() {
            return configuration.getPolicies().getReconnectionPolicy();
        }

        InetSocketAddress translateAddress(InetAddress address) {
            InetSocketAddress sa = new InetSocketAddress(address, connectionFactory.getPort());
            InetSocketAddress translated = configuration.getPolicies().getAddressTranslater().translate(sa);
            return translated == null ? sa : translated;
        }

        private Session newSession() {
            SessionManager session = new SessionManager(Cluster.this);
            sessions.add(session);
            return session;
        }

        boolean removeSession(Session session) {
            return sessions.remove(session);
        }

        void reportLatency(Host host, Statement statement, Exception exception, long latencyNanos) {
            for (LatencyTracker tracker : trackers) {
                tracker.update(host, statement, exception, latencyNanos);
            }
        }

        boolean isClosed() {
            return closeFuture.get() != null;
        }

        private CloseFuture close() {

            CloseFuture future = closeFuture.get();
            if (future != null)
                return future;

            if (isInit) {
                logger.debug("Shutting down");

                // If we're shutting down, there is no point in waiting on scheduled reconnections, nor on notifications
                // delivery or blocking tasks so we use shutdownNow
                shutdownNow(reconnectionExecutor);
                shutdownNow(scheduledTasksExecutor);
                shutdownNow(blockingExecutor);

                // but for the worker executor, we want to let submitted tasks finish unless the shutdown is forced.
                executor.shutdown();

                // We also close the metrics
                if (metrics != null)
                    metrics.shutdown();

                // And the load balancing policy
                LoadBalancingPolicy loadBalancingPolicy = loadBalancingPolicy();
                if (loadBalancingPolicy instanceof CloseableLoadBalancingPolicy)
                    ((CloseableLoadBalancingPolicy)loadBalancingPolicy).close();

                speculativeRetryPolicy().close();

                AddressTranslater translater = configuration.getPolicies().getAddressTranslater();
                if (translater instanceof CloseableAddressTranslater)
                    ((CloseableAddressTranslater)translater).close();

                // Then we shutdown all connections
                List<CloseFuture> futures = new ArrayList<CloseFuture>(sessions.size() + 1);
                futures.add(controlConnection.closeAsync());
                for (Session session : sessions)
                    futures.add(session.closeAsync());

                future = new ClusterCloseFuture(futures);
                // The rest will happen asynchronously, when all connections are successfully closed
            } else {
                future = CloseFuture.immediateFuture();
            }

            return closeFuture.compareAndSet(null, future)
                ? future
                : closeFuture.get(); // We raced, it's ok, return the future that was actually set
        }

        private void shutdownNow(ExecutorService executor) {
            List<Runnable> pendingTasks = executor.shutdownNow();
            // If some tasks were submitted to this executor but not yet commenced, make sure the corresponding futures complete
            for (Runnable pendingTask : pendingTasks) {
                if (pendingTask instanceof FutureTask<?>)
                    ((FutureTask<?>)pendingTask).cancel(false);
            }
        }

        void logUnsupportedVersionProtocol(Host host) {
            logger.warn("Detected added or restarted Cassandra host {} but ignoring it since it does not support the version 2 of the native "
                      + "protocol which is currently in use. If you want to force the use of the version 1 of the native protocol, use "
                      + "Cluster.Builder#usingProtocolVersion() when creating the Cluster instance.", host);
        }

        void logClusterNameMismatch(Host host, String expectedClusterName, String actualClusterName) {
            logger.warn("Detected added or restarted Cassandra host {} but ignoring it since its cluster name '{}' does not match the one "
                    + "currently known ({})",
                host, actualClusterName, expectedClusterName);
        }

        public ListenableFuture<?> triggerOnUp(final Host host) {
            return executor.submit(new ExceptionCatchingRunnable() {
                @Override
                public void runMayThrow() throws InterruptedException, ExecutionException {
                    onUp(host, null);
                }
            });
        }

        // Use triggerOnUp unless you're sure you want to run this on the current thread.
        private void onUp(final Host host, Connection reusedConnection) throws InterruptedException, ExecutionException {
            logger.debug("Host {} is UP", host);

            if (isClosed())
                return;

            if (connectionFactory.protocolVersion == 2 && !supportsProtocolV2(host)) {
                logUnsupportedVersionProtocol(host);
                return;
            }

            try {

                boolean locked = host.notificationsLock.tryLock(NOTIF_LOCK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                if (!locked) {
                    logger.warn("Could not acquire notifications lock within {} seconds, ignoring UP notification for {}", NOTIF_LOCK_TIMEOUT_SECONDS, host);
                    return;
                }
                try {

                    // We don't want to use the public Host.isUp() as this would make us skip the rest for suspected hosts
                    if (host.state == Host.State.UP)
                        return;

                    // If there is a reconnection attempt scheduled for that node, cancel it
                    Future<?> scheduledAttempt = host.reconnectionAttempt.getAndSet(null);
                    if (scheduledAttempt != null) {
                        logger.debug("Cancelling reconnection attempt since node is UP");
                        scheduledAttempt.cancel(false);
                    }

                    try {
                        reusedConnection = prepareAllQueries(host, reusedConnection);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        // Don't propagate because we don't want to prevent other listener to run
                    } catch (UnsupportedProtocolVersionException e) {
                        logUnsupportedVersionProtocol(host);
                        return;
                    } catch (ClusterNameMismatchException e) {
                        logClusterNameMismatch(host, e.expectedClusterName, e.actualClusterName);
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

                    logger.trace("Adding/renewing host pools for newly UP host {}", host);

                    List<ListenableFuture<Boolean>> futures = Lists.newArrayListWithCapacity(sessions.size());
                    for (SessionManager s : sessions)
                        futures.add(s.forceRenewPool(host, reusedConnection));

                    try {
                        // Only mark the node up once all session have re-added their pool (if the load-balancing
                        // policy says it should), so that Host.isUp() don't return true before we're reconnected
                        // to the node.
                        List<Boolean> poolCreationResults = Futures.allAsList(futures).get();

                        // If any of the creation failed, they will have signaled a connection failure
                        // which will trigger a reconnection to the node. So don't bother marking UP.
                        if (Iterables.any(poolCreationResults, Predicates.equalTo(false))) {
                            logger.debug("Connection pool cannot be created, not marking {} UP", host);
                            return;
                        }

                        host.setUp();

                        for (Host.StateListener listener : listeners)
                            listener.onUp(host);

                    } catch (ExecutionException e) {
                        Throwable t = e.getCause();
                        // That future is not really supposed to throw unexpected exceptions
                        if (!(t instanceof InterruptedException) && !(t instanceof CancellationException))
                            logger.error("Unexpected error while marking node UP: while this shouldn't happen, this shouldn't be critical", t);
                    }

                    // Now, check if there isn't pools to create/remove following the addition.
                    // We do that now only so that it's not called before we've set the node up.
                    for (SessionManager s : sessions)
                        s.updateCreatedPools();

                } finally {
                    host.notificationsLock.unlock();
                }

            } finally {
                if (reusedConnection != null && !reusedConnection.hasPool())
                    reusedConnection.closeAsync();
            }
        }

        public ListenableFuture<?> triggerOnDown(final Host host, boolean startReconnection) {
            return triggerOnDown(host, false, startReconnection);
        }

        public ListenableFuture<?> triggerOnDown(final Host host, final boolean isHostAddition, final boolean startReconnection) {
            return executor.submit(new ExceptionCatchingRunnable() {
                @Override
                public void runMayThrow() throws InterruptedException, ExecutionException {
                    onDown(host, isHostAddition, startReconnection);
                }
            });
        }

        // Use triggerOnDown unless you're sure you want to run this on the current thread.
        private void onDown(final Host host, final boolean isHostAddition, boolean startReconnection) throws InterruptedException, ExecutionException {
            logger.debug("Host {} is DOWN", host);

            if (isClosed())
                return;

            boolean locked = host.notificationsLock.tryLock(NOTIF_LOCK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (!locked) {
                logger.warn("Could not acquire notifications lock within {} seconds, ignoring DOWN notification for {}", NOTIF_LOCK_TIMEOUT_SECONDS, host);
                return;
            }
            try {

                // Note: we don't want to skip that method if !host.isUp() because we set isUp
                // late in onUp, and so we can rely on isUp if there is an error during onUp.
                // But if there is a reconnection attempt in progress already, then we know
                // we've already gone through that method since the last successful onUp(), so
                // we're good skipping it.
                if (host.reconnectionAttempt.get() != null) {
                    logger.debug("Aborting onDown because a reconnection is running on DOWN host {}", host);
                    return;
                }

                // Remember if we care about this node at all. We must call this before
                // we've signalled the load balancing policy, since most policy will always
                // IGNORE down nodes anyway.
                HostDistance distance = loadBalancingPolicy().distance(host);

                boolean wasUp = host.isUp();
                host.setDown();

                loadBalancingPolicy().onDown(host);
                controlConnection.onDown(host);
                for (SessionManager s : sessions)
                    s.onDown(host);

                // Contrarily to other actions of that method, there is no reason to notify listeners
                // unless the host was UP at the beginning of this function since even if a onUp fail
                // mid-method, listeners won't have been notified of the UP.
                if (wasUp) {
                    for (Host.StateListener listener : listeners)
                        listener.onDown(host);
                }

                // Don't start a reconnection if we ignore the node anyway (JAVA-314)
                if (distance == HostDistance.IGNORED || !startReconnection)
                    return;

                logger.debug("{} is down, scheduling connection retries", host);
                startPeriodicReconnectionAttempt(host, isHostAddition);
            } finally {
                host.notificationsLock.unlock();
            }
        }

        void startPeriodicReconnectionAttempt(final Host host, final boolean isHostAddition) {
            new AbstractReconnectionHandler(reconnectionExecutor, reconnectionPolicy().newSchedule(), host.reconnectionAttempt) {

                protected Connection tryReconnect() throws ConnectionException, InterruptedException, UnsupportedProtocolVersionException, ClusterNameMismatchException {
                    return connectionFactory.open(host);
                }

                protected void onReconnection(Connection connection) {
                    // Make sure we have up-to-date infos on that host before adding it (so we typically
                    // catch that an upgraded node uses a new cassandra version).
                    if (controlConnection.refreshNodeInfo(host)) {
                        logger.debug("Successful reconnection to {}, setting host UP", host);
                        try {
                            if (isHostAddition)
                                onAdd(host, connection);
                            else
                                onUp(host, connection);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        } catch (Exception e) {
                            logger.error("Unexpected error while setting node up", e);
                        }
                    } else {
                        logger.debug("Not enough info for {}, ignoring host", host);
                        connection.closeAsync();
                    }
                }

                protected boolean onConnectionException(ConnectionException e, long nextDelayMs) {
                    if (logger.isDebugEnabled())
                        logger.debug("Failed reconnection to {} ({}), scheduling retry in {} milliseconds", host, e.getMessage(), nextDelayMs);
                    return true;
                }

                protected boolean onUnknownException(Exception e, long nextDelayMs) {
                    logger.error(String.format("Unknown error during reconnection to %s, scheduling retry in %d milliseconds", host, nextDelayMs), e);
                    return true;
                }

                protected boolean onAuthenticationException(AuthenticationException e, long nextDelayMs) {
                    logger.error(String.format("Authentication error during reconnection to %s, scheduling retry in %d milliseconds", host, nextDelayMs), e);
                    return true;
                }

            }.start();
        }

        void startSingleReconnectionAttempt(final Host host) {
            if (isClosed() || host.isUp())
                return;

            logger.debug("Scheduling one-time reconnection to {}", host);

            // Setting an initial delay of 0 to start immediately, and all the exception handlers return false to prevent further attempts
            new AbstractReconnectionHandler(reconnectionExecutor, reconnectionPolicy().newSchedule(), host.reconnectionAttempt, 0) {

                protected Connection tryReconnect() throws ConnectionException, InterruptedException, UnsupportedProtocolVersionException, ClusterNameMismatchException {
                    return connectionFactory.open(host);
                }

                protected void onReconnection(Connection connection) {
                    // Make sure we have up-to-date infos on that host before adding it (so we typically
                    // catch that an upgraded node uses a new cassandra version).
                    if (controlConnection.refreshNodeInfo(host)) {
                        logger.debug("Successful reconnection to {}, setting host UP", host);
                        try {
                            onUp(host, connection);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        } catch (Exception e) {
                            logger.error("Unexpected error while setting node up", e);
                        }
                    } else {
                        logger.debug("Not enough info for {}, ignoring host", host);
                        connection.closeAsync();
                    }
                }

                protected boolean onConnectionException(ConnectionException e, long nextDelayMs) {
                    if (logger.isDebugEnabled())
                        logger.debug("Failed one-time reconnection to {} ({})", host, e.getMessage());
                    return false;
                }

                protected boolean onUnknownException(Exception e, long nextDelayMs) {
                    logger.error(String.format("Unknown error during one-time reconnection to %s", host), e);
                    return false;
                }

                protected boolean onAuthenticationException(AuthenticationException e, long nextDelayMs) {
                    logger.error(String.format("Authentication error during one-time reconnection to %s", host), e);
                    return false;
                }
            }.start();
        }

        public ListenableFuture<?> triggerOnAdd(final Host host) {
            return executor.submit(new ExceptionCatchingRunnable() {
                @Override
                public void runMayThrow() throws InterruptedException, ExecutionException {
                    onAdd(host, null);
                }
            });
        }

        // Use triggerOnAdd unless you're sure you want to run this on the current thread.
        private void onAdd(final Host host, Connection reusedConnection) throws InterruptedException, ExecutionException {
            if (isClosed())
                return;

            logger.info("New Cassandra host {} added", host);

            if (connectionFactory.protocolVersion == 2 && !supportsProtocolV2(host)) {
                logUnsupportedVersionProtocol(host);
                return;
            }

            try {

                boolean locked = host.notificationsLock.tryLock(NOTIF_LOCK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                if (!locked) {
                    logger.warn("Could not acquire notifications lock within {} seconds, ignoring ADD notification for {}", NOTIF_LOCK_TIMEOUT_SECONDS, host);
                    return;
                }
                try {

                    // Adds to the load balancing first and foremost, as doing so might change the decision
                    // it will make for distance() on that node (not likely but we leave that possibility).
                    // This does mean the policy may start returning that node for query plan, but as long
                    // as no pools have been created (below) this will be ignored by RequestHandler so it's fine.
                    loadBalancingPolicy().onAdd(host);

                    // Next, if the host should be ignored, well, ignore it.
                    if (loadBalancingPolicy().distance(host) == HostDistance.IGNORED) {
                        // We still mark the node UP though as it should be (and notifiy the listeners).
                        // We'll mark it down if we have  a notification anyway and we've documented that especially
                        // for IGNORED hosts, the isUp() method was a best effort guess
                        host.setUp();
                        for (Host.StateListener listener : listeners)
                            listener.onAdd(host);
                        return;
                    }

                    try {
                        reusedConnection = prepareAllQueries(host, reusedConnection);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        // Don't propagate because we don't want to prevent other listener to run
                    } catch (UnsupportedProtocolVersionException e) {
                        logUnsupportedVersionProtocol(host);
                        return;
                    } catch (ClusterNameMismatchException e) {
                        logClusterNameMismatch(host, e.expectedClusterName, e.actualClusterName);
                        return;
                    }

                    controlConnection.onAdd(host);

                    List<ListenableFuture<Boolean>> futures = Lists.newArrayListWithCapacity(sessions.size());
                    for (SessionManager s : sessions)
                        futures.add(s.maybeAddPool(host, reusedConnection));

                    try {
                        // Only mark the node up once all session have added their pool (if the load-balancing
                        // policy says it should), so that Host.isUp() don't return true before we're reconnected
                        // to the node.
                        List<Boolean> poolCreationResults = Futures.allAsList(futures).get();

                        // If any of the creation failed, they will have signaled a connection failure
                        // which will trigger a reconnection to the node. So don't bother marking UP.
                        if (Iterables.any(poolCreationResults, Predicates.equalTo(false))) {
                            logger.debug("Connection pool cannot be created, not marking {} UP", host);
                            return;
                        }

                        host.setUp();

                        for (Host.StateListener listener : listeners)
                            listener.onAdd(host);

                    } catch (ExecutionException e) {
                        Throwable t = e.getCause();
                        // That future is not really supposed to throw unexpected exceptions
                        if (!(t instanceof InterruptedException) && !(t instanceof CancellationException))
                            logger.error("Unexpected error while adding node: while this shouldn't happen, this shouldn't be critical", t);
                    }

                    // Now, check if there isn't pools to create/remove following the addition.
                    // We do that now only so that it's not called before we've set the node up.
                    for (SessionManager s : sessions)
                        s.updateCreatedPools();

                } finally {
                    host.notificationsLock.unlock();
                }

            } finally {
                if (reusedConnection != null && !reusedConnection.hasPool())
                    reusedConnection.closeAsync();
            }
        }

        public ListenableFuture<?> triggerOnRemove(final Host host) {
            return executor.submit(new ExceptionCatchingRunnable() {
                @Override
                public void runMayThrow() throws InterruptedException, ExecutionException {
                    onRemove(host);
                }
            });
        }

        // Use triggerOnRemove unless you're sure you want to run this on the current thread.
        private void onRemove(Host host) throws InterruptedException, ExecutionException {
            if (isClosed())
                return;

            boolean locked = host.notificationsLock.tryLock(NOTIF_LOCK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (!locked) {
                logger.warn("Could not acquire notifications lock within {} seconds, ignoring REMOVE notification for {}", NOTIF_LOCK_TIMEOUT_SECONDS, host);
                return;
            }
            try {

                host.setDown();

                logger.debug("Removing host {}", host);
                loadBalancingPolicy().onRemove(host);
                controlConnection.onRemove(host);
                for (SessionManager s : sessions)
                    s.onRemove(host);

                for (Host.StateListener listener : listeners)
                    listener.onRemove(host);
            } finally {
                host.notificationsLock.unlock();
            }
        }

        public boolean signalConnectionFailure(Host host, ConnectionException exception, boolean isHostAddition) {
            // Don't signal failure until we've fully initialized the controlConnection as this might mess up with
            // the protocol detection
            if (!isFullyInit || isClosed())
                return true;

            boolean isDown = host.signalConnectionFailure(exception);
            if (isDown)
                triggerOnDown(host, isHostAddition, true);
            return isDown;
        }

        private boolean supportsProtocolV2(Host newHost) {
            return newHost.getCassandraVersion() == null || newHost.getCassandraVersion().getMajor() >= 2;
        }

        public void removeHost(Host host, boolean isInitialConnection) {
            if (host == null)
                return;

            if (metadata.remove(host)) {
                if (isInitialConnection) {
                    logger.warn("You listed {} in your contact points, but it could not be reached at startup", host);
                } else {
                    logger.info("Cassandra host {} removed", host);
                    triggerOnRemove(host);
                }
            }
        }

        public void ensurePoolsSizing() {
            for (SessionManager session : sessions) {
                for (HostConnectionPool pool : session.pools.values())
                    pool.ensureCoreConnections();
            }
        }

        public PreparedStatement addPrepared(PreparedStatement stmt) {
            PreparedStatement previous = preparedQueries.putIfAbsent(stmt.getPreparedId().id, stmt);
            if (previous != null) {
                logger.warn("Re-preparing already prepared query {}. Please note that preparing the same query more than once is "
                          + "generally an anti-pattern and will likely affect performance. Consider preparing the statement only once.", stmt.getQueryString());

                // The one object in the cache will get GCed once it's not referenced by the client anymore since we use a weak reference.
                // So we need to make sure that the instance we do return to the user is the one that is in the cache.
                return previous;
            }
            return stmt;
        }

        /**
         * @param reusedConnection an existing connection (from a reconnection attempt) that we want to
         *                         reuse to prepare the statements (might be null).
         * @return a connection that the rest of the initialization process can use (it will be made part
         *         of a connection pool). Can be reusedConnection, or one that was open in the method.
         */
        private Connection prepareAllQueries(Host host, Connection reusedConnection) throws InterruptedException, UnsupportedProtocolVersionException, ClusterNameMismatchException {
            if (preparedQueries.isEmpty())
                return reusedConnection;

            logger.debug("Preparing {} prepared queries on newly up node {}", preparedQueries.size(), host);
            Connection connection = null;
            try {
                connection = (reusedConnection == null)
                    ? connectionFactory.open(host)
                    : reusedConnection;

                try {
                    ControlConnection.waitForSchemaAgreement(connection, this);
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

                for (String keyspace : perKeyspace.keySet()) {
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

                return connection;
            } catch (ConnectionException e) {
                // Ignore, not a big deal
                if (connection != null)
                    connection.closeAsync();
                return null;
            } catch (AuthenticationException e) {
                // That's a bad news, but ignore at this point
                if (connection != null)
                    connection.closeAsync();
                return null;
            } catch (BusyConnectionException e) {
                // Ignore, not a big deal
                // In theory the problem is transient so the connection could be reused later, but if the core pool size is 1
                // it's better to close this one so that we start with a fresh connection.
                if (connection != null)
                    connection.closeAsync();
                return null;
            }
        }

        void submitNodeListRefresh() {
            logger.trace("Submitting node list and token map refresh");
            nodeListRefreshLock.readLock().lock();
            try {
                int count = pendingRefreshNodeListRequests.incrementAndGet();
                if (count == MAX_PENDING_REFRESH_NODE_LIST_REQUESTS) {
                    executor.submit(new ExceptionCatchingRunnable() {
                        @Override
                        public void runMayThrow() throws InterruptedException, ExecutionException {
                            flushNodeListRequests();
                        }
                    });
                }
            } finally {
                nodeListRefreshLock.readLock().unlock();
            }
        }

        void submitSchemaRefresh(String keyspace, String table) {
            logger.trace("Submitting schema refresh (ks {}, table {})", keyspace, table);
            SchemaRefreshRequest request = new SchemaRefreshRequest(keyspace, table);
            schemaRefreshLock.readLock().lock();
            try {
                schemaRefreshRequests.add(request);
                int count = pendingRefreshSchemaRequests.incrementAndGet();
                if (count == MAX_PENDING_REFRESH_SCHEMA_REQUESTS) {
                    executor.submit(new ExceptionCatchingRunnable() {
                        @Override
                        public void runMayThrow() throws InterruptedException, ExecutionException {
                            flushSchemaRefreshRequests();
                        }
                    });
                }
            } finally {
                schemaRefreshLock.readLock().unlock();
            }
        }

        private void submitNodeRefresh(Host host, Host.State state, boolean delay) {
            logger.trace("Submitting node refresh (host {}, state {})", host, state);
            NodeRefreshRequest request = new NodeRefreshRequest(host, state, delay);
            nodeRefreshLock.readLock().lock();
            try {
                nodeRefreshRequests.add(request);
                int count = pendingRefreshNodeRequests.incrementAndGet();
                if (count == MAX_PENDING_REFRESH_NODE_REQUESTS) {
                    executor.submit(new ExceptionCatchingRunnable() {
                        @Override
                        public void runMayThrow() throws InterruptedException, ExecutionException {
                            flushNodeRefreshRequests();
                        }
                    });
                }
            } finally {
                nodeRefreshLock.readLock().unlock();
            }
        }

        private void flushNodeListRequests() {
            nodeListRefreshLock.writeLock().lock();
            try {
                int count = pendingRefreshNodeListRequests.get();
                if(count > 0 && pendingRefreshNodeListRequests.compareAndSet(count, 0)) {
                    controlConnection.refreshNodeListAndTokenMap();
                }
            } finally {
                nodeListRefreshLock.writeLock().unlock();
            }
        }

        private void flushSchemaRefreshRequests() throws InterruptedException {
            schemaRefreshLock.writeLock().lock();
            try {
                final Set<SchemaRefreshRequest> requests = new HashSet<SchemaRefreshRequest>();
                int drained = schemaRefreshRequests.drainTo(requests);
                if (!requests.isEmpty()) {
                    logger.trace("Flushing {} schema refresh requests", requests.size());
                    SchemaRefreshRequest coalesced = null;
                    for (SchemaRefreshRequest request : requests) {
                        coalesced = coalesced == null ? request : coalesced.coalesce(request);
                    }
                    logger.trace("Coalesced schema refresh request: {}", coalesced);
                    controlConnection.refreshSchema(coalesced.keyspace, coalesced.table);
                    int current = pendingRefreshSchemaRequests.get();
                    pendingRefreshSchemaRequests.set(current - drained);
                }
            } finally {
                schemaRefreshLock.writeLock().unlock();
            }
        }

        private void flushNodeRefreshRequests() throws ExecutionException, InterruptedException {
            nodeRefreshLock.writeLock().lock();
            try {
                Set<NodeRefreshRequest> requests = new HashSet<NodeRefreshRequest>();
                int drained = nodeRefreshRequests.drainTo(requests);
                if (!requests.isEmpty()) {
                    logger.trace("Flushing {} node refresh requests", requests.size());
                    Map<Host, Host.State> hosts = new HashMap<Host, Host.State>();
                    for (NodeRefreshRequest req : requests) {
                        hosts.put(req.host, req.state);
                    }
                    for (final Entry<Host, Host.State> entry : hosts.entrySet()) {
                        // Make sure we have up-to-date infos on that host before adding it (so we typically
                        // catch that an upgraded node uses a new cassandra version).
                        Host host = entry.getKey();
                        Host.State state = entry.getValue();
                        if (controlConnection.refreshNodeInfo(host)) {
                            switch (state) {
                                case UP:
                                    onUp(host, null);
                                    break;
                                case ADDED:
                                    onAdd(host, null);
                                    break;
                            }
                        } else {
                            logger.debug("Not enough info for {}, ignoring host", host);
                        }
                    }
                    int current = pendingRefreshNodeRequests.get();
                    pendingRefreshNodeRequests.set(current - drained);
                }
            } finally {
                nodeRefreshLock.writeLock().unlock();
            }
        }

        // refresh the schema using the provided connection, and notice the future with the provided resultset once done
        public void refreshSchemaAndSignal(final Connection connection, final DefaultResultSetFuture future, final ResultSet rs, final String keyspace, final String table) {
            if (logger.isDebugEnabled())
                logger.debug("Refreshing schema for {}{}", keyspace == null ? "" : keyspace, table == null ? "" : '.' + table);

            maybeRefreshSchemaAndSignal(connection, future, rs, keyspace, table);
        }

        public void waitForSchemaAgreementAndSignal(final Connection connection, final DefaultResultSetFuture future, final ResultSet rs) {
            maybeRefreshSchemaAndSignal(connection, future, rs, null, null);
        }

        private void maybeRefreshSchemaAndSignal(final Connection connection, final DefaultResultSetFuture future, final ResultSet rs, final String keyspace, final String table) {
            final boolean refreshSchema = (keyspace != null); // if false, only wait for schema agreement

            executor.submit(new Runnable() {
                @Override
                public void run() {
                    boolean schemaInAgreement = false;
                    try {
                        // Before refreshing the schema, wait for schema agreement so
                        // that querying a table just after having created it don't fail.
                        schemaInAgreement = ControlConnection.waitForSchemaAgreement(connection, Manager.this);
                        if (!schemaInAgreement)
                            logger.warn("No schema agreement from live replicas after {} s. The schema may not be up to date on some nodes.", configuration.getProtocolOptions().getMaxSchemaAgreementWaitSeconds());
                        if (refreshSchema) {
                            // forcefully trigger a synchronous schema refresh
                            submitSchemaRefresh(keyspace, table);
                            flushSchemaRefreshRequests();
                        }
                    } catch (Exception e) {
                        if (refreshSchema) {
                            logger.error("Error during schema refresh ({}). The schema from Cluster.getMetadata() might appear stale. Asynchronously submitting job to fix.", e.getMessage());
                            submitSchemaRefresh(keyspace, table);
                        } else {
                            logger.warn("Error while waiting for schema agreement", e);
                        }
                    } finally {
                        // Always sets the result, but remember if we reached schema agreement
                        rs.getExecutionInfo().setSchemaInAgreement(schemaInAgreement);
                        future.setResult(rs);
                    }
                }
            });
        }

        // Called when some message has been received but has been initiated from the server (streamId < 0).
        // This is called on an I/O thread, so all blocking operation must be done on an executor.
        @Override
        public void handle(Message.Response response) {

            if (!(response instanceof Responses.Event)) {
                logger.error("Received an unexpected message from the server: {}", response);
                return;
            }

            final ProtocolEvent event = ((Responses.Event)response).event;

            logger.debug("Received event {}, scheduling delivery", response);

            switch (event.type) {
                case TOPOLOGY_CHANGE:
                    ProtocolEvent.TopologyChange tpc = (ProtocolEvent.TopologyChange)event;
                    InetSocketAddress tpAddr = translateAddress(tpc.node.getAddress());
                    switch (tpc.change) {
                        case NEW_NODE:
                            final Host newHost = metadata.add(tpAddr);
                            if (newHost != null) {
                                // Cassandra tends to send notifications for new/up nodes a bit early (it is triggered once
                                // gossip is up, but that is before the client-side server is up), so we add a delay
                                // (otherwise the connection will likely fail and have to be retry which is wasteful). This
                                // probably should be fixed C* side, after which we'll be able to remove this.
                                submitNodeRefresh(newHost, Host.State.ADDED, true);
                            }
                            break;
                        case REMOVED_NODE:
                            removeHost(metadata.getHost(tpAddr), false);
                            break;
                        case MOVED_NODE:
                            submitNodeListRefresh();
                            break;
                    }
                    break;
                case STATUS_CHANGE:
                    ProtocolEvent.StatusChange stc = (ProtocolEvent.StatusChange)event;
                    InetSocketAddress stAddr = translateAddress(stc.node.getAddress());
                    switch (stc.status) {
                        case UP:
                            final Host hostUp = metadata.getHost(stAddr);
                            if (hostUp == null) {
                                final Host h = metadata.add(stAddr);
                                // If hostUp is still null, it means we didn't knew about it the line before but
                                // got beaten at adding it to the metadata by another thread. In that case, it's
                                // fine to let the other thread win and ignore the notification here
                                if (h == null)
                                    return;

                                // See NEW_NODE above
                                submitNodeRefresh(h, Host.State.ADDED, true);
                            } else {
                                submitNodeRefresh(hostUp, Host.State.UP, false);
                            }
                            break;
                        case DOWN:
                            // Note that there is a slight risk we can receive the event late and thus
                            // mark the host down even though we already had reconnected successfully.
                            // But it is unlikely, and don't have too much consequence since we'll try reconnecting
                            // right away, so we favor the detection to make the Host.isUp method more reliable.
                            Host hostDown = metadata.getHost(stAddr);
                            if (hostDown != null)
                                triggerOnDown(hostDown, true);
                            break;
                    }
                    break;
                case SCHEMA_CHANGE:
                    ProtocolEvent.SchemaChange scc = (ProtocolEvent.SchemaChange)event;
                    switch (scc.change) {
                        case CREATED:
                            if (scc.table.isEmpty())
                                submitSchemaRefresh(scc.keyspace, null);
                            else
                                submitSchemaRefresh(scc.keyspace, scc.table);
                            break;
                        case DROPPED:
                            if (scc.table.isEmpty())
                                manager.metadata.removeKeyspace(scc.keyspace);
                            else {
                                KeyspaceMetadata keyspace = manager.metadata.getKeyspaceInternal(scc.keyspace);
                                if (keyspace == null)
                                    logger.warn("Received a DROPPED notification for {}.{}, but this keyspace is unknown in our metadata",
                                        scc.keyspace, scc.table);
                                else
                                    keyspace.removeTable(scc.table);
                            }
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

        void refreshConnectedHosts() {
            // Deal first with the control connection: if it's connected to a node that is not LOCAL, try
            // reconnecting (thus letting the loadBalancingPolicy pick a better node)
            Host ccHost = controlConnection.connectedHost();
            if (ccHost == null || loadBalancingPolicy().distance(ccHost) != HostDistance.LOCAL)
                controlConnection.reconnect();

            for (SessionManager s : sessions)
                s.updateCreatedPools();
        }

        void refreshConnectedHost(Host host) {
            // Deal with the control connection if it was using this host
            Host ccHost = controlConnection.connectedHost();
            if (ccHost == null || ccHost.equals(host) && loadBalancingPolicy().distance(ccHost) != HostDistance.LOCAL)
                controlConnection.reconnect();

            for (SessionManager s : sessions)
                s.updateCreatedPools(host);
        }

        private class ClusterCloseFuture extends CloseFuture.Forwarding {

            ClusterCloseFuture(List<CloseFuture> futures) {
                super(futures);
            }

            @Override
            public CloseFuture force() {
                // The only ExecutorService we haven't forced yet is executor
                shutdownNow(executor);
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
                        // Just wait indefinitely on the the completion of the thread pools. Provided the user
                        // call force(), we'll never really block forever.
                        try {
                            reconnectionExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
                            scheduledTasksExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
                            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
                            blockingExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);

                            // Some of the jobs on the executors can be doing query stuff, so close the
                            // connectionFactory at the very last
                            connectionFactory.shutdown();

                            reaper.shutdown();

                            set(null);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            setException(e);
                        }
                    }
                }).start();
            }
        }

        private class CleanupIdleConnectionsTask implements Runnable {
            @Override public void run() {
                try {
                    long now = System.currentTimeMillis();
                    for (SessionManager session : sessions) {
                        session.cleanupIdleConnections(now);
                    }
                } catch (Exception e) {
                    logger.warn("Error while trashing idle connections", e);
                }
            }
        }

        private class SchemaRefreshRequest {

            private final String keyspace;

            private final String table;

            SchemaRefreshRequest(String keyspace, String table) {
                this.keyspace = keyspace;
                this.table = table;
            }

            SchemaRefreshRequest coalesce(SchemaRefreshRequest that) {
                if(this.keyspace == null || that.keyspace == null)
                    return new SchemaRefreshRequest(null, null);
                if(!this.keyspace.equals(that.keyspace))
                    return new SchemaRefreshRequest(null, null);
                if(this.table == null || that.table == null)
                    return new SchemaRefreshRequest(keyspace, null);
                if(!this.table.equals(that.table))
                    return new SchemaRefreshRequest(keyspace, null);
                return this;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o)
                    return true;
                if (o == null || getClass() != o.getClass())
                    return false;
                SchemaRefreshRequest that = (SchemaRefreshRequest)o;
                return Objects.equal(keyspace, that.keyspace) &&
                    Objects.equal(table, that.table);
            }

            @Override
            public int hashCode() {
                return Objects.hashCode(keyspace, table);
            }

            @Override
            public String toString() {
                return Objects.toStringHelper(this)
                    .add("keyspace", keyspace)
                    .add("table", table)
                    .toString();
            }
        }

        private class NodeRefreshRequest implements Delayed {

            private final Host host;

            private final Host.State state;

            private final long created;

            private NodeRefreshRequest(Host host, Host.State state, boolean delay) {
                this.host = host;
                this.state = state;
                this.created = delay ? System.currentTimeMillis() : -1;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o)
                    return true;
                if (o == null || getClass() != o.getClass())
                    return false;
                NodeRefreshRequest that = (NodeRefreshRequest)o;
                return Objects.equal(host, that.host) &&
                    Objects.equal(state, that.state);
            }

            @Override
            public int hashCode() {
                return Objects.hashCode(host, state);
            }

            @Override
            public String toString() {
                return Objects.toStringHelper(this)
                    .add("host", host)
                    .add("state", state)
                    .toString();
            }

            @Override
            public long getDelay(TimeUnit unit) {
                if(created == -1)
                    return -1;
                long now = System.currentTimeMillis();
                long delayMillis = created + NEW_NODE_DELAY_SECONDS * 1000 - now;
                return unit.convert(delayMillis, TimeUnit.MILLISECONDS);
            }

            @Override
            public int compareTo(Delayed o) {
                return Long.compare(this.created, ((NodeRefreshRequest)o).created);
            }
        }

        private class RefreshNodeListTimerTask extends ExceptionCatchingRunnable {
            @Override
            public void runMayThrow() {
                flushNodeListRequests();
            }
        }

        private class RefreshSchemaTimerTask extends ExceptionCatchingRunnable {
            @Override
            public void runMayThrow() throws InterruptedException {
                flushSchemaRefreshRequests();
            }
        }

        private class RefreshNodeTimerTask extends ExceptionCatchingRunnable {
            @Override
            public void runMayThrow() throws InterruptedException, ExecutionException {
                flushNodeRefreshRequests();
            }
        }

    }

    /**
     * Periodically ensures that closed connections are properly terminated once they have no more pending requests.
     *
     * This is normally done when the connection errors out, or when the last request is processed; this class acts as
     * a last-effort protection since unterminated connections can lead to deadlocks. If it terminates a connection,
     * this indicates a bug; warnings are logged so that this can be reported.
     *
     * @see Connection#tryTerminate(boolean)
     */
    static class ConnectionReaper {
        private static final int INTERVAL_MS = 15000;

        private final ScheduledExecutorService executor;
        @VisibleForTesting
        final Map<Connection, Long> connections = new ConcurrentHashMap<Connection, Long>();

        private volatile boolean shutdown;

        private final Runnable reaperTask = new Runnable() {
            @Override
            public void run() {
                long now = System.currentTimeMillis();
                Iterator<Entry<Connection, Long>> iterator = connections.entrySet().iterator();
                while (iterator.hasNext()) {
                    Entry<Connection, Long> entry = iterator.next();
                    Connection connection = entry.getKey();
                    Long terminateTime = entry.getValue();
                    if (terminateTime <= now) {
                        boolean terminated = connection.tryTerminate(true);
                        if (terminated)
                            iterator.remove();
                    }
                }
            }
        };

        ConnectionReaper(Cluster.Manager manager) {
            executor = Executors.newScheduledThreadPool(1, manager.threadFactory("connection-reaper"));
            executor.scheduleWithFixedDelay(reaperTask, INTERVAL_MS, INTERVAL_MS, TimeUnit.MILLISECONDS);
        }

        void register(Connection connection, long terminateTime) {
            if (shutdown) {
                // This should not happen since the reaper is shut down after all sessions.
                logger.warn("Connection registered after reaper shutdown: {}", connection);
                connection.tryTerminate(true);
            } else {
                connections.put(connection, terminateTime);
            }
        }

        void shutdown() {
            shutdown = true;
            // Force shutdown to avoid waiting for the interval, and run the task manually one last time
            executor.shutdownNow();
            reaperTask.run();
        }
    }
}
