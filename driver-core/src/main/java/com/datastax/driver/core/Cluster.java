/*
 * Copyright (C) 2012-2017 DataStax Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.driver.core;

import com.datastax.driver.core.exceptions.*;
import com.datastax.driver.core.policies.*;
import com.datastax.driver.core.utils.MoreFutures;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Functions;
import com.google.common.base.Predicates;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.*;
import com.google.common.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.datastax.driver.core.SchemaElement.KEYSPACE;

/**
 * Information and known state of a Cassandra cluster.
 * <p/>
 * This is the main entry point of the driver. A simple example of access to a
 * Cassandra cluster would be:
 * <pre>
 *   Cluster cluster = Cluster.builder().addContactPoint("192.168.0.1").build();
 *   Session session = cluster.connect("db1");
 *
 *   for (Row row : session.execute("SELECT * FROM table1"))
 *       // do something ...
 * </pre>
 * <p/>
 * A cluster object maintains a permanent connection to one of the cluster nodes
 * which it uses solely to maintain information on the state and current
 * topology of the cluster. Using the connection, the driver will discover all
 * the nodes currently in the cluster as well as new nodes joining the cluster
 * subsequently.
 */
public class Cluster implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(Cluster.class);

    static {
        // Force initialization to fail fast if there is an issue detecting the version
        GuavaCompatibility.init();
    }

    @VisibleForTesting
    static final int NEW_NODE_DELAY_SECONDS = SystemProperties.getInt("com.datastax.driver.NEW_NODE_DELAY_SECONDS", 1);

    private static final ResourceBundle driverProperties = ResourceBundle.getBundle("com.datastax.driver.core.Driver");

    // Some per-JVM number that allows to generate unique cluster names when
    // multiple Cluster instance are created in the same JVM.
    private static final AtomicInteger CLUSTER_ID = new AtomicInteger(0);

    private static final int NOTIF_LOCK_TIMEOUT_SECONDS = SystemProperties.getInt("com.datastax.driver.NOTIF_LOCK_TIMEOUT_SECONDS", 60);

    final Manager manager;

    /**
     * Constructs a new Cluster instance.
     * <p/>
     * This constructor is mainly exposed so Cluster can be sub-classed as a means to make testing/mocking
     * easier or to "intercept" its method call. Most users shouldn't extend this class however and
     * should prefer either using the {@link #builder} or calling {@link #buildFrom} with a custom
     * Initializer.
     *
     * @param name          the name to use for the cluster (this is not the Cassandra cluster name, see {@link #getClusterName}).
     * @param contactPoints the list of contact points to use for the new cluster.
     * @param configuration the configuration for the new cluster.
     */
    protected Cluster(String name, List<InetSocketAddress> contactPoints, Configuration configuration) {
        this(name, contactPoints, configuration, Collections.<Host.StateListener>emptySet());
    }

    /**
     * Constructs a new Cluster instance.
     * <p/>
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
     * <p/>
     * This method creates an initial connection to one of the contact points
     * used to construct the {@code Cluster} instance. That connection is then
     * used to populate the cluster {@link Metadata}.
     * <p/>
     * Calling this method is optional in the sense that any call to one of the
     * {@code connect} methods of this object will automatically trigger a call
     * to this method beforehand. It is thus only useful to call this method if
     * for some reason you want to populate the metadata (or test that at least
     * one contact point can be reached) without creating a first {@code
     * Session}.
     * <p/>
     * Please note that this method only creates one control connection for
     * gathering cluster metadata. In particular, it doesn't create any connection pools.
     * Those are created when a new {@code Session} is created through
     * {@code connect}.
     * <p/>
     * This method has no effect if the cluster is already initialized.
     *
     * @return this {@code Cluster} object.
     * @throws NoHostAvailableException if no host amongst the contact points
     *                                  can be reached.
     * @throws AuthenticationException  if an authentication error occurs
     *                                  while contacting the initial contact points.
     * @throws IllegalStateException    if the Cluster was closed prior to calling
     *                                  this method. This can occur either directly (through {@link #close()} or
     *                                  {@link #closeAsync()}), or as a result of an error while initializing the
     *                                  Cluster.
     */
    public Cluster init() {
        this.manager.init();
        return this;
    }

    /**
     * Build a new cluster based on the provided initializer.
     * <p/>
     * Note that for building a cluster pragmatically, Cluster.Builder
     * provides a slightly less verbose shortcut with {@link Builder#build}.
     * <p/>
     * Also note that that all the contact points provided by {@code
     * initializer} must share the same port.
     *
     * @param initializer the Cluster.Initializer to use
     * @return the newly created Cluster instance
     * @throws IllegalArgumentException if the list of contact points provided
     *                                  by {@code initializer} is empty or if not all those contact points have the same port.
     */
    public static Cluster buildFrom(Initializer initializer) {
        return new Cluster(initializer);
    }

    /**
     * Creates a new {@link Cluster.Builder} instance.
     * <p/>
     * This is a convenience method for {@code new Cluster.Builder()}.
     *
     * @return the new cluster builder.
     */
    public static Cluster.Builder builder() {
        return new Cluster.Builder();
    }

    /**
     * Returns the current version of the driver.
     * <p/>
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
     * <p/>
     * Because this method does not perform any initialization, it cannot fail.
     * The initialization of the session (the connection of the Session to the
     * Cassandra nodes) will occur if either the {@link Session#init} method is
     * called explicitly, or whenever the returned session object is used.
     * <p/>
     * Once a session returned by this method gets initialized (see above), it
     * will be set to no keyspace. If you want to set such session to a
     * keyspace, you will have to explicitly execute a 'USE mykeyspace' query.
     * <p/>
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
     * <p/>
     * Note that this method will initialize the newly created session, trying
     * to connect to the Cassandra nodes before returning. If you only want to
     * create a Session object without initializing it right away, see
     * {@link #newSession}.
     *
     * @return a new session on this cluster sets to no keyspace.
     * @throws NoHostAvailableException if the Cluster has not been initialized
     *                                  yet ({@link #init} has not be called and this is the first connect call)
     *                                  and no host amongst the contact points can be reached.
     * @throws AuthenticationException  if an authentication error occurs while
     *                                  contacting the initial contact points.
     * @throws IllegalStateException    if the Cluster was closed prior to calling
     *                                  this method. This can occur either directly (through {@link #close()} or
     *                                  {@link #closeAsync()}), or as a result of an error while initializing the
     *                                  Cluster.
     */
    public Session connect() {
        try {
            return Uninterruptibles.getUninterruptibly(connectAsync());
        } catch (ExecutionException e) {
            throw DriverThrowables.propagateCause(e);
        }
    }

    /**
     * Creates a new session on this cluster, initialize it and sets the
     * keyspace to the provided one.
     * <p/>
     * Note that this method will initialize the newly created session, trying
     * to connect to the Cassandra nodes before returning. If you only want to
     * create a Session object without initializing it right away, see
     * {@link #newSession}.
     *
     * @param keyspace The name of the keyspace to use for the created
     *                 {@code Session}.
     * @return a new session on this cluster sets to keyspace
     * {@code keyspaceName}.
     * @throws NoHostAvailableException if the Cluster has not been initialized
     *                                  yet ({@link #init} has not be called and this is the first connect call)
     *                                  and no host amongst the contact points can be reached, or if no host can
     *                                  be contacted to set the {@code keyspace}.
     * @throws AuthenticationException  if an authentication error occurs while
     *                                  contacting the initial contact points.
     * @throws InvalidQueryException    if the keyspace does not exist.
     * @throws IllegalStateException    if the Cluster was closed prior to calling
     *                                  this method. This can occur either directly (through {@link #close()} or
     *                                  {@link #closeAsync()}), or as a result of an error while initializing the
     *                                  Cluster.
     */
    public Session connect(String keyspace) {
        try {
            return Uninterruptibles.getUninterruptibly(connectAsync(keyspace));
        } catch (ExecutionException e) {
            throw DriverThrowables.propagateCause(e);
        }
    }

    /**
     * Creates a new session on this cluster and initializes it asynchronously.
     * <p/>
     * This will also initialize the {@code Cluster} if needed; note that cluster
     * initialization happens synchronously on the thread that called this method.
     * Therefore it is recommended to initialize the cluster at application
     * startup, and not rely on this method to do it.
     *
     * @return a future that will complete when the session is fully initialized.
     * @throws NoHostAvailableException if the Cluster has not been initialized
     *                                  yet ({@link #init} has not been called and this is the first connect call)
     *                                  and no host amongst the contact points can be reached.
     * @throws IllegalStateException    if the Cluster was closed prior to calling
     *                                  this method. This can occur either directly (through {@link #close()} or
     *                                  {@link #closeAsync()}), or as a result of an error while initializing the
     *                                  Cluster.
     * @see #connect()
     */
    public ListenableFuture<Session> connectAsync() {
        return connectAsync(null);
    }

    /**
     * Creates a new session on this cluster, and initializes it to the given
     * keyspace asynchronously.
     * <p/>
     * This will also initialize the {@code Cluster} if needed; note that cluster
     * initialization happens synchronously on the thread that called this method.
     * Therefore it is recommended to initialize the cluster at application
     * startup, and not rely on this method to do it.
     *
     * @param keyspace The name of the keyspace to use for the created
     *                 {@code Session}.
     * @return a future that will complete when the session is fully initialized.
     * @throws NoHostAvailableException if the Cluster has not been initialized
     *                                  yet ({@link #init} has not been called and this is the first connect call)
     *                                  and no host amongst the contact points can be reached.
     * @throws IllegalStateException    if the Cluster was closed prior to calling
     *                                  this method. This can occur either directly (through {@link #close()} or
     *                                  {@link #closeAsync()}), or as a result of an error while initializing the
     *                                  Cluster.
     */
    public ListenableFuture<Session> connectAsync(final String keyspace) {
        checkNotClosed(manager);
        init();
        final Session session = manager.newSession();
        ListenableFuture<Session> sessionInitialized = session.initAsync();
        if (keyspace == null) {
            return sessionInitialized;
        } else {
            final String useQuery = "USE " + keyspace;
            ListenableFuture<ResultSet> keyspaceSet = GuavaCompatibility.INSTANCE.transformAsync(sessionInitialized, new AsyncFunction<Session, ResultSet>() {
                @Override
                public ListenableFuture<ResultSet> apply(Session session) throws Exception {
                    return session.executeAsync(useQuery);
                }
            });
            ListenableFuture<ResultSet> withErrorHandling = GuavaCompatibility.INSTANCE.withFallback(keyspaceSet, new AsyncFunction<Throwable, ResultSet>() {
                @Override
                public ListenableFuture<ResultSet> apply(Throwable t) throws Exception {
                    session.closeAsync();
                    if (t instanceof SyntaxError) {
                        // Give a more explicit message, because it's probably caused by a bad keyspace name
                        SyntaxError e = (SyntaxError) t;
                        t = new SyntaxError(e.getAddress(),
                                String.format("Error executing \"%s\" (%s). Check that your keyspace name is valid",
                                        useQuery, e.getMessage()));
                    }
                    throw Throwables.propagate(t);
                }
            });
            return Futures.transform(withErrorHandling, Functions.constant(session));
        }
    }

    /**
     * The name of this cluster object.
     * <p/>
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
     * <p/>
     * This includes the known nodes with their status as seen by the driver,
     * as well as the schema definitions. Since this return metadata on the
     * connected cluster, this method may trigger the creation of a connection
     * if none has been established yet (neither {@code init()} nor {@code connect()}
     * has been called yet).
     *
     * @return the cluster metadata.
     * @throws NoHostAvailableException if the Cluster has not been initialized yet
     *                                  and no host amongst the contact points can be reached.
     * @throws AuthenticationException  if an authentication error occurs
     *                                  while contacting the initial contact points.
     * @throws IllegalStateException    if the Cluster was closed prior to calling
     *                                  this method. This can occur either directly (through {@link #close()} or
     *                                  {@link #closeAsync()}), or as a result of an error while initializing the
     *                                  Cluster.
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
     * @return the cluster metrics, or {@code null} if this cluster has not yet been {@link #init() initialized}, or if
     * metrics collection has been disabled (that is if {@link Configuration#getMetricsOptions} returns {@code null}).
     */
    public Metrics getMetrics() {
        checkNotClosed(manager);
        return manager.metrics;
    }

    /**
     * Registers the provided listener to be notified on hosts
     * up/down/added/removed events.
     * <p/>
     * Registering the same listener multiple times is a no-op.
     * <p/>
     * This method should be used to register additional listeners
     * on an already-initialized cluster.
     * To add listeners to a cluster object prior to its initialization,
     * use {@link Builder#withInitialListeners(Collection)}.
     * Calling this method on a non-initialized cluster
     * will result in the listener being
     * {@link com.datastax.driver.core.Host.StateListener#onRegister(Cluster) notified}
     * twice of cluster registration: once inside this method, and once at cluster initialization.
     *
     * @param listener the new {@link Host.StateListener} to register.
     * @return this {@code Cluster} object;
     */
    public Cluster register(Host.StateListener listener) {
        checkNotClosed(manager);
        boolean added = manager.listeners.add(listener);
        if (added)
            listener.onRegister(this);
        return this;
    }

    /**
     * Unregisters the provided listener from being notified on hosts events.
     * <p/>
     * This method is a no-op if {@code listener} hasn't previously been
     * registered against this Cluster.
     *
     * @param listener the {@link Host.StateListener} to unregister.
     * @return this {@code Cluster} object;
     */
    public Cluster unregister(Host.StateListener listener) {
        checkNotClosed(manager);
        boolean removed = manager.listeners.remove(listener);
        if (removed)
            listener.onUnregister(this);
        return this;
    }

    /**
     * Registers the provided tracker to be updated with hosts read
     * latencies.
     * <p/>
     * Registering the same tracker multiple times is a no-op.
     * <p/>
     * Beware that the registered tracker's
     * {@link LatencyTracker#update(Host, Statement, Exception, long) update}
     * method will be called
     * very frequently (at the end of every query to a Cassandra host) and
     * should thus not be costly.
     * <p/>
     * The main use case for a {@link LatencyTracker} is to allow
     * load balancing policies to implement latency awareness.
     * For example, {@link LatencyAwarePolicy} registers  it's own internal
     * {@code LatencyTracker} (automatically, you don't have to call this
     * method directly).
     *
     * @param tracker the new {@link LatencyTracker} to register.
     * @return this {@code Cluster} object;
     */
    public Cluster register(LatencyTracker tracker) {
        checkNotClosed(manager);
        boolean added = manager.latencyTrackers.add(tracker);
        if (added)
            tracker.onRegister(this);
        return this;
    }

    /**
     * Unregisters the provided latency tracking from being updated
     * with host read latencies.
     * <p/>
     * This method is a no-op if {@code tracker} hasn't previously been
     * registered against this Cluster.
     *
     * @param tracker the {@link LatencyTracker} to unregister.
     * @return this {@code Cluster} object;
     */
    public Cluster unregister(LatencyTracker tracker) {
        checkNotClosed(manager);
        boolean removed = manager.latencyTrackers.remove(tracker);
        if (removed)
            tracker.onUnregister(this);
        return this;
    }

    /**
     * Registers the provided listener to be updated with schema change events.
     * <p/>
     * Registering the same listener multiple times is a no-op.
     *
     * @param listener the new {@link SchemaChangeListener} to register.
     * @return this {@code Cluster} object;
     */
    public Cluster register(SchemaChangeListener listener) {
        checkNotClosed(manager);
        boolean added = manager.schemaChangeListeners.add(listener);
        if (added)
            listener.onRegister(this);
        return this;
    }

    /**
     * Unregisters the provided schema change listener from being updated
     * with schema change events.
     * <p/>
     * This method is a no-op if {@code listener} hasn't previously been
     * registered against this Cluster.
     *
     * @param listener the {@link SchemaChangeListener} to unregister.
     * @return this {@code Cluster} object;
     */
    public Cluster unregister(SchemaChangeListener listener) {
        checkNotClosed(manager);
        boolean removed = manager.schemaChangeListeners.remove(listener);
        if (removed)
            listener.onUnregister(this);
        return this;
    }

    /**
     * Initiates a shutdown of this cluster instance.
     * <p/>
     * This method is asynchronous and return a future on the completion
     * of the shutdown process. As soon a the cluster is shutdown, no
     * new request will be accepted, but already submitted queries are
     * allowed to complete. This method closes all connections from all
     * sessions and reclaims all resources used by this Cluster
     * instance.
     * <p/>
     * If for some reason you wish to expedite this process, the
     * {@link CloseFuture#force} can be called on the result future.
     * <p/>
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
     * <p/>
     * This method is a shortcut for {@code closeAsync().get()}.
     */
    @Override
    public void close() {
        try {
            closeAsync().get();
        } catch (ExecutionException e) {
            throw DriverThrowables.propagateCause(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Whether this Cluster instance has been closed.
     * <p/>
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
     * <p/>
     * If you want to create a new {@code Cluster} instance programmatically,
     * then it is advised to use {@link Cluster.Builder} which can be obtained from the
     * {@link Cluster#builder} method.
     * <p/>
     * But it is also possible to implement a custom {@code Initializer} that
     * retrieves initialization from a web-service or from a configuration file.
     */
    public interface Initializer {

        /**
         * An optional name for the created cluster.
         * <p/>
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
         * <p/>
         * Note that some configuration can be modified after the cluster
         * initialization but some others cannot. In particular, the ones that
         * cannot be changed afterwards includes:
         * <ul>
         * <li>the port use to connect to Cassandra nodes (see {@link ProtocolOptions}).</li>
         * <li>the policies used (see {@link Policies}).</li>
         * <li>the authentication info provided (see {@link Configuration}).</li>
         * <li>whether metrics are enabled (see {@link Configuration}).</li>
         * </ul>
         *
         * @return the configuration to use for the new cluster.
         */
        public Configuration getConfiguration();

        /**
         * Optional listeners to register against the newly created cluster.
         * <p/>
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
        private ProtocolVersion protocolVersion;
        private AuthProvider authProvider = AuthProvider.NONE;

        private final Policies.Builder policiesBuilder = Policies.builder();
        private final Configuration.Builder configurationBuilder = Configuration.builder();

        private ProtocolOptions.Compression compression = ProtocolOptions.Compression.NONE;
        private SSLOptions sslOptions = null;
        private boolean metricsEnabled = true;
        private boolean jmxEnabled = true;
        private boolean allowBetaProtocolVersion = false;

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
         * <p/>
         * Note: this is not related to the Cassandra cluster name (though you
         * are free to provide the same name). See {@link Cluster#getClusterName} for
         * details.
         * <p/>
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
         * <p/>
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
         * Create cluster connection using latest development protocol version,
         * which is currently in beta. Calling this method will result into setting
         * USE_BETA flag in all outgoing messages, which allows server to negotiate
         * the supported protocol version even if it is currently in beta.
         * <p/>
         * This feature is only available starting with version {@link ProtocolVersion#V5 V5}.
         * <p/>
         * Use with caution, refer to the server and protocol documentation for the details
         * on latest protocol version.
         *
         * @return this Builder.
         */
        public Builder allowBetaProtocolVersion() {
            if (protocolVersion != null)
                throw new IllegalArgumentException("Can't use beta flag with initial protocol version of " + protocolVersion);

            this.allowBetaProtocolVersion = true;
            this.protocolVersion = ProtocolVersion.NEWEST_BETA;
            return this;
        }

        /**
         * Sets the maximum time to wait for schema agreement before returning from a DDL query.
         * <p/>
         * If not set through this method, the default value (10 seconds) will be used.
         *
         * @param maxSchemaAgreementWaitSeconds the new value to set.
         * @return this Builder.
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
         * <p/>
         * The driver supports versions 1 to 5 of the native protocol. Higher versions
         * of the protocol have more features and should be preferred, but this also depends
         * on the Cassandra version:
         * <p/>
         * <table>
         * <caption>Native protocol version to Cassandra version correspondence</caption>
         * <tr><th>Protocol version</th><th>Minimum Cassandra version</th></tr>
         * <tr><td>1</td><td>1.2</td></tr>
         * <tr><td>2</td><td>2.0</td></tr>
         * <tr><td>3</td><td>2.1</td></tr>
         * <tr><td>4</td><td>2.2</td></tr>
         * <tr><td>5</td><td>3.10</td></tr>
         * </table>
         * <p/>
         * By default, the driver will "auto-detect" which protocol version it can use
         * when connecting to the first node. More precisely, it will try first with
         * {@link ProtocolVersion#NEWEST_SUPPORTED}, and if not supported fallback to
         * the highest version supported by the first node it connects to. Please note
         * that once the version is "auto-detected", it won't change: if the first node
         * the driver connects to is a Cassandra 1.2 node and auto-detection is used
         * (the default), then the native protocol version 1 will be use for the lifetime
         * of the Cluster instance.
         * <p/>
         * By using {@link Builder#allowBetaProtocolVersion()}, it is
         * possible to force driver to connect to Cassandra node that supports the latest
         * protocol beta version. Leaving this flag out will let client to connect with
         * latest released version.
         * <p/>
         * This method allows to force the use of a particular protocol version. Forcing
         * version 1 is always fine since all Cassandra version (at least all those
         * supporting the native protocol in the first place) so far support it. However,
         * please note that a number of features of the driver won't be available if that
         * version of the protocol is in use, including result set paging,
         * {@link BatchStatement}, executing a non-prepared query with binary values
         * ({@link Session#execute(String, Object...)}), ... (those methods will throw
         * an UnsupportedFeatureException). Using the protocol version 1 should thus
         * only be considered when using Cassandra 1.2, until nodes have been upgraded
         * to Cassandra 2.0.
         * <p/>
         * If version 2 of the protocol is used, then Cassandra 1.2 nodes will be ignored
         * (the driver won't connect to them).
         * <p/>
         * The default behavior (auto-detection) is fine in almost all case, but you may
         * want to force a particular version if you have a Cassandra cluster with mixed
         * 1.2/2.0 nodes (i.e. during a Cassandra upgrade).
         *
         * @param version the native protocol version to use. {@code null} is also supported
         *                to trigger auto-detection (see above) but this is the default (so you don't have
         *                to call this method for that behavior).
         * @return this Builder.
         */
        public Builder withProtocolVersion(ProtocolVersion version) {
            if (allowBetaProtocolVersion)
                throw new IllegalStateException("Can not set the version explicitly if `allowBetaProtocolVersion` was used.");
            if (version.compareTo(ProtocolVersion.NEWEST_SUPPORTED) > 0)
                throw new IllegalArgumentException("Can not use " + version + " protocol version. " +
                        "Newest supported protocol version is: " + ProtocolVersion.NEWEST_SUPPORTED + ". " +
                        "For beta versions, use `allowBetaProtocolVersion` instead");
            this.protocolVersion = version;
            return this;
        }

        /**
         * Adds a contact point - or many if the given address resolves to multiple
         * <code>InetAddress</code>s (A records).
         * <p/>
         * Contact points are addresses of Cassandra nodes that the driver uses
         * to discover the cluster topology. Only one contact point is required
         * (the driver will retrieve the address of the other nodes
         * automatically), but it is usually a good idea to provide more than
         * one contact point, because if that single contact point is unavailable,
         * the driver cannot initialize itself correctly.
         * <p/>
         * Note that by default (that is, unless you use the {@link #withLoadBalancingPolicy})
         * method of this builder), the first successfully contacted host will be used
         * to define the local data-center for the client. If follows that if you are
         * running Cassandra in a  multiple data-center setting, it is a good idea to
         * only provide contact points that are in the same datacenter than the client,
         * or to provide manually the load balancing policy that suits your need.
         * <p/>
         * If the host name points to a DNS record with multiple a-records, all InetAddresses
         * returned will be used. Make sure that all resulting <code>InetAddress</code>s returned
         * point to the same cluster and datacenter.
         *
         * @param address the address of the node(s) to connect to.
         * @return this Builder.
         * @throws IllegalArgumentException if the given {@code address}
         *                                  could not be resolved.
         * @throws SecurityException        if a security manager is present and
         *                                  permission to resolve the host name is denied.
         */
        public Builder addContactPoint(String address) {
            // We explicitly check for nulls because InetAdress.getByName() will happily
            // accept it and use localhost (while a null here almost likely mean a user error,
            // not "connect to localhost")
            if (address == null)
                throw new NullPointerException();

            try {
                addContactPoints(InetAddress.getAllByName(address));
                return this;
            } catch (UnknownHostException e) {
                throw new IllegalArgumentException("Failed to add contact point: " + address, e);
            }
        }

        /**
         * Adds contact points.
         * <p/>
         * See {@link Builder#addContactPoint} for more details on contact
         * points.
         * <p/>
         * Note that all contact points must be resolvable;
         * if <em>any</em> of them cannot be resolved, this method will fail.
         *
         * @param addresses addresses of the nodes to add as contact points.
         * @return this Builder.
         * @throws IllegalArgumentException if any of the given {@code addresses}
         *                                  could not be resolved.
         * @throws SecurityException        if a security manager is present and
         *                                  permission to resolve the host name is denied.
         * @see Builder#addContactPoint
         */
        public Builder addContactPoints(String... addresses) {
            for (String address : addresses)
                addContactPoint(address);
            return this;
        }

        /**
         * Adds contact points.
         * <p/>
         * See {@link Builder#addContactPoint} for more details on contact
         * points.
         * <p/>
         * Note that all contact points must be resolvable;
         * if <em>any</em> of them cannot be resolved, this method will fail.
         *
         * @param addresses addresses of the nodes to add as contact points.
         * @return this Builder.
         * @throws IllegalArgumentException if any of the given {@code addresses}
         *                                  could not be resolved.
         * @throws SecurityException        if a security manager is present and
         *                                  permission to resolve the host name is denied.
         * @see Builder#addContactPoint
         */
        public Builder addContactPoints(InetAddress... addresses) {
            Collections.addAll(this.rawAddresses, addresses);
            return this;
        }

        /**
         * Adds contact points.
         * <p/>
         * See {@link Builder#addContactPoint} for more details on contact
         * points.
         *
         * @param addresses addresses of the nodes to add as contact points.
         * @return this Builder
         * @see Builder#addContactPoint
         */
        public Builder addContactPoints(Collection<InetAddress> addresses) {
            this.rawAddresses.addAll(addresses);
            return this;
        }

        /**
         * Adds contact points.
         * <p/>
         * See {@link Builder#addContactPoint} for more details on contact
         * points. Contrarily to other {@code addContactPoints} methods, this method
         * allows to provide a different port for each contact point. Since Cassandra
         * nodes must always all listen on the same port, this is rarely what you
         * want and most users should prefer other {@code addContactPoints} methods to
         * this one. However, this can be useful if the Cassandra nodes are behind
         * a router and are not accessed directly. Note that if you are in this
         * situation (Cassandra nodes are behind a router, not directly accessible),
         * you almost surely want to provide a specific {@link AddressTranslator}
         * (through {@link #withAddressTranslator}) to translate actual Cassandra node
         * addresses to the addresses the driver should use, otherwise the driver
         * will not be able to auto-detect new nodes (and will generally not function
         * optimally).
         *
         * @param addresses addresses of the nodes to add as contact points.
         * @return this Builder
         * @see Builder#addContactPoint
         */
        public Builder addContactPointsWithPorts(InetSocketAddress... addresses) {
            Collections.addAll(this.addresses, addresses);
            return this;
        }

        /**
         * Adds contact points.
         * <p/>
         * See {@link Builder#addContactPoint} for more details on contact
         * points. Contrarily to other {@code addContactPoints} methods, this method
         * allows to provide a different port for each contact point. Since Cassandra
         * nodes must always all listen on the same port, this is rarely what you
         * want and most users should prefer other {@code addContactPoints} methods to
         * this one. However, this can be useful if the Cassandra nodes are behind
         * a router and are not accessed directly. Note that if you are in this
         * situation (Cassandra nodes are behind a router, not directly accessible),
         * you almost surely want to provide a specific {@link AddressTranslator}
         * (through {@link #withAddressTranslator}) to translate actual Cassandra node
         * addresses to the addresses the driver should use, otherwise the driver
         * will not be able to auto-detect new nodes (and will generally not function
         * optimally).
         *
         * @param addresses addresses of the nodes to add as contact points.
         * @return this Builder
         * @see Builder#addContactPoint
         */
        public Builder addContactPointsWithPorts(Collection<InetSocketAddress> addresses) {
            this.addresses.addAll(addresses);
            return this;
        }

        /**
         * Configures the load balancing policy to use for the new cluster.
         * <p/>
         * If no load balancing policy is set through this method,
         * {@link Policies#defaultLoadBalancingPolicy} will be used instead.
         *
         * @param policy the load balancing policy to use.
         * @return this Builder.
         */
        public Builder withLoadBalancingPolicy(LoadBalancingPolicy policy) {
            policiesBuilder.withLoadBalancingPolicy(policy);
            return this;
        }

        /**
         * Configures the reconnection policy to use for the new cluster.
         * <p/>
         * If no reconnection policy is set through this method,
         * {@link Policies#DEFAULT_RECONNECTION_POLICY} will be used instead.
         *
         * @param policy the reconnection policy to use.
         * @return this Builder.
         */
        public Builder withReconnectionPolicy(ReconnectionPolicy policy) {
            policiesBuilder.withReconnectionPolicy(policy);
            return this;
        }

        /**
         * Configures the retry policy to use for the new cluster.
         * <p/>
         * If no retry policy is set through this method,
         * {@link Policies#DEFAULT_RETRY_POLICY} will be used instead.
         *
         * @param policy the retry policy to use.
         * @return this Builder.
         */
        public Builder withRetryPolicy(RetryPolicy policy) {
            policiesBuilder.withRetryPolicy(policy);
            return this;
        }

        /**
         * Configures the address translator to use for the new cluster.
         * <p/>
         * See {@link AddressTranslator} for more detail on address translation,
         * but the default translator, {@link IdentityTranslator}, should be
         * correct in most cases. If unsure, stick to the default.
         *
         * @param translator the translator to use.
         * @return this Builder.
         */
        public Builder withAddressTranslator(AddressTranslator translator) {
            policiesBuilder.withAddressTranslator(translator);
            return this;
        }

        /**
         * Configures the generator that will produce the client-side timestamp sent
         * with each query.
         * <p/>
         * This feature is only available with version {@link ProtocolVersion#V3 V3} or
         * above of the native protocol. With earlier versions, timestamps are always
         * generated server-side, and setting a generator through this method will have
         * no effect.
         * <p/>
         * If no generator is set through this method, the driver will default to
         * client-side timestamps by using {@link AtomicMonotonicTimestampGenerator}.
         *
         * @param timestampGenerator the generator to use.
         * @return this Builder.
         */
        public Builder withTimestampGenerator(TimestampGenerator timestampGenerator) {
            policiesBuilder.withTimestampGenerator(timestampGenerator);
            return this;
        }

        /**
         * Configures the speculative execution policy to use for the new cluster.
         * <p/>
         * If no policy is set through this method, {@link Policies#defaultSpeculativeExecutionPolicy()}
         * will be used instead.
         *
         * @param policy the policy to use.
         * @return this Builder.
         */
        public Builder withSpeculativeExecutionPolicy(SpeculativeExecutionPolicy policy) {
            policiesBuilder.withSpeculativeExecutionPolicy(policy);
            return this;
        }


        /**
         * Configures the {@link CodecRegistry} instance to use for the new cluster.
         * <p/>
         * If no codec registry is set through this method, {@link CodecRegistry#DEFAULT_INSTANCE}
         * will be used instead.
         * <p>Note that if two or more {@link Cluster} instances are configured to
         * use the default codec registry, they are going to share the same instance.
         * In this case, care should be taken when registering new codecs on it as any
         * codec registered by one cluster would be immediately available to others
         * sharing the same default instance.
         *
         * @param codecRegistry the codec registry to use.
         * @return this Builder.
         */
        public Builder withCodecRegistry(CodecRegistry codecRegistry) {
            configurationBuilder.withCodecRegistry(codecRegistry);
            return this;
        }

        /**
         * Uses the provided credentials when connecting to Cassandra hosts.
         * <p/>
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
         * <p/>
         * Use this method when a custom authentication scheme is in place.
         * You shouldn't call both this method and {@code withCredentials}
         * on the same {@code Builder} instance as one will supersede the
         * other
         *
         * @param authProvider the {@link AuthProvider} to use to login to
         *                     Cassandra hosts.
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
         * <p/>
         * Calling this method will use the JDK-based implementation with the default options
         * (see {@link RemoteEndpointAwareJdkSSLOptions.Builder}).
         * This is thus a shortcut for {@code withSSL(JdkSSLOptions.builder().build())}.
         * <p/>
         * Note that if SSL is enabled, the driver will not connect to any
         * Cassandra nodes that doesn't have SSL enabled and it is strongly
         * advised to enable SSL on every Cassandra node if you plan on using
         * SSL in the driver.
         *
         * @return this builder.
         */
        public Builder withSSL() {
            this.sslOptions = RemoteEndpointAwareJdkSSLOptions.builder().build();
            return this;
        }

        /**
         * Enable the use of SSL for the created {@code Cluster} using the provided options.
         *
         * @param sslOptions the SSL options to use.
         * @return this builder.
         */
        public Builder withSSL(SSLOptions sslOptions) {
            this.sslOptions = sslOptions;
            return this;
        }

        /**
         * Register the provided listeners in the newly created cluster.
         * <p/>
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
         * <p/>
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
         * <p/>
         * If no pooling options are set through this method, default pooling
         * options will be used.
         *
         * @param options the pooling options to use.
         * @return this builder.
         */
        public Builder withPoolingOptions(PoolingOptions options) {
            configurationBuilder.withPoolingOptions(options);
            return this;
        }

        /**
         * Sets the SocketOptions to use for the newly created Cluster.
         * <p/>
         * If no socket options are set through this method, default socket
         * options will be used.
         *
         * @param options the socket options to use.
         * @return this builder.
         */
        public Builder withSocketOptions(SocketOptions options) {
            configurationBuilder.withSocketOptions(options);
            return this;
        }

        /**
         * Sets the QueryOptions to use for the newly created Cluster.
         * <p/>
         * If no query options are set through this method, default query
         * options will be used.
         *
         * @param options the query options to use.
         * @return this builder.
         */
        public Builder withQueryOptions(QueryOptions options) {
            configurationBuilder.withQueryOptions(options);
            return this;
        }

        /**
         * Sets the threading options to use for the newly created Cluster.
         * <p/>
         * If no options are set through this method, a new instance of {@link ThreadingOptions} will be used.
         *
         * @param options the options.
         * @return this builder.
         */
        public Builder withThreadingOptions(ThreadingOptions options) {
            configurationBuilder.withThreadingOptions(options);
            return this;
        }

        /**
         * Set the {@link NettyOptions} to use for the newly created Cluster.
         * <p/>
         * If no Netty options are set through this method, {@link NettyOptions#DEFAULT_INSTANCE}
         * will be used as a default value, which means that no customization will be applied.
         *
         * @param nettyOptions the {@link NettyOptions} to use.
         * @return this builder.
         */
        public Builder withNettyOptions(NettyOptions nettyOptions) {
            configurationBuilder.withNettyOptions(nettyOptions);
            return this;
        }

        /**
         * The configuration that will be used for the new cluster.
         * <p/>
         * You <b>should not</b> modify this object directly because changes made
         * to the returned object may not be used by the cluster build.
         * Instead, you should use the other methods of this {@code Builder}.
         *
         * @return the configuration to use for the new cluster.
         */
        @Override
        public Configuration getConfiguration() {
            ProtocolOptions protocolOptions = new ProtocolOptions(port, protocolVersion, maxSchemaAgreementWaitSeconds, sslOptions, authProvider)
                    .setCompression(compression);

            MetricsOptions metricsOptions = new MetricsOptions(metricsEnabled, jmxEnabled);

            return configurationBuilder
                    .withProtocolOptions(protocolOptions)
                    .withMetricsOptions(metricsOptions)
                    .withPolicies(policiesBuilder.build())
                    .build();
        }

        @Override
        public Collection<Host.StateListener> getInitialListeners() {
            return listeners == null ? Collections.<Host.StateListener>emptySet() : listeners;
        }

        /**
         * Builds the cluster with the configured set of initial contact points
         * and policies.
         * <p/>
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
     * <p/>
     * Note: the reason we create a Manager object separate from Cluster is
     * that Manager is not publicly visible. For instance, we wouldn't want
     * user to be able to call the {@link #onUp} and {@link #onDown} methods.
     */
    class Manager implements Connection.DefaultResponseHandler {

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

        final ConvictionPolicy.Factory convictionPolicyFactory = new ConvictionPolicy.DefaultConvictionPolicy.Factory();

        ListeningExecutorService executor;
        ListeningExecutorService blockingExecutor;
        ScheduledExecutorService reconnectionExecutor;
        ScheduledExecutorService scheduledTasksExecutor;

        BlockingQueue<Runnable> executorQueue;
        BlockingQueue<Runnable> blockingExecutorQueue;
        BlockingQueue<Runnable> reconnectionExecutorQueue;
        BlockingQueue<Runnable> scheduledTasksExecutorQueue;

        ConnectionReaper reaper;

        final AtomicReference<CloseFuture> closeFuture = new AtomicReference<CloseFuture>();

        // All the queries that have been prepared (we keep them so we can re-prepared them when a node fail or a
        // new one join the cluster).
        // Note: we could move this down to the session level, but since prepared statement are global to a node,
        // this would yield a slightly less clear behavior.
        ConcurrentMap<MD5Digest, PreparedStatement> preparedQueries;

        final Set<Host.StateListener> listeners;
        final Set<LatencyTracker> latencyTrackers = new CopyOnWriteArraySet<LatencyTracker>();
        final Set<SchemaChangeListener> schemaChangeListeners = new CopyOnWriteArraySet<SchemaChangeListener>();

        EventDebouncer<NodeListRefreshRequest> nodeListRefreshRequestDebouncer;
        EventDebouncer<NodeRefreshRequest> nodeRefreshRequestDebouncer;
        EventDebouncer<SchemaRefreshRequest> schemaRefreshRequestDebouncer;

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

            ThreadingOptions threadingOptions = this.configuration.getThreadingOptions();

            // executor
            ExecutorService tmpExecutor = threadingOptions.createExecutor(clusterName);
            this.executorQueue = (tmpExecutor instanceof ThreadPoolExecutor)
                    ? ((ThreadPoolExecutor) tmpExecutor).getQueue() : null;
            this.executor = MoreExecutors.listeningDecorator(tmpExecutor);

            // blocking executor
            ExecutorService tmpBlockingExecutor = threadingOptions.createBlockingExecutor(clusterName);
            this.blockingExecutorQueue = (tmpBlockingExecutor instanceof ThreadPoolExecutor)
                    ? ((ThreadPoolExecutor) tmpBlockingExecutor).getQueue() : null;
            this.blockingExecutor = MoreExecutors.listeningDecorator(tmpBlockingExecutor);

            // reconnection executor
            this.reconnectionExecutor = threadingOptions.createReconnectionExecutor(clusterName);
            this.reconnectionExecutorQueue = (reconnectionExecutor instanceof ThreadPoolExecutor)
                    ? ((ThreadPoolExecutor) reconnectionExecutor).getQueue() : null;

            // scheduled tasks executor
            this.scheduledTasksExecutor = threadingOptions.createScheduledTasksExecutor(clusterName);
            this.scheduledTasksExecutorQueue = (scheduledTasksExecutor instanceof ThreadPoolExecutor)
                    ? ((ThreadPoolExecutor) scheduledTasksExecutor).getQueue() : null;

            this.reaper = new ConnectionReaper(threadingOptions.createReaperExecutor(clusterName));
            this.metadata = new Metadata(this);
            this.connectionFactory = new Connection.Factory(this, configuration);
            this.controlConnection = new ControlConnection(this);
            this.metrics = configuration.getMetricsOptions().isEnabled() ? new Metrics(this) : null;
            this.preparedQueries = new MapMaker().weakValues().makeMap();

            // create debouncers - at this stage, they are not running yet
            final QueryOptions queryOptions = configuration.getQueryOptions();
            this.nodeListRefreshRequestDebouncer = new EventDebouncer<NodeListRefreshRequest>(
                    "Node list refresh",
                    scheduledTasksExecutor,
                    new NodeListRefreshRequestDeliveryCallback()
            ) {

                @Override
                int maxPendingEvents() {
                    return configuration.getQueryOptions().getMaxPendingRefreshNodeListRequests();
                }

                @Override
                long delayMs() {
                    return configuration.getQueryOptions().getRefreshNodeListIntervalMillis();
                }
            };
            this.nodeRefreshRequestDebouncer = new EventDebouncer<NodeRefreshRequest>(
                    "Node refresh",
                    scheduledTasksExecutor,
                    new NodeRefreshRequestDeliveryCallback()
            ) {

                @Override
                int maxPendingEvents() {
                    return configuration.getQueryOptions().getMaxPendingRefreshNodeRequests();
                }

                @Override
                long delayMs() {
                    return configuration.getQueryOptions().getRefreshNodeIntervalMillis();
                }
            };
            this.schemaRefreshRequestDebouncer = new EventDebouncer<SchemaRefreshRequest>(
                    "Schema refresh",
                    scheduledTasksExecutor,
                    new SchemaRefreshRequestDeliveryCallback()
            ) {

                @Override
                int maxPendingEvents() {
                    return configuration.getQueryOptions().getMaxPendingRefreshSchemaRequests();
                }

                @Override
                long delayMs() {
                    return configuration.getQueryOptions().getRefreshSchemaIntervalMillis();
                }
            };

            this.scheduledTasksExecutor.scheduleWithFixedDelay(new CleanupIdleConnectionsTask(), 10, 10, TimeUnit.SECONDS);

            for (InetSocketAddress address : contactPoints) {
                // We don't want to signal -- call onAdd() -- because nothing is ready
                // yet (loadbalancing policy, control connection, ...). All we want is
                // create the Host object so we can initialize the control connection.
                metadata.add(address);
            }

            Collection<Host> allHosts = metadata.allHosts();

            // At this stage, metadata.allHosts() only contains the contact points, that's what we want to pass to LBP.init().
            // But the control connection will initialize first and discover more hosts, so make a copy.
            Set<Host> contactPointHosts = Sets.newHashSet(allHosts);

            try {
                negotiateProtocolVersionAndConnect();

                // The control connection can mark hosts down if it failed to connect to them, or remove them if they weren't found
                // in the control host's system.peers. Separate them:
                Set<Host> downContactPointHosts = Sets.newHashSet();
                Set<Host> removedContactPointHosts = Sets.newHashSet();
                for (Host host : contactPointHosts) {
                    if (!allHosts.contains(host))
                        removedContactPointHosts.add(host);
                    else if (host.state == Host.State.DOWN)
                        downContactPointHosts.add(host);
                }
                contactPointHosts.removeAll(removedContactPointHosts);
                contactPointHosts.removeAll(downContactPointHosts);

                // Now that the control connection is ready, we have all the information we need about the nodes (datacenter,
                // rack...) to initialize the load balancing policy
                loadBalancingPolicy().init(Cluster.this, contactPointHosts);

                speculativeExecutionPolicy().init(Cluster.this);
                configuration.getPolicies().getRetryPolicy().init(Cluster.this);
                reconnectionPolicy().init(Cluster.this);
                configuration.getPolicies().getAddressTranslator().init(Cluster.this);
                for (LatencyTracker tracker : latencyTrackers)
                    tracker.onRegister(Cluster.this);
                for (Host.StateListener listener : listeners)
                    listener.onRegister(Cluster.this);
                for (Host host : removedContactPointHosts) {
                    loadBalancingPolicy().onRemove(host);
                    for (Host.StateListener listener : listeners)
                        listener.onRemove(host);
                }

                for (Host host : downContactPointHosts) {
                    loadBalancingPolicy().onDown(host);
                    for (Host.StateListener listener : listeners)
                        listener.onDown(host);
                    startPeriodicReconnectionAttempt(host, true);
                }

                configuration.getPoolingOptions().setProtocolVersion(protocolVersion());

                for (Host host : allHosts) {
                    // If the host is down at this stage, it's a contact point that the control connection failed to reach.
                    // Reconnection attempts are already scheduled, and the LBP and listeners have been notified above.
                    if (host.state == Host.State.DOWN) continue;

                    // Otherwise, we want to do the equivalent of onAdd(). But since we know for sure that no sessions or prepared
                    // statements exist at this point, we can skip some of the steps (plus this avoids scheduling concurrent pool
                    // creations if a session is created right after this method returns).
                    logger.info("New Cassandra host {} added", host);

                    if (!host.supports(connectionFactory.protocolVersion)) {
                        logUnsupportedVersionProtocol(host, connectionFactory.protocolVersion);
                        continue;
                    }

                    if (!contactPointHosts.contains(host))
                        loadBalancingPolicy().onAdd(host);

                    host.setUp();

                    for (Host.StateListener listener : listeners)
                        listener.onAdd(host);
                }

                // start debouncers
                this.nodeListRefreshRequestDebouncer.start();
                this.schemaRefreshRequestDebouncer.start();
                this.nodeRefreshRequestDebouncer.start();

                isFullyInit = true;
            } catch (NoHostAvailableException e) {
                close();
                throw e;
            }
        }

        private void negotiateProtocolVersionAndConnect() {
            boolean shouldNegotiate = (configuration.getProtocolOptions().initialProtocolVersion == null);
            while (true) {
                try {
                    controlConnection.connect();
                    return;
                } catch (UnsupportedProtocolVersionException e) {
                    if (!shouldNegotiate) {
                        throw e;
                    }
                    // Do not trust version of server's response, as C* behavior in case of protocol negotiation is not
                    // properly documented, and varies over time (specially after CASSANDRA-11464). Instead, always
                    // retry at attempted version - 1, if such a version exists; and otherwise, stop and fail.
                    ProtocolVersion attemptedVersion = e.getUnsupportedVersion();
                    ProtocolVersion retryVersion = attemptedVersion.getLowerSupported();
                    if (retryVersion == null) {
                        throw e;
                    }
                    logger.info("Cannot connect with protocol version {}, trying with {}", attemptedVersion, retryVersion);
                    connectionFactory.protocolVersion = retryVersion;
                }
            }
        }

        ProtocolVersion protocolVersion() {
            return connectionFactory.protocolVersion;
        }

        Cluster getCluster() {
            return Cluster.this;
        }

        LoadBalancingPolicy loadBalancingPolicy() {
            return configuration.getPolicies().getLoadBalancingPolicy();
        }

        SpeculativeExecutionPolicy speculativeExecutionPolicy() {
            return configuration.getPolicies().getSpeculativeExecutionPolicy();
        }

        ReconnectionPolicy reconnectionPolicy() {
            return configuration.getPolicies().getReconnectionPolicy();
        }

        InetSocketAddress translateAddress(InetAddress address) {
            InetSocketAddress sa = new InetSocketAddress(address, connectionFactory.getPort());
            InetSocketAddress translated = configuration.getPolicies().getAddressTranslator().translate(sa);
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

        void reportQuery(Host host, Statement statement, Exception exception, long latencyNanos) {
            for (LatencyTracker tracker : latencyTrackers) {
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

                // stop debouncers
                nodeListRefreshRequestDebouncer.stop();
                nodeRefreshRequestDebouncer.stop();
                schemaRefreshRequestDebouncer.stop();

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

                loadBalancingPolicy().close();
                speculativeExecutionPolicy().close();
                configuration.getPolicies().getRetryPolicy().close();
                reconnectionPolicy().close();
                configuration.getPolicies().getAddressTranslator().close();
                for (LatencyTracker tracker : latencyTrackers)
                    tracker.onUnregister(Cluster.this);
                for (Host.StateListener listener : listeners)
                    listener.onUnregister(Cluster.this);
                for (SchemaChangeListener listener : schemaChangeListeners)
                    listener.onUnregister(Cluster.this);

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
                    ((FutureTask<?>) pendingTask).cancel(false);
            }
        }

        void logUnsupportedVersionProtocol(Host host, ProtocolVersion version) {
            logger.warn("Detected added or restarted Cassandra host {} but ignoring it since it does not support the version {} of the native "
                    + "protocol which is currently in use. If you want to force the use of a particular version of the native protocol, use "
                    + "Cluster.Builder#usingProtocolVersion() when creating the Cluster instance.", host, version);
        }

        void logClusterNameMismatch(Host host, String expectedClusterName, String actualClusterName) {
            logger.warn("Detected added or restarted Cassandra host {} but ignoring it since its cluster name '{}' does not match the one "
                            + "currently known ({})",
                    host, actualClusterName, expectedClusterName);
        }

        public ListenableFuture<?> triggerOnUp(final Host host) {
            if (!isClosed()) {
                return executor.submit(new ExceptionCatchingRunnable() {
                    @Override
                    public void runMayThrow() throws InterruptedException, ExecutionException {
                        onUp(host, null);
                    }
                });
            } else {
                return MoreFutures.VOID_SUCCESS;
            }
        }

        // Use triggerOnUp unless you're sure you want to run this on the current thread.
        private void onUp(final Host host, Connection reusedConnection) throws InterruptedException, ExecutionException {
            if (isClosed())
                return;

            if (!host.supports(connectionFactory.protocolVersion)) {
                logUnsupportedVersionProtocol(host, connectionFactory.protocolVersion);
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

                    Host.statesLogger.debug("[{}] marking host UP", host);

                    // If there is a reconnection attempt scheduled for that node, cancel it
                    Future<?> scheduledAttempt = host.reconnectionAttempt.getAndSet(null);
                    if (scheduledAttempt != null) {
                        logger.debug("Cancelling reconnection attempt since node is UP");
                        scheduledAttempt.cancel(false);
                    }

                    try {
                        if (getCluster().getConfiguration().getQueryOptions().isReprepareOnUp())
                            reusedConnection = prepareAllQueries(host, reusedConnection);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        // Don't propagate because we don't want to prevent other listener to run
                    } catch (UnsupportedProtocolVersionException e) {
                        logUnsupportedVersionProtocol(host, e.getUnsupportedVersion());
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
                        s.updateCreatedPools().get();

                } finally {
                    host.notificationsLock.unlock();
                }

            } finally {
                if (reusedConnection != null && !reusedConnection.hasOwner())
                    reusedConnection.closeAsync();
            }
        }

        public ListenableFuture<?> triggerOnDown(final Host host, boolean startReconnection) {
            return triggerOnDown(host, false, startReconnection);
        }

        public ListenableFuture<?> triggerOnDown(final Host host, final boolean isHostAddition, final boolean startReconnection) {
            if (!isClosed()) {
                return executor.submit(new ExceptionCatchingRunnable() {
                    @Override
                    public void runMayThrow() throws InterruptedException, ExecutionException {
                        onDown(host, isHostAddition, startReconnection);
                    }
                });
            } else {
                return MoreFutures.VOID_SUCCESS;
            }
        }

        // Use triggerOnDown unless you're sure you want to run this on the current thread.
        private void onDown(final Host host, final boolean isHostAddition, boolean startReconnection) throws InterruptedException, ExecutionException {
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

                Host.statesLogger.debug("[{}] marking host DOWN", host);

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

                startPeriodicReconnectionAttempt(host, isHostAddition);
            } finally {
                host.notificationsLock.unlock();
            }
        }

        void startPeriodicReconnectionAttempt(final Host host, final boolean isHostAddition) {
            new AbstractReconnectionHandler(host.toString(), reconnectionExecutor, reconnectionPolicy().newSchedule(), host.reconnectionAttempt) {

                @Override
                protected Connection tryReconnect() throws ConnectionException, InterruptedException, UnsupportedProtocolVersionException, ClusterNameMismatchException {
                    return connectionFactory.open(host);
                }

                @Override
                protected void onReconnection(Connection connection) {
                    // Make sure we have up-to-date infos on that host before adding it (so we typically
                    // catch that an upgraded node uses a new cassandra version).
                    if (controlConnection.refreshNodeInfo(host)) {
                        logger.debug("Successful reconnection to {}, setting host UP", host);
                        try {
                            if (isHostAddition) {
                                onAdd(host, connection);
                                submitNodeListRefresh();
                            } else
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

                @Override
                protected boolean onConnectionException(ConnectionException e, long nextDelayMs) {
                    if (logger.isDebugEnabled())
                        logger.debug("Failed reconnection to {} ({}), scheduling retry in {} milliseconds", host, e.getMessage(), nextDelayMs);
                    return true;
                }

                @Override
                protected boolean onUnknownException(Exception e, long nextDelayMs) {
                    logger.error(String.format("Unknown error during reconnection to %s, scheduling retry in %d milliseconds", host, nextDelayMs), e);
                    return true;
                }

                @Override
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
            new AbstractReconnectionHandler(host.toString(), reconnectionExecutor, reconnectionPolicy().newSchedule(), host.reconnectionAttempt, 0) {

                @Override
                protected Connection tryReconnect() throws ConnectionException, InterruptedException, UnsupportedProtocolVersionException, ClusterNameMismatchException {
                    return connectionFactory.open(host);
                }

                @Override
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

                @Override
                protected boolean onConnectionException(ConnectionException e, long nextDelayMs) {
                    if (logger.isDebugEnabled())
                        logger.debug("Failed one-time reconnection to {} ({})", host, e.getMessage());
                    return false;
                }

                @Override
                protected boolean onUnknownException(Exception e, long nextDelayMs) {
                    logger.error(String.format("Unknown error during one-time reconnection to %s", host), e);
                    return false;
                }

                @Override
                protected boolean onAuthenticationException(AuthenticationException e, long nextDelayMs) {
                    logger.error(String.format("Authentication error during one-time reconnection to %s", host), e);
                    return false;
                }
            }.start();
        }

        public ListenableFuture<?> triggerOnAdd(final Host host) {
            if (!isClosed()) {
                return executor.submit(new ExceptionCatchingRunnable() {
                    @Override
                    public void runMayThrow() throws InterruptedException, ExecutionException {
                        onAdd(host, null);
                    }
                });
            } else {
                return MoreFutures.VOID_SUCCESS;
            }
        }

        // Use triggerOnAdd unless you're sure you want to run this on the current thread.
        private void onAdd(final Host host, Connection reusedConnection) throws InterruptedException, ExecutionException {
            if (isClosed())
                return;

            if (!host.supports(connectionFactory.protocolVersion)) {
                logUnsupportedVersionProtocol(host, connectionFactory.protocolVersion);
                return;
            }

            try {

                boolean locked = host.notificationsLock.tryLock(NOTIF_LOCK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                if (!locked) {
                    logger.warn("Could not acquire notifications lock within {} seconds, ignoring ADD notification for {}", NOTIF_LOCK_TIMEOUT_SECONDS, host);
                    return;
                }
                try {
                    Host.statesLogger.debug("[{}] adding host", host);

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
                        logUnsupportedVersionProtocol(host, e.getUnsupportedVersion());
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
                        s.updateCreatedPools().get();

                } finally {
                    host.notificationsLock.unlock();
                }

            } finally {
                if (reusedConnection != null && !reusedConnection.hasOwner())
                    reusedConnection.closeAsync();
            }
        }

        public ListenableFuture<?> triggerOnRemove(final Host host) {
            if (!isClosed()) {
                return executor.submit(new ExceptionCatchingRunnable() {
                    @Override
                    public void runMayThrow() throws InterruptedException, ExecutionException {
                        onRemove(host);
                    }
                });
            } else {
                return MoreFutures.VOID_SUCCESS;
            }
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

                Host.statesLogger.debug("[{}] removing host", host);

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

        public void signalHostDown(Host host, boolean isHostAddition) {
            // Don't mark the node down until we've fully initialized the controlConnection as this might mess up with
            // the protocol detection
            if (!isFullyInit || isClosed())
                return;

            triggerOnDown(host, isHostAddition, true);
        }

        public void removeHost(Host host, boolean isInitialConnection) {
            if (host == null)
                return;

            if (metadata.remove(host)) {
                if (isInitialConnection) {
                    logger.warn("You listed {} in your contact points, but it wasn't found in the control host's system.peers at startup", host);
                } else {
                    logger.info("Cassandra host {} removed", host);
                    triggerOnRemove(host);
                }
            }
        }

        public void ensurePoolsSizing() {
            if (protocolVersion().compareTo(ProtocolVersion.V3) >= 0)
                return;

            for (SessionManager session : sessions) {
                for (HostConnectionPool pool : session.pools.values())
                    pool.ensureCoreConnections();
            }
        }

        public PreparedStatement addPrepared(PreparedStatement stmt) {
            PreparedStatement previous = preparedQueries.putIfAbsent(stmt.getPreparedId().id, stmt);
            if (previous != null) {
                logger.warn("Re-preparing already prepared query is generally an anti-pattern and will likely affect performance. "
                        + "Consider preparing the statement only once. Query='{}'", stmt.getQueryString());

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
         * of a connection pool). Can be reusedConnection, or one that was open in the method.
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

        ListenableFuture<Void> submitSchemaRefresh(final SchemaElement targetType, final String targetKeyspace, final String targetName, final List<String> targetSignature) {
            SchemaRefreshRequest request = new SchemaRefreshRequest(targetType, targetKeyspace, targetName, targetSignature);
            logger.trace("Submitting schema refresh: {}", request);
            return schemaRefreshRequestDebouncer.eventReceived(request);
        }

        ListenableFuture<Void> submitNodeListRefresh() {
            logger.trace("Submitting node list and token map refresh");
            return nodeListRefreshRequestDebouncer.eventReceived(new NodeListRefreshRequest());
        }

        ListenableFuture<Void> submitNodeRefresh(InetSocketAddress address, HostEvent eventType) {
            NodeRefreshRequest request = new NodeRefreshRequest(address, eventType);
            logger.trace("Submitting node refresh: {}", request);
            return nodeRefreshRequestDebouncer.eventReceived(request);
        }

        // refresh the schema using the provided connection, and notice the future with the provided resultset once done
        public void refreshSchemaAndSignal(final Connection connection, final DefaultResultSetFuture future, final ResultSet rs, final SchemaElement targetType, final String targetKeyspace, final String targetName, final List<String> targetSignature) {
            if (logger.isDebugEnabled())
                logger.debug("Refreshing schema for {}{}",
                        targetType == null ? "everything" : targetKeyspace,
                        (targetType == KEYSPACE) ? "" : "." + targetName + " (" + targetType + ")");

            maybeRefreshSchemaAndSignal(connection, future, rs, targetType, targetKeyspace, targetName, targetSignature);
        }

        public void waitForSchemaAgreementAndSignal(final Connection connection, final DefaultResultSetFuture future, final ResultSet rs) {
            maybeRefreshSchemaAndSignal(connection, future, rs, null, null, null, null);
        }

        private void maybeRefreshSchemaAndSignal(final Connection connection, final DefaultResultSetFuture future, final ResultSet rs, final SchemaElement targetType, final String targetKeyspace, final String targetName, final List<String> targetSignature) {
            final boolean refreshSchema = (targetKeyspace != null); // if false, only wait for schema agreement

            executor.submit(new Runnable() {
                @Override
                public void run() {
                    boolean schemaInAgreement = false;
                    try {
                        // Before refreshing the schema, wait for schema agreement so
                        // that querying a table just after having created it don't fail.
                        schemaInAgreement = ControlConnection.waitForSchemaAgreement(connection, Cluster.Manager.this);
                        if (!schemaInAgreement)
                            logger.warn("No schema agreement from live replicas after {} s. The schema may not be up to date on some nodes.", configuration.getProtocolOptions().getMaxSchemaAgreementWaitSeconds());

                        ListenableFuture<Void> schemaReady;
                        if (refreshSchema) {
                            schemaReady = submitSchemaRefresh(targetType, targetKeyspace, targetName, targetSignature);
                            // JAVA-1120: skip debouncing delay and force immediate delivery
                            if (!schemaReady.isDone())
                                schemaRefreshRequestDebouncer.scheduleImmediateDelivery();
                        } else {
                            schemaReady = MoreFutures.VOID_SUCCESS;
                        }
                        final boolean finalSchemaInAgreement = schemaInAgreement;
                        schemaReady.addListener(new Runnable() {
                            @Override
                            public void run() {
                                rs.getExecutionInfo().setSchemaInAgreement(finalSchemaInAgreement);
                                future.setResult(rs);
                            }
                        }, GuavaCompatibility.INSTANCE.sameThreadExecutor());

                    } catch (Exception e) {
                        logger.warn("Error while waiting for schema agreement", e);
                        // This is not fatal, complete the future anyway
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

            final ProtocolEvent event = ((Responses.Event) response).event;

            logger.debug("Received event {}, scheduling delivery", response);

            switch (event.type) {
                case TOPOLOGY_CHANGE:
                    ProtocolEvent.TopologyChange tpc = (ProtocolEvent.TopologyChange) event;
                    InetSocketAddress tpAddr = translateAddress(tpc.node.getAddress());
                    Host.statesLogger.debug("[{}] received event {}", tpAddr, tpc.change);
                    switch (tpc.change) {
                        case NEW_NODE:
                            submitNodeRefresh(tpAddr, HostEvent.ADDED);
                            break;
                        case REMOVED_NODE:
                            submitNodeRefresh(tpAddr, HostEvent.REMOVED);
                            break;
                        case MOVED_NODE:
                            submitNodeListRefresh();
                            break;
                    }
                    break;
                case STATUS_CHANGE:
                    ProtocolEvent.StatusChange stc = (ProtocolEvent.StatusChange) event;
                    InetSocketAddress stAddr = translateAddress(stc.node.getAddress());
                    Host.statesLogger.debug("[{}] received event {}", stAddr, stc.status);
                    switch (stc.status) {
                        case UP:
                            submitNodeRefresh(stAddr, HostEvent.UP);
                            break;
                        case DOWN:
                            submitNodeRefresh(stAddr, HostEvent.DOWN);
                            break;
                    }
                    break;
                case SCHEMA_CHANGE:
                    if (!configuration.getQueryOptions().isMetadataEnabled())
                        return;

                    ProtocolEvent.SchemaChange scc = (ProtocolEvent.SchemaChange) event;
                    switch (scc.change) {
                        case CREATED:
                        case UPDATED:
                            submitSchemaRefresh(scc.targetType, scc.targetKeyspace, scc.targetName, scc.targetSignature);
                            break;
                        case DROPPED:
                            if (scc.targetType == KEYSPACE) {
                                final KeyspaceMetadata removedKeyspace = manager.metadata.removeKeyspace(scc.targetKeyspace);
                                if (removedKeyspace != null) {
                                    executor.submit(new Runnable() {
                                        @Override
                                        public void run() {
                                            manager.metadata.triggerOnKeyspaceRemoved(removedKeyspace);
                                        }
                                    });
                                }
                            } else {
                                KeyspaceMetadata keyspace = manager.metadata.keyspaces.get(scc.targetKeyspace);
                                if (keyspace == null) {
                                    logger.warn("Received a DROPPED notification for {} {}.{}, but this keyspace is unknown in our metadata",
                                            scc.targetType, scc.targetKeyspace, scc.targetName);
                                } else {
                                    switch (scc.targetType) {
                                        case TABLE:
                                            // we can't tell whether it's a table or a view,
                                            // but since two objects cannot have the same name,
                                            // try removing both
                                            final TableMetadata removedTable = keyspace.removeTable(scc.targetName);
                                            if (removedTable != null) {
                                                executor.submit(new Runnable() {
                                                    @Override
                                                    public void run() {
                                                        manager.metadata.triggerOnTableRemoved(removedTable);
                                                    }
                                                });
                                            } else {
                                                final MaterializedViewMetadata removedView = keyspace.removeMaterializedView(scc.targetName);
                                                if (removedView != null) {
                                                    executor.submit(new Runnable() {
                                                        @Override
                                                        public void run() {
                                                            manager.metadata.triggerOnMaterializedViewRemoved(removedView);
                                                        }
                                                    });
                                                }
                                            }
                                            break;
                                        case TYPE:
                                            final UserType removedType = keyspace.removeUserType(scc.targetName);
                                            if (removedType != null) {
                                                executor.submit(new Runnable() {
                                                    @Override
                                                    public void run() {
                                                        manager.metadata.triggerOnUserTypeRemoved(removedType);
                                                    }
                                                });
                                            }
                                            break;
                                        case FUNCTION:
                                            final FunctionMetadata removedFunction = keyspace.removeFunction(Metadata.fullFunctionName(scc.targetName, scc.targetSignature));
                                            if (removedFunction != null) {
                                                executor.submit(new Runnable() {
                                                    @Override
                                                    public void run() {
                                                        manager.metadata.triggerOnFunctionRemoved(removedFunction);
                                                    }
                                                });
                                            }
                                            break;
                                        case AGGREGATE:
                                            final AggregateMetadata removedAggregate = keyspace.removeAggregate(Metadata.fullFunctionName(scc.targetName, scc.targetSignature));
                                            if (removedAggregate != null) {
                                                executor.submit(new Runnable() {
                                                    @Override
                                                    public void run() {
                                                        manager.metadata.triggerOnAggregateRemoved(removedAggregate);
                                                    }
                                                });
                                            }
                                            break;
                                    }
                                }
                            }
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
                controlConnection.triggerReconnect();

            try {
                for (SessionManager s : sessions)
                    Uninterruptibles.getUninterruptibly(s.updateCreatedPools());
            } catch (ExecutionException e) {
                throw DriverThrowables.propagateCause(e);
            }
        }

        void refreshConnectedHost(Host host) {
            // Deal with the control connection if it was using this host
            Host ccHost = controlConnection.connectedHost();
            if (ccHost == null || ccHost.equals(host) && loadBalancingPolicy().distance(ccHost) != HostDistance.LOCAL)
                controlConnection.triggerReconnect();

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
                    @Override
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
            @Override
            public void run() {
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

            private final SchemaElement targetType;
            private final String targetKeyspace;
            private final String targetName;
            private final List<String> targetSignature;

            public SchemaRefreshRequest(SchemaElement targetType, String targetKeyspace, String targetName, List<String> targetSignature) {
                this.targetType = targetType;
                this.targetKeyspace = Strings.emptyToNull(targetKeyspace);
                this.targetName = Strings.emptyToNull(targetName);
                this.targetSignature = targetSignature;
            }

            /**
             * Coalesce schema refresh requests.
             * The algorithm is simple: if more than 2 keyspaces
             * need refresh, then refresh the entire schema;
             * otherwise if more than 2 elements in the same keyspace
             * need refresh, then refresh the entire keyspace.
             *
             * @param that the other request to merge with the current one.
             * @return A coalesced request
             */
            SchemaRefreshRequest coalesce(SchemaRefreshRequest that) {
                if (this.targetType == null || that.targetType == null)
                    return new SchemaRefreshRequest(null, null, null, null);
                if (!this.targetKeyspace.equals(that.targetKeyspace))
                    return new SchemaRefreshRequest(null, null, null, null);
                if (this.targetName == null || that.targetName == null)
                    return new SchemaRefreshRequest(KEYSPACE, targetKeyspace, null, null);
                if (!this.targetName.equals(that.targetName))
                    return new SchemaRefreshRequest(KEYSPACE, targetKeyspace, null, null);
                return this;
            }

            @Override
            public String toString() {
                if (this.targetType == null)
                    return "Refresh ALL";
                if (this.targetName == null)
                    return "Refresh keyspace " + targetKeyspace;
                return String.format("Refresh %s %s.%s", targetType, targetKeyspace, targetName);
            }
        }

        private class SchemaRefreshRequestDeliveryCallback implements EventDebouncer.DeliveryCallback<SchemaRefreshRequest> {

            @Override
            public ListenableFuture<?> deliver(final List<SchemaRefreshRequest> events) {
                return executor.submit(new ExceptionCatchingRunnable() {
                    @Override
                    public void runMayThrow() throws InterruptedException, ExecutionException {
                        SchemaRefreshRequest coalesced = null;
                        for (SchemaRefreshRequest request : events) {
                            coalesced = coalesced == null ? request : coalesced.coalesce(request);
                        }
                        assert coalesced != null;
                        logger.trace("Coalesced schema refresh request: {}", coalesced);
                        controlConnection.refreshSchema(coalesced.targetType, coalesced.targetKeyspace, coalesced.targetName, coalesced.targetSignature);
                    }
                });
            }

        }

        private class NodeRefreshRequest {

            private final InetSocketAddress address;

            private final HostEvent eventType;

            private NodeRefreshRequest(InetSocketAddress address, HostEvent eventType) {
                this.address = address;
                this.eventType = eventType;
            }

            @Override
            public String toString() {
                return address + " " + eventType;
            }

        }

        private class NodeRefreshRequestDeliveryCallback implements EventDebouncer.DeliveryCallback<NodeRefreshRequest> {

            @Override
            public ListenableFuture<?> deliver(List<NodeRefreshRequest> events) {
                Map<InetSocketAddress, HostEvent> hosts = new HashMap<InetSocketAddress, HostEvent>();
                // only keep the last event for each host
                for (NodeRefreshRequest req : events) {
                    hosts.put(req.address, req.eventType);
                }
                List<ListenableFuture<?>> futures = new ArrayList<ListenableFuture<?>>(hosts.size());
                for (final Entry<InetSocketAddress, HostEvent> entry : hosts.entrySet()) {
                    InetSocketAddress address = entry.getKey();
                    HostEvent eventType = entry.getValue();
                    switch (eventType) {
                        case UP:
                            Host upHost = metadata.getHost(address);
                            if (upHost == null) {
                                upHost = metadata.add(address);
                                // If upHost is still null, it means we didn't know about it the line before but
                                // got beaten at adding it to the metadata by another thread. In that case, it's
                                // fine to let the other thread win and ignore the notification here
                                if (upHost == null)
                                    continue;
                                futures.add(schedule(hostAdded(upHost)));
                            } else {
                                futures.add(schedule(hostUp(upHost)));
                            }
                            break;
                        case ADDED:
                            Host newHost = metadata.add(address);
                            if (newHost != null) {
                                futures.add(schedule(hostAdded(newHost)));
                            } else {
                                // If host already existed, retrieve it and check its state, if it's not up schedule a
                                // hostUp event.
                                Host existingHost = metadata.getHost(address);
                                if (!existingHost.isUp())
                                    futures.add(schedule(hostUp(existingHost)));
                            }
                            break;
                        case DOWN:
                            // Note that there is a slight risk we can receive the event late and thus
                            // mark the host down even though we already had reconnected successfully.
                            // But it is unlikely, and don't have too much consequence since we'll try reconnecting
                            // right away, so we favor the detection to make the Host.isUp method more reliable.
                            Host downHost = metadata.getHost(address);
                            if (downHost != null) {
                                // Only process DOWN events if we have no active connections to the host . Otherwise, we
                                // wait for the connections to fail. This is to prevent against a bad control host
                                // aggressively marking DOWN all of its peers.
                                if (downHost.convictionPolicy.hasActiveConnections()) {
                                    logger.debug("Ignoring down event on {} because it still has active connections", downHost);
                                } else {
                                    futures.add(execute(hostDown(downHost)));
                                }
                            }
                            break;
                        case REMOVED:
                            Host removedHost = metadata.getHost(address);
                            if (removedHost != null)
                                futures.add(execute(hostRemoved(removedHost)));
                            break;
                    }
                }
                return Futures.allAsList(futures);
            }

            private ListenableFuture<?> execute(ExceptionCatchingRunnable task) {
                return executor.submit(task);
            }

            private ListenableFuture<?> schedule(final ExceptionCatchingRunnable task) {
                // Cassandra tends to send notifications for new/up nodes a bit early (it is triggered once
                // gossip is up, but that is before the client-side server is up), so we add a delay
                // (otherwise the connection will likely fail and have to be retry which is wasteful).
                // This has been fixed by CASSANDRA-8236 and does not apply to protocol versions >= 4
                // and C* versions >= 2.2.0
                if (protocolVersion().compareTo(ProtocolVersion.V4) < 0) {
                    final SettableFuture<?> future = SettableFuture.create();
                    scheduledTasksExecutor.schedule(new ExceptionCatchingRunnable() {
                        @Override
                        public void runMayThrow() throws Exception {
                            ListenableFuture<?> f = execute(task);
                            Futures.addCallback(f, new FutureCallback<Object>() {
                                @Override
                                public void onSuccess(Object result) {
                                    future.set(null);
                                }

                                @Override
                                public void onFailure(Throwable t) {
                                    future.setException(t);
                                }
                            });
                        }
                    }, NEW_NODE_DELAY_SECONDS, TimeUnit.SECONDS);
                    return future;
                } else {
                    return execute(task);
                }
            }

            // Make sure we  call controlConnection.refreshNodeInfo(host)
            // so that we have up-to-date infos on that host before adding it (so we typically
            // catch that an upgraded node uses a new cassandra version).

            private ExceptionCatchingRunnable hostAdded(final Host host) {
                return new ExceptionCatchingRunnable() {
                    @Override
                    public void runMayThrow() throws Exception {
                        if (controlConnection.refreshNodeInfo(host)) {
                            onAdd(host, null);
                            submitNodeListRefresh();
                        } else {
                            logger.debug("Not enough info for {}, ignoring host", host);
                        }
                    }
                };
            }

            private ExceptionCatchingRunnable hostUp(final Host host) {
                return new ExceptionCatchingRunnable() {
                    @Override
                    public void runMayThrow() throws Exception {
                        if (controlConnection.refreshNodeInfo(host)) {
                            onUp(host, null);
                        } else {
                            logger.debug("Not enough info for {}, ignoring host", host);
                        }
                    }
                };
            }

            private ExceptionCatchingRunnable hostDown(final Host host) {
                return new ExceptionCatchingRunnable() {
                    @Override
                    public void runMayThrow() throws Exception {
                        onDown(host, false, true);
                    }
                };
            }

            private ExceptionCatchingRunnable hostRemoved(final Host host) {
                return new ExceptionCatchingRunnable() {
                    @Override
                    public void runMayThrow() throws Exception {
                        if (metadata.remove(host)) {
                            logger.info("Cassandra host {} removed", host);
                            onRemove(host);
                            submitNodeListRefresh();
                        }
                    }
                };
            }

        }

        private class NodeListRefreshRequest {
            @Override
            public String toString() {
                return "Refresh node list and token map";
            }
        }

        private class NodeListRefreshRequestDeliveryCallback implements EventDebouncer.DeliveryCallback<NodeListRefreshRequest> {

            @Override
            public ListenableFuture<?> deliver(List<NodeListRefreshRequest> events) {
                // The number of received requests does not matter
                // as long as one request is made, refresh the entire node list
                return executor.submit(new ExceptionCatchingRunnable() {
                    @Override
                    public void runMayThrow() throws InterruptedException, ExecutionException {
                        controlConnection.refreshNodeListAndTokenMap();
                    }
                });
            }
        }

    }

    private enum HostEvent {
        UP, DOWN, ADDED, REMOVED
    }

    /**
     * Periodically ensures that closed connections are properly terminated once they have no more pending requests.
     * <p/>
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

        ConnectionReaper(ScheduledExecutorService executor) {
            this.executor = executor;
            this.executor.scheduleWithFixedDelay(reaperTask, INTERVAL_MS, INTERVAL_MS, TimeUnit.MILLISECONDS);
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
