package com.datastax.driver.core;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.cassandra.utils.MD5Digest;
import org.apache.cassandra.transport.Event;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.messages.EventMessage;
import org.apache.cassandra.transport.messages.PrepareMessage;
import org.apache.cassandra.transport.messages.QueryMessage;

import com.datastax.driver.core.exceptions.*;
import com.datastax.driver.core.configuration.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.PatternLayout;

/**
 * Informations and known state of a Cassandra cluster.
 * <p>
 * This is the main entry point of the driver. A simple example of access to a
 * Cassandra cluster would be:
 * <pre>
 *   Cluster cluster = new Cluster.Builder().addContactPoint("192.168.0.1").build();
 *   Session session = cluster.connect("db1");
 *
 *   for (CQLRow row : session.execute("SELECT * FROM table1"))
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

    static {
        org.apache.log4j.Logger rootLogger = org.apache.log4j.Logger.getRootLogger();
        if (!rootLogger.getAllAppenders().hasMoreElements()) {
            rootLogger.setLevel(Level.DEBUG);
            rootLogger.addAppender(new ConsoleAppender(new PatternLayout("%-5p [%t]: %m%n")));
        }
    }

    /**
     * The default cassandra port for the native client protocol.
     */
    public static final int DEFAULT_PORT = 9042;

    final Manager manager;

    private Cluster(List<InetSocketAddress> contactPoints, int port, Policies policies, AuthInfoProvider authProvider) throws NoHostAvailableException {
        this.manager = new Manager(contactPoints, port, policies, authProvider);
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
        List<InetSocketAddress> contactPoints = initializer.getContactPoints();
        if (contactPoints.isEmpty())
            throw new IllegalArgumentException("Cannot build a cluster without contact points");

        int port = -1;
        for (InetSocketAddress a : contactPoints) {
            if (port != -1 && a.getPort() != port)
                throw new IllegalArgumentException(String.format("Not all hosts have the same port, found port %d and %d", port, a.getPort()));
            port = a.getPort();
        }
        return new Cluster(contactPoints, port, initializer.getPolicies(), initializer.getAuthInfoProvider());
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
    public ClusterMetadata getMetadata() {
        return manager.metadata;
    }

    /**
     * The cluster configuration.
     *
     * @return the cluster configuration.
     */
    public Cluster.Configuration getConfiguration() {
        return manager.configuration;
    }

    /**
     * Shutdown this cluster instance.
     *
     * This closes all connections from all the sessions of this {@code
     * Cluster} instance and reclam all ressources used by it.
     * <p>
     * This method has no effect if the cluster was already shutdown.
     */
    public void shutdown() {
        manager.shutdown();
    }

    /**
     * Initializer for {@link Cluster} instances.
     */
    public interface Initializer {

        /**
         * Returns the initial Cassandra hosts to connect to.
         *
         * @return the initial Cassandra contact points. See {@link Builder#addContactPoint}
         * for more details on contact points.
         */
        public List<InetSocketAddress> getContactPoints();

        /**
         * Returns the policies to use for this cluster.
         *
         * @return the policies to use for this cluster.
         */
        public Policies getPolicies();

        /**
         * The authentication provider to use to connect to the Cassandra cluster.
         *
         * @return the authentication provider to use. Use
         * AuthInfoProvider.NONE if authentication is not to be used.
         */
        public AuthInfoProvider getAuthInfoProvider();
    }

    /**
     * Helper class to build {@link Cluster} instances.
     */
    public static class Builder implements Initializer {

        private final List<InetAddress> addresses = new ArrayList<InetAddress>();
        private int port = DEFAULT_PORT;
        private AuthInfoProvider authProvider = AuthInfoProvider.NONE;

        private LoadBalancingPolicy.Factory loadBalancingPolicyFactory;
        private ReconnectionPolicy.Factory reconnectionPolicyFactory;
        private RetryPolicy retryPolicy;

        public List<InetSocketAddress> getContactPoints() {
            List<InetSocketAddress> cp = new ArrayList<InetSocketAddress>(addresses.size());
            for (InetAddress address : addresses)
                cp.add(new InetSocketAddress(address, port));
            return cp;
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
         * Configure the load balancing policy (factory) to use for the new cluster.
         * <p>
         * If no load balancing policy factory is set through this method,
         * {@link Policies#DEFAULT_LOAD_BALANCING_POLICY_FACTORY} will be used instead.
         *
         * @param factory the load balancing policy factory to use
         * @return this Builder
         */
        public Builder withLoadBalancingPolicyFactory(LoadBalancingPolicy.Factory factory) {
            this.loadBalancingPolicyFactory = factory;
            return this;
        }

        /**
         * Configure the reconnection policy (factory) to use for the new cluster.
         * <p>
         * If no reconnection policy factory is set through this method,
         * {@link Policies#DEFAULT_RECONNECTION_POLICY_FACTORY} will be used instead.
         *
         * @param factory the reconnection policy factory to use
         * @return this Builder
         */
        public Builder withReconnectionPolicyFactory(ReconnectionPolicy.Factory factory) {
            this.reconnectionPolicyFactory = factory;
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
         * Returns the policies to use for this cluster.
         * <p>
         * The policies used are the one set by the {@code with*} methods of
         * this builder, or the default ones defined in {@link Policies} for
         * the policies that hasn't been explicitely set.
         *
         * @return the policies to use for this cluster.
         */
        public Policies getPolicies() {
            return new Policies(
                loadBalancingPolicyFactory == null ? Policies.DEFAULT_LOAD_BALANCING_POLICY_FACTORY : loadBalancingPolicyFactory,
                reconnectionPolicyFactory == null ? Policies.DEFAULT_RECONNECTION_POLICY_FACTORY : reconnectionPolicyFactory,
                retryPolicy == null ? Policies.DEFAULT_RETRY_POLICY : retryPolicy
            );
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
         * The authentication provider to use to connect to the Cassandra cluster.
         *
         * @return the authentication provider set through {@link #withAuthInfoProvider}
         * or AuthInfoProvider.NONE if nothing was set.
         */
        public AuthInfoProvider getAuthInfoProvider() {
            return this.authProvider;
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

    /**
     * The configuration of the cluster.
     */
    public static class Configuration {

        private final Policies policies;
        private final ConnectionsConfiguration connections = new ConnectionsConfiguration();

        private Configuration(Policies policies) {
            this.policies = policies;
        }

        /**
         * The policies set for the cluster.
         *
         * @return the policies set for the cluster.
         */
        public Policies getPolicies() {
            return policies;
        }

        /**
         * Configuration related to the connections the driver maintains to the
         * Cassandra hosts.
         *
         * @return the configuration of the connections to Cassandra hosts.
         */
        public ConnectionsConfiguration getConnectionsConfiguration() {
            return connections;
        }
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
        final List<InetSocketAddress> contactPoints;
        final int port;
        private final Set<Session> sessions = new CopyOnWriteArraySet<Session>();

        final ClusterMetadata metadata;
        final Configuration configuration;

        final Connection.Factory connectionFactory;
        private final ControlConnection controlConnection;

        final ConvictionPolicy.Factory convictionPolicyFactory = new ConvictionPolicy.Simple.Factory();

        final ScheduledExecutorService reconnectionExecutor = Executors.newScheduledThreadPool(2, new NamedThreadFactory("Reconnection"));
        final ScheduledExecutorService scheduledTasksExecutor = Executors.newScheduledThreadPool(1, new NamedThreadFactory("Scheduled Tasks"));

        final ExecutorService executor = Executors.newCachedThreadPool(new NamedThreadFactory("Cassandra Java Driver worker"));

        final AtomicBoolean isShutdown = new AtomicBoolean(false);

        // All the queries that have been prepared (we keep them so we can
        // re-prepared them when a node fail or a new one join the cluster).
        // Note: we could move this down to the session level, but since
        // prepared statement are global to a node, this would yield a slightly
        // less clear behavior.
        final Map<MD5Digest, String> preparedQueries = new ConcurrentHashMap<MD5Digest, String>();

        private Manager(List<InetSocketAddress> contactPoints, int port, Policies policies, AuthInfoProvider authProvider) throws NoHostAvailableException {
            this.port = port;
            this.configuration = new Configuration(policies);
            this.metadata = new ClusterMetadata(this);
            this.contactPoints = contactPoints;
            this.connectionFactory = new Connection.Factory(this, authProvider);

            for (InetSocketAddress address : contactPoints)
                addHost(address, false);

            this.controlConnection = new ControlConnection(this);
            controlConnection.connect();
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
        }

        public void onUp(Host host) {
            logger.trace(String.format("Host %s is UP", host));

            // If there is a reconnection attempt scheduled for that node, cancel it
            ScheduledFuture scheduledAttempt = host.reconnectionAttempt.getAndSet(null);
            if (scheduledAttempt != null)
                scheduledAttempt.cancel(false);

            prepareAllQueries(host);

            controlConnection.onUp(host);
            for (Session s : sessions)
                s.manager.onUp(host);
        }

        public void onDown(final Host host) {
            logger.trace(String.format("Host %s is DOWN", host));
            controlConnection.onDown(host);
            for (Session s : sessions)
                s.manager.onDown(host);

            // Note: we basically waste the first successful reconnection, but it's probably not a big deal
            logger.debug(String.format("%s is down, scheduling connection retries", host));
            new AbstractReconnectionHandler(reconnectionExecutor, configuration.getPolicies().getReconnectionPolicyFactory().create(), host.reconnectionAttempt) {

                protected Connection tryReconnect() throws ConnectionException {
                    return connectionFactory.open(host);
                }

                protected void onReconnection(Connection connection) {
                    logger.debug(String.format("Successful reconnection to %s, setting host UP", host));
                    host.getMonitor().reset();
                }

                protected boolean onConnectionException(ConnectionException e, long nextDelayMs) {
                    logger.debug(String.format("Failed reconnection to %s (%s), scheduling retry in %d milliseconds", host, e.getMessage(), nextDelayMs));
                    return true;
                }

                protected boolean onUnknownException(Exception e, long nextDelayMs) {
                    logger.error(String.format("Unknown error during control connection reconnection, scheduling retry in %d milliseconds", nextDelayMs), e);
                    return true;
                }

            }.start();
        }

        public void onAdd(Host host) {
            logger.trace(String.format("Adding new host %s", host));
            prepareAllQueries(host);
            controlConnection.onAdd(host);
            for (Session s : sessions)
                s.manager.onAdd(host);
        }

        public void onRemove(Host host) {
            logger.trace(String.format("Removing host %s", host));
            controlConnection.onRemove(host);
            for (Session s : sessions)
                s.manager.onRemove(host);
        }

        public Host addHost(InetSocketAddress address, boolean signal) {
            Host newHost = metadata.add(address);
            if (newHost != null && signal) {
                logger.info(String.format("New Cassandra host %s added", newHost));
                onAdd(newHost);
            }
            return newHost;
        }

        public void removeHost(Host host) {
            if (host == null)
                return;

            if (metadata.remove(host)) {
                logger.info(String.format("Cassandra host %s removed", host));
                onRemove(host);
            }
        }

        // Prepare a query on all nodes
        public void prepare(MD5Digest digest, String query, InetSocketAddress toExclude) {
            preparedQueries.put(digest, query);
            for (Session s : sessions)
                s.manager.prepare(query, toExclude);
        }

        private void prepareAllQueries(Host host) {
            if (preparedQueries.isEmpty())
                return;

            try {
                Connection connection = connectionFactory.open(host);
                List<Connection.Future> futures = new ArrayList<Connection.Future>(preparedQueries.size());
                for (String query : preparedQueries.values()) {
                    futures.add(connection.write(new PrepareMessage(query)));
                }
                for (Connection.Future future : futures) {
                    try {
                        future.get();
                    } catch (InterruptedException e) {
                        logger.debug("Interupted while preparing queries on new/newly up host", e);
                    } catch (ExecutionException e) {
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
                    controlConnection.refreshSchema(keyspace, table);
                }
            });
        }

        // refresh the schema using the provided connection, and notice the future with the provided resultset once done
        public void refreshSchema(final Connection connection, final SimpleFuture future, final ResultSet rs, final String keyspace, final String table) {
            // TODO: figure out why this doesn't work
            //logger.debug("Refreshing schema for {}{}", keyspace == null ? "" : keyspace, table == null ? "" : "." + table);
            executor.submit(new Runnable() {
                public void run() {
                    try {
                        ControlConnection.refreshSchema(connection, keyspace, table, Cluster.Manager.this);
                    } catch (Exception e) {
                        logger.error("Error during schema refresh ({}). The schema from Cluster.getMetadata() migth appear stale. Asynchronously submitting job to fix.", e.getMessage());
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
                logger.error("Received an unexpected message from the server: " + response);
                return;
            }

            final Event event = ((EventMessage)response).event;

            logger.trace(String.format("Received event %s, scheduling delivery", response));

            // When handle is called, the current thread is a network I/O  thread, and we don't want to block
            // it (typically addHost() will create the connection pool to the new node, which can take time)
            // Besides, events are usually sent a bit too early (since they're
            // triggered once gossip is up, but that before the client-side
            // server is up) so adds a second delay.
            scheduledTasksExecutor.schedule(new Runnable() {
                public void run() {
                    switch (event.type) {
                        case TOPOLOGY_CHANGE:
                            Event.TopologyChange tpc = (Event.TopologyChange)event;
                            switch (tpc.change) {
                                case NEW_NODE:
                                    addHost(tpc.node, true);
                                    break;
                                case REMOVED_NODE:
                                    removeHost(metadata.getHost(tpc.node));
                                    break;
                            }
                            break;
                        case STATUS_CHANGE:
                            Event.StatusChange stc = (Event.StatusChange)event;
                            switch (stc.status) {
                                case UP:
                                    Host host = metadata.getHost(stc.node);
                                    if (host == null) {
                                        // first time we heard about that node apparently, add it
                                        addHost(stc.node, true);
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
