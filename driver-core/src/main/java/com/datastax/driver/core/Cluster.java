package com.datastax.driver.core;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;

import org.apache.cassandra.utils.MD5Digest;
import org.apache.cassandra.transport.Event;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.messages.EventMessage;
import org.apache.cassandra.transport.messages.PrepareMessage;
import org.apache.cassandra.transport.messages.QueryMessage;

import com.datastax.driver.core.exceptions.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Informations and known state of a Cassandra cluster.
 * <p>
 * This is the main entry point of the driver. A simple example of access to a
 * Cassandra cluster would be:
 * <pre>
 *   Cluster cluster = new Cluster.Builder().addContactPoints("192.168.0.1").build();
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

    /**
     * The default cassandra port for the native client protocol.
     */
    public static final int DEFAULT_PORT = 9042;

    final Manager manager;

    private Cluster(List<InetSocketAddress> contactPoints) throws NoHostAvailableException {
        this.manager = new Manager(contactPoints);
    }

    /**
     * Build a new cluster based on the provided configuration.
     *
     * Note that for building a cluster programmatically, Cluster.Builder
     * provides a slightly less verbose shortcut with {@link Builder#build}.
     *
     * @param config the Cluster.Configuration to use
     * @return the newly created Cluster instance
     *
     * @throws NoHostAvailableException if no host amongst the contact points
     * can be reached.
     */
    public static Cluster buildFrom(Configuration config) throws NoHostAvailableException {
        return new Cluster(config.getContactPoints());
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
     * Creates a new session on this cluster.
     *
     * @param authInfo The authorisation credentials to use to connect to
     * Cassandra nodes.
     * @return a new session on this cluster sets to no keyspace.
     */
    // TODO
    //public Session connect(AuthInfo authInfo) {
    //    return null;
    //}

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
     * Creates a new session on this cluster and sets a keyspace to use.
     *
     * @param authInfo The authorisation credentials to use to connect to
     * Cassandra nodes.
     * @return a new session on this cluster sets to keyspace
     * {@code keyspaceName}.
     *
     * @throws NoHostAvailableException if no host can be contacted to set the
     * {@code keyspace}.
     */
    //    Session session = connect(authInfo);
    //    session.manager.setKeyspace(keyspace);
    //    return session;
    //}

    /**
     * Returns read-only metadata on the connected cluster.
     *
     * This includes the know nodes (with their status as seen by the driver)
     * as well as the schema definitions.
     *
     * @return the cluster metadata.
     */
    public ClusterMetadata getMetadata() {
        return manager.metadata;
    }

    /**
     * Configuration for {@link Cluster} instances.
     */
    public interface Configuration {

        /**
         * Returns the initial Cassandra hosts to connect to.
         *
         * @return the initial Cassandra contact points. See {@link Builder#addContactPoint}
         * for more details on contact points.
         */
        public List<InetSocketAddress> getContactPoints();
    }

    /**
     * Helper class to build {@link Cluster} instances.
     */
    public static class Builder implements Configuration {

        private List<InetSocketAddress> addresses = new ArrayList<InetSocketAddress>();

        public List<InetSocketAddress> getContactPoints() {
            return addresses;
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
         * @param port the port to connect to
         * @return this Builder
         *
         * @throws IllegalArgumentException if the port parameter is outside
         * the range of valid port values, or if the hostname parameter is
         * null.
         * @throws SecurityException if a security manager is present and
         * permission to resolve the host name is denied.
         */
        public Builder addContactPoint(String address, int port) {
            this.addresses.add(new InetSocketAddress(address, port));
            return this;
        }

        /**
         * Add a contact point using the default Cassandra port.
         *
         * See {@link Builder#addContactPoint} for more details on contact
         * points.
         *
         * @param address the address of the node to add as contact point
         * @return this Builder
         *
         * @throws SecurityException if a security manager is present and
         * permission to resolve the host name is denied.
         *
         * @see Builder#addContactPoint
         */
        public Builder addContactPoint(String address) {
            return addContactPoint(address, DEFAULT_PORT);
        }

        /**
         * Add contact points using the default Cassandra port.
         *
         * See {@link Builder#addContactPoint} for more details on contact
         * points.
         *
         * @param addresses addresses of the nodes to add as contact point
         * @return this Builder
         *
         * @throws SecurityException if a security manager is present and
         * permission to resolve the host name is denied.
         *
         * @see Builder#addContactPoint
         */
        public Builder addContactPoints(String... addresses) {
            for (String address : addresses)
                addContactPoint(address, DEFAULT_PORT);
            return this;
        }

        /**
         * Add contact points using the default Cassandra port.
         *
         * See {@link Builder#addContactPoint} for more details on contact
         * points.
         *
         * @param addresses addresses of the nodes to add as contact point
         * @return this Builder
         *
         * @throws SecurityException if a security manager is present and
         * permission to resolve the host name is denied.
         *
         * @see Builder#addContactPoint
         */
        public Builder addContactPoints(InetAddress... addresses) {
            for (InetAddress address : addresses)
                this.addresses.add(new InetSocketAddress(address, DEFAULT_PORT));
            return this;
        }

        /**
         * Add contact points.
         *
         * See {@link Builder#addContactPoint} for more details on contact
         * points.
         *
         * @param addresses the socket addresses of the nodes to add as contact
         * point
         * @return this Builder
         *
         * @throws SecurityException if a security manager is present and
         * permission to resolve the host name is denied.
         *
         * @see Builder#addContactPoint
         */
        public Builder addContactPoints(InetSocketAddress... addresses) {
            this.addresses.addAll(Arrays.asList(addresses));
            return this;
        }

        /**
         * Build the cluster with the configured set of initial contact points.
         *
         * This is a shorthand for {@code Cluster.buildFrom(this)}.
         *
         * @return the newly build Cluster instance.
         *
         * @throws NoHostAvailableException if none of the contact points
         * provided can be reached.
         */
        public Cluster build() throws NoHostAvailableException {
            return Cluster.buildFrom(this);
        }
    }

    /**
     * The sessions and hosts managed by this a Cluster instance.
     *
     * Note: the reason we create a Manager object separate from Cluster is
     * that Manager is not publicly visible. For instance, we wouldn't want
     * user to be able to call the {@link #onUp} and {@link #onDown} methods.
     */
    class Manager implements Host.StateListener, Connection.DefaultResponseHandler {

        // Initial contacts point
        final List<InetSocketAddress> contactPoints;

        private final Set<Session> sessions = new CopyOnWriteArraySet<Session>();
        final ClusterMetadata metadata;

        // TODO: Make that configurable
        final ConvictionPolicy.Factory convictionPolicyFactory = new SimpleConvictionPolicy.Factory();
        final ReconnectionPolicy.Factory reconnectionPolicyFactory = ReconnectionPolicy.Exponential.makeFactory(2 * 1000, 5 * 60 * 1000);
        final Connection.Factory connectionFactory;
        private final ControlConnection controlConnection;

        // TODO: make configurable
        final LoadBalancingPolicy.Factory loadBalancingFactory = LoadBalancingPolicy.RoundRobin.Factory.INSTANCE;

        final ScheduledExecutorService reconnectionExecutor = Executors.newScheduledThreadPool(2, new NamedThreadFactory("Reconnection"));
        final ScheduledExecutorService scheduledTasksExecutor = Executors.newScheduledThreadPool(1, new NamedThreadFactory("Scheduled Tasks"));

        // TODO: give a name to the threads of this executor
        final ExecutorService executor = Executors.newCachedThreadPool(new NamedThreadFactory("Cassandra Java Driver worker"));

        // All the queries that have been prepared (we keep them so we can
        // re-prepared them when a node fail or a new one join the cluster).
        // Note: we could move this down to the session level, but since
        // prepared statement are global to a node, this would yield a slightly
        // less clear behavior.
        final Map<MD5Digest, String> preparedQueries = new ConcurrentHashMap<MD5Digest, String>();

        private Manager(List<InetSocketAddress> contactPoints) throws NoHostAvailableException {
            this.metadata = new ClusterMetadata(this);
            this.contactPoints = contactPoints;
            this.connectionFactory = new Connection.Factory(this);

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
            new AbstractReconnectionHandler(reconnectionExecutor, reconnectionPolicyFactory.create(), host.reconnectionAttempt) {

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
            if (newHost != null && signal)
                onAdd(newHost);
            return newHost;
        }

        public void removeHost(Host host) {
            if (host == null)
                return;

            if (metadata.remove(host))
                onRemove(host);
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
            }
        }

        // TODO: take a lot or something so that if a a getSchema() is called,
        // we wait for that to be finished. And maybe avoid multiple refresh at
        // the same time.
        public void submitSchemaRefresh(final String keyspace, final String table) {
            executor.submit(new Runnable() {
                public void run() {
                    controlConnection.refreshSchema(keyspace, table);
                }
            });
        }

        // Called when some message has been received but has been initiated from the server (streamId < 0).
        public void handle(Message.Response response) {

            if (!(response instanceof EventMessage)) {
                // TODO: log some error
                return;
            }

            final Event event = ((EventMessage)response).event;

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
