package com.datastax.driver.core;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;

import com.datastax.driver.core.transport.Connection;
import com.datastax.driver.core.transport.ConnectionException;
import com.datastax.driver.core.utils.SimpleConvictionPolicy;
import com.datastax.driver.core.utils.RoundRobinPolicy;

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

    // TODO: Make that configurable
    private final ConvictionPolicy.Factory convictionPolicyFactory = new SimpleConvictionPolicy.Factory();
    final Manager manager;

    private Cluster(List<InetSocketAddress> contactPoints) {
        List<Host> hosts = new ArrayList<Host>(contactPoints.size());
        for (InetSocketAddress address : contactPoints)
            hosts.add(new Host(address, convictionPolicyFactory));
        this.manager = new Manager(hosts);
    }

    /**
     * Build a new cluster based on the provided configuration.
     *
     * Note that for building a cluster programmatically, Cluster.Builder
     * provides a slightly less verbose shortcut with {@link Builder#build}.
     *
     * @param config the Cluster.Configuration to use
     * @return the newly created Cluster instance
     */
    public static Cluster buildFrom(Configuration config) {
        return new Cluster(config.contactPoints());
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
    public Session connect(AuthInfo authInfo) {
        // TODO
        return null;
    }

    /**
     * Creates a new session on this cluster and sets a keyspace to use.
     *
     * @param keyspace The name of the keyspace to use for the created
     * {@code Session}. This can be later changed using {@link Session#use}.
     * @return a new session on this cluster sets to keyspace
     * {@code keyspaceName}.
     */
    public Session connect(String keyspace) {
        return connect().use(keyspace);
    }

    /**
     * Creates a new session on this cluster and sets a keyspace to use.
     *
     * @param authInfo The authorisation credentials to use to connect to
     * Cassandra nodes.
     * @return a new session on this cluster sets to keyspace
     * {@code keyspaceName}.
     */
    public Session connect(String keyspace, AuthInfo authInfo) {
        return connect(authInfo).use(keyspace);
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
        public List<InetSocketAddress> contactPoints();
    }

    /**
     * Helper class to build {@link Cluster} instances.
     */
    public static class Builder implements Configuration {

        // TODO: might not be the best default port, look at changing in C*
        private static final int DEFAULT_PORT = 8000;

        private List<InetSocketAddress> addresses = new ArrayList<InetSocketAddress>();

        public List<InetSocketAddress> contactPoints() {
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

        public Cluster build() {
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
    class Manager implements Host.StateListener {

        // Initial contacts point
        private final List<Host> contactPoints;

        private final Set<Session> sessions = new CopyOnWriteArraySet<Session>();

        final Connection.Factory connectionFactory = new Connection.Factory();

        // TODO: make configurable
        final LoadBalancingPolicy.Factory loadBalancingFactory = new RoundRobinPolicy.Factory();

        private final ScheduledExecutorService scheduledExecutor = Executors.newScheduledThreadPool(2);

        private Manager(List<Host> contactPoints) {
            this.contactPoints = contactPoints;

            // TODO: this probably belong some place else
            for (Host host : contactPoints)
                host.monitor().register(this);
        }

        private Session newSession() {
            Session session = new Session(Cluster.this, contactPoints);
            sessions.add(session);
            return session;
        }

        public void onUp(Host host) {
            // Nothing specific
            // TODO: We should register reconnection attempts, to avoid starting two of
            // them and if this method is called by other means that the
            // reconnection handler (like C* tells us it's up), cancel the latter
        }

        public void onDown(Host host) {
            // Note: we'll basically waste the first successful reconnection that way, but it's probably not a big deal
            logger.debug(String.format("%s is down, scheduling connection retries", host));
            new ReconnectionHandler(host, scheduledExecutor, connectionFactory).start();
        }

        public void onAdd(Host host) {
            // TODO
        }

        public void onRemove(Host host) {
            // TODO
        }
    }
}
