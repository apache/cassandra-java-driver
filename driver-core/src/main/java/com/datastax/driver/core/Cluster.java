package com.datastax.driver.core;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.datastax.driver.core.transport.ConnectionException;

/**
 * Informations and known state of a Cassandra cluster.
 * <p>
 * This is the main entry point of the driver. A simple example of access to a
 * Cassandra cluster would be:
 * <code>
 *   Cluster cluster = new Cluster.Builder().addContactPoints("192.168.0.1").build();
 *   Session session = cluster.connect("db1");
 *
 *   for (CQLRow row : session.execute("SELECT * FROM table1"))
 *       // do something ...
 * </code>
 * <p>
 * A cluster object maintains a permanent connection to one of the cluster node
 * that it uses solely to maintain informations on the state and current
 * topology of the cluster. Using the connection, the driver will discover all
 * the nodes composing the cluster as well as new nodes joining the cluster.
 * You can disable that connection through the disableStateConnection() method.
 * This is however discouraged as it means queries will only ever be executed
 * against node set as contact point. If you want to limit the number of nodes
 * to which this driver connects to, prefer maxConnectedNode().
 */
public class Cluster {

    private final List<InetSocketAddress> contactPoints;

    private Cluster(List<InetSocketAddress> contactPoints) {
        this.contactPoints = contactPoints;
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
        try {
            return new Session(contactPoints);
        } catch (ConnectionException e) {
            // TODO: Figure what exception we want to return (but maybe the ConnectionException is good enough)
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates a new session on this cluster.
     *
     * @param authInfo The authorisation credentials to use to connect to
     * Cassandra nodes.
     * @return a new session on this cluster sets to no keyspace.
     */
    public Session connect(AuthInfo authInfo) {
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
        return null;
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
        return null;
    }

    public interface Configuration {

        public List<InetSocketAddress> contactPoints();
    }

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
}
