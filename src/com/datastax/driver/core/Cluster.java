package com.datastax.driver.core;

/**
 * Informations and known state of a Cassandra cluster.
 * <p>
 * This is the main entry point of the driver. A simple example of access to a
 * Cassandra cluster would be:
 * <code>
 *   Cluster cluster = Cluster.Builder().addContactPoint("192.168.0.1").build();
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

    /**
     * Creates a new session on this cluster.
     *
     * @return a new session on this cluster sets to no keyspace.
     */
    public Session connect() {
        return null;
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
     * @param keyspaceName The name of the keyspace to use for the created
     * <code>Session</code>. This can be later changed using {@link Session#use}.
     * @return a new session on this cluster sets to keyspace
     * <code>keyspaceName</code>.
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
     * <code>keyspaceName</code>.
     */
    public Session connect(String keyspace, AuthInfo authInfo) {
        return null;
    }

    public class Builder {
        // TODO
    }
}
