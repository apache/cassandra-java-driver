package com.datastax.driver.core;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;

import com.datastax.driver.core.pool.HostConnectionPool;
import com.datastax.driver.core.transport.Connection;
import com.datastax.driver.core.transport.ConnectionException;

import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.messages.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A session holds connections to a Cassandra cluster, allowing to query it.
 *
 * Each session will maintain multiple connections to the cluster nodes, and
 * provides policies to choose which node to use for each query (round-robin on
 * all nodes of the cluster by default), handles retries for failed query (when
 * it makes sense), etc...
 * <p>
 * Session instances are thread-safe and usually a single instance is enough
 * per application. However, a given session can only use set to one keyspace
 * at a time, so this is really more one instance per keyspace used.
 */
public class Session {

    private static final Logger logger = LoggerFactory.getLogger(Session.class);

    private final Manager manager;

    // Package protected, only Cluster should construct that.
    Session(Cluster cluster, List<Host> hosts) {
        this.manager = new Manager(cluster, hosts);
    }

    /**
     * Sets the current keyspace to use for this session.
     *
     * Note that it is up to the application to synchronize calls to this
     * method with queries executed against this session.
     *
     * @param keyspace the name of the keyspace to set
     * @return this session.
     */
    public Session use(String keyspace) {
        manager.setKeyspace(keyspace);
        return this;
    }

    /**
     * Execute the provided query.
     *
     * This method blocks until at least some result has been received from the
     * database. However, for SELECT queries, it does not guarantee that the
     * result has been received in full. But it does guarantee that some
     * response has been received from the database, and in particular
     * guarantee that if the request is invalid, an exception will be thrown
     * by this method.
     *
     * @param query the CQL query to execute
     * @return the result of the query. That result will never be null be can
     * be empty and will be for any non SELECT query.
     */
    public ResultSet execute(String query) {
        // TODO: Deal with exceptions
        try {
            return toResultSet(manager.execute(new QueryMessage(query)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Execute the provided query.
     *
     * This method works exactly as {@link #execute(String)}.
     *
     * @param query the CQL query to execute
     * @return the result of the query. That result will never be null be can
     * be empty and will be for any non SELECT query.
     *
     * @see #execute(String)
     */
    public ResultSet execute(CQLQuery query) {
        return execute(query.toString());
    }

    /**
     * Execute the provided query asynchronously.
     *
     * This method does not block. It returns as soon as the query has been
     * successfully sent to a Cassandra node. In particular, returning from
     * this method does not guarantee that the query is valid. Any exception
     * pertaining to the failure of the query will be thrown by the first
     * access to the {@link ResultSet}.
     *
     * Note that for queries that doesn't return a result (INSERT, UPDATE and
     * DELETE), you will need to access the ResultSet (i.e. call any of its
     * method) to make sure the query was successful.
     *
     * @param query the CQL query to execute
     * @return the result of the query. That result will never be null be can
     * be empty and will be for any non SELECT query.
     */
    public ResultSet.Future executeAsync(String query) {
        return null;
    }

    /**
     * Execute the provided query asynchronously.
     *
     * This method works exactly as {@link #executeAsync(String)}.
     *
     * @param query the CQL query to execute
     * @return the result of the query. That result will never be null be can
     * be empty and will be for any non SELECT query.
     *
     * @see #executeAsync(String)
     */
    public ResultSet.Future executeAsync(CQLQuery query) {
        return null;
    }

    /**
     * Prepare the provided query.
     *
     * @param query the CQL query to prepare
     * @return the prepared statement corresponding to {@code query}.
     */
    public PreparedStatement prepare(String query) {
        // TODO: Deal with exceptions
        try {
            return toPreparedStatement(manager.execute(new PrepareMessage(query)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Prepare the provided query.
     *
     * @param query the CQL query to prepare
     * @return the prepared statement corresponding to {@code query}.
     *
     * @see #prepare(String)
     */
    public PreparedStatement prepare(CQLQuery query) {
        return prepare(query.toString());
    }

    /**
     * Execute a prepared statement that had values provided for its bound
     * variables.
     *
     * This method performs like {@link #execute} but for prepared statements.
     * It blocks until at least some result has been received from the
     * database.
     *
     * @param stmt the prepared statement with values for its bound variables.
     * @return the result of the query. That result will never be null be can
     * be empty and will be for any non SELECT query.
     */
    public ResultSet executePrepared(BoundStatement stmt) {
        // TODO: Deal with exceptions
        try {
            return toResultSet(manager.execute(new ExecuteMessage(stmt.statement.id, Arrays.asList(stmt.values))));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Execute a prepared statement that had values provided for its bound
     * variables asynchronously.
     *
     * This method performs like {@link #executeAsync} but for prepared
     * statements. It return as soon as the query has been successfully sent to
     * the database.
     *
     * @param stmt the prepared statement with values for its bound variables.
     * @return the result of the query. That result will never be null be can
     * be empty and will be for any non SELECT query.
     */
    public ResultSet.Future executePreparedAsync(BoundStatement stmt) {
        return null;
    }

    private ResultSet toResultSet(Connection.Future future) {
        try {
            Message.Response response = future.get();
            switch (response.type) {
                case RESULT:
                    return ResultSet.fromMessage((ResultMessage)response);
                case ERROR:
                    // TODO: handle errors
                    logger.info("Got " + response);
                    return null;
                default:
                    // TODO: handle errors (set the connection to defunct as this mean it is in a bad state)
                    logger.info("Got " + response);
                    return null;
            }
        } catch (Exception e) {
            // TODO: do better
            throw new RuntimeException(e);
        }
    }

    private PreparedStatement toPreparedStatement(Connection.Future future) {
        try {
            Message.Response response = future.get();
            switch (response.type) {
                case RESULT:
                    return PreparedStatement.fromMessage((ResultMessage)response);
                case ERROR:
                    // TODO: handle errors
                    logger.info("Got " + response);
                    return null;
                default:
                    // TODO: handle errors (set the connection to defunct as this mean it is in a bad state)
                    logger.info("Got " + response);
                    return null;
            }
        } catch (Exception e) {
            // TODO: do better
            throw new RuntimeException(e);
        }
    }

    private static class Manager implements Host.StateListener {

        private final Cluster cluster;

        private final ConcurrentMap<Host, HostConnectionPool> pools;
        private final LoadBalancingPolicy loadBalancer;

        private final HostConnectionPool.Configuration poolsConfiguration;

        // TODO: Make that configurable
        private final long DEFAULT_CONNECTION_TIMEOUT = 3000;

        public Manager(Cluster cluster, List<Host> hosts) {
            this.cluster = cluster;

            // TODO: consider the use of NonBlockingHashMap
            this.pools = new ConcurrentHashMap<Host, HostConnectionPool>(hosts.size());
            this.loadBalancer = cluster.manager.loadBalancingFactory.create(hosts);
            this.poolsConfiguration = new HostConnectionPool.Configuration();

            for (Host host : hosts) {
                logger.debug("Adding new host " + host);
                host.monitor().register(this);

                addHost(host);
                // If we fail to connect, the pool will be shutdown right away
                if (pools.get(host).isShutdown()) {
                    logger.debug("Cannot connect to " + host);
                    pools.remove(host);
                }
            }
        }

        private HostConnectionPool addHost(Host host) {
            return pools.put(host, new HostConnectionPool(host, host.monitor().signaler, cluster.manager.connectionFactory, poolsConfiguration));
        }

        public void onUp(Host host) {
            HostConnectionPool previous = addHost(host);;
            loadBalancer.onUp(host);

            // This should not be necessary but it's harmless
            if (previous != null)
                previous.shutdown();
        }

        public void onDown(Host host) {
            loadBalancer.onDown(host);
            HostConnectionPool pool = pools.remove(host);

            // This should not be necessary but it's harmless
            if (pool != null)
                pool.shutdown();
        }

        public void onAdd(Host host) {
            // TODO
        }

        public void onRemove(Host host) {
            // TODO
        }

        public void setKeyspace(String keyspace) {
            poolsConfiguration.setKeyspace(keyspace);
        }

        /**
         * Execute the provided request.
         *
         * This method will find a suitable node to connect to using the {@link LoadBalancingPolicy}
         * and handle host failover.
         *
         * @return a future on the response to the request.
         */
        public Connection.Future execute(Message.Request msg) {

            Iterator<Host> plan = loadBalancer.newQueryPlan();
            while (plan.hasNext()) {
                Host host = plan.next();
                HostConnectionPool pool = pools.get(host);
                if (pool == null || pool.isShutdown())
                    continue;

                try {
                    Connection connection = pool.borrowConnection(DEFAULT_CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS);
                    try {
                        return connection.write(msg);
                    } finally {
                        pool.returnConnection(connection);
                    }
                } catch (ConnectionException e) {
                    logger.trace("Error: " + e.getMessage());
                    // If we have any problem with the connection, just move to the next node.
                    // If that happens during the write of the request, the pool act on the error during returnConnection.
                }
            }
            // TODO: Change that to a "NoAvailableHostException"
            throw new RuntimeException();
        }
    }
}
