package com.datastax.driver.core;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;

import com.datastax.driver.core.exceptions.*;
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

    final Manager manager;

    // Package protected, only Cluster should construct that.
    Session(Cluster cluster, Collection<Host> hosts) {
        this.manager = new Manager(cluster, hosts);
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
     *
     * @throws NoHostAvailableException if no host in the cluster can be
     * contacted successfully to execute this query.
     * @throws QueryExecutionException if the query triggered an execution
     * exception, i.e. an exception thrown by Cassandra when it cannot execute
     * the query with the requested consistency level successfully.
     */
    public ResultSet execute(String query) throws NoHostAvailableException, QueryExecutionException {
        return executeAsync(query).getUninterruptibly();
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
     * @throws NoHostAvailableException if no host in the cluster can be
     * contacted successfully to execute this query.
     * @throws QueryExecutionException if the query triggered an execution
     * exception, i.e. an exception thrown by Cassandra when it cannot execute
     * the query with the requested consistency level successfully.
     *
     * @see #execute(String)
     */
    public ResultSet execute(CQLQuery query) throws NoHostAvailableException, QueryExecutionException {
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
        return manager.executeQuery(new QueryMessage(query));
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
        return executeAsync(query.toString());
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
            Connection.Future future = new Connection.Future(new PrepareMessage(query));
            manager.execute(future);
            return toPreparedStatement(future);
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
     *
     * @throws IllegalStateException if {@code !stmt.ready()}.
     */
    public ResultSet executePrepared(BoundStatement stmt) throws NoHostAvailableException, QueryExecutionException {
        return executePreparedAsync(stmt).getUninterruptibly();
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
     *
     * @throws IllegalStateException if {@code !stmt.ready()}.
     */
    public ResultSet.Future executePreparedAsync(BoundStatement stmt) {
        if (!stmt.ready())
            throw new IllegalStateException("Some bind variables haven't been bound in the provided statement");

        return manager.executeQuery(new ExecuteMessage(stmt.statement.id, Arrays.asList(stmt.values)));
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

    static class Manager implements Host.StateListener {

        final Cluster cluster;

        final ConcurrentMap<Host, HostConnectionPool> pools;
        final LoadBalancingPolicy loadBalancer;

        // TODO: make that configurable
        final RetryPolicy retryPolicy = RetryPolicy.DefaultPolicy.INSTANCE;

        final HostConnectionPool.Configuration poolsConfiguration;

        // TODO: Make that configurable
        final long DEFAULT_CONNECTION_TIMEOUT = 3000;

        public Manager(Cluster cluster, Collection<Host> hosts) {
            this.cluster = cluster;

            // TODO: consider the use of NonBlockingHashMap
            this.pools = new ConcurrentHashMap<Host, HostConnectionPool>(hosts.size());
            this.loadBalancer = cluster.manager.loadBalancingFactory.create(hosts);
            this.poolsConfiguration = new HostConnectionPool.Configuration();

            for (Host host : hosts)
                addHost(host);
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
            HostConnectionPool previous = addHost(host);;
            loadBalancer.onAdd(host);

            // This should not be necessary, especially since the host is
            // supposed to be new, but it's safer to make that work correctly
            // if the even is triggered multiple times.
            if (previous != null)
                previous.shutdown();
        }

        public void onRemove(Host host) {
            loadBalancer.onRemove(host);
            HostConnectionPool pool = pools.remove(host);
            if (pool != null)
                pool.shutdown();
        }

        public void setKeyspace(String keyspace) throws NoHostAvailableException {
            try {
                executeQuery(new QueryMessage("use " + keyspace)).get();
            } catch (InterruptedException e) {
                // TODO: do we want to handle interrupted exception in a better way?
                throw new DriverInternalError("Hey! I was waiting!", e);
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                // A USE query should never fail unless we cannot contact a node
                if (cause instanceof NoHostAvailableException)
                    throw (NoHostAvailableException)cause;
                else if (cause instanceof DriverUncheckedException)
                    throw (DriverUncheckedException)cause;
                else
                    throw new DriverInternalError("Unexpected exception thrown", cause);
            }
        }

        /**
         * Execute the provided request.
         *
         * This method will find a suitable node to connect to using the
         * {@link LoadBalancingPolicy} and handle host failover.
         */
        public void execute(Connection.ResponseCallback callback) {
            new RetryingCallback(this, callback).sendRequest();
        }

        public ResultSet.Future executeQuery(Message.Request msg) {
            ResultSet.Future future = new ResultSet.Future(this, msg);
            execute(future);
            return future;
        }
    }
}
