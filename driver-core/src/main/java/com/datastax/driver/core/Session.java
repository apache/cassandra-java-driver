package com.datastax.driver.core;

import java.util.List;
import java.net.InetSocketAddress;

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

    // TODO: we can do better :)
    private final Connection connection;

    // Package protected, only Cluster should construct that.
    Session(List<InetSocketAddress> addresses) throws ConnectionException {
        Connection.Factory factory = new Connection.Factory(addresses.get(0));
        this.connection = factory.open();
    }

    /**
     * Sets the current keyspace to use for this session.
     *
     * Note that it is up to the application to synchronize calls to this
     * method with queries executed against this session.
     *
     * @param keyspace the name of the keyspace to set
     * @return this session.
     * 
     */
    public Session use(String keyspace) {
        return null;
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

        // TODO: this is not the real deal, just for tests
        try {
            QueryMessage msg = new QueryMessage(query);
            Connection.Future future = connection.write(msg);
            Message.Response response = future.get();

            if (response.type == Message.Type.RESULT) {
                ResultMessage rmsg = (ResultMessage)response;
                switch (rmsg.kind) {
                    case VOID:
                    case ROWS:
                        return ResultSet.fromMessage(rmsg);
                }
                logger.info("Got " + response);
                return null;
            }
            else {
                logger.info("Got " + response);
                return null;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
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
        return null;
    }

    /**
     * @see #prepare(String)
     */
    public PreparedStatement prepare(CQLQuery query) {
        return prepare(query.toString());
    }

    /**
     * Execute a prepared statement that had values provided for its bound
     * variables.
     *
     * This method performs like {@link execute} but for prepared statements.
     * It blocks until at least some result has been received from the
     * database.
     *
     * @param stmt the prepared statement with values for its bound variables.
     * @return the result of the query. That result will never be null be can
     * be empty and will be for any non SELECT query.
     */
    public ResultSet executePrepared(BoundStatement stmt) {
        return null;
    }

    /**
     * Execute a prepared statement that had values provided for its bound
     * variables asynchronously.
     *
     * This method performs like {@link executeAsync} but for prepared
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
}
