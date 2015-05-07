package com.datastax.driver.jdbc;

import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.cache.LoadingCache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

import static com.datastax.driver.jdbc.Utils.*;

/**
 * Holds a {@link Session} shared among multiple {@link CassandraConnection} objects.
 *
 * This class uses reference counting to track if active CassandraConnections still use
 * the Session. When the last CassandraConnection has closed, the Session gets closed.
 */
class SessionHolder {
    private static final Logger logger = LoggerFactory.getLogger(SessionHolder.class);
    static final String URL_KEY = "url";

    private final LoadingCache<Map<String, String>, SessionHolder> parentCache;
    private final Map<String, String> cacheKey;

    private final AtomicInteger references = new AtomicInteger();
    final Session session;
    final Properties properties;

    SessionHolder(Map<String, String> params, LoadingCache<Map<String, String>, SessionHolder> parentCache) throws SQLException {
        this.cacheKey = params;
        this.parentCache = parentCache;

        String url = params.get(URL_KEY);

        // parse the URL into a set of Properties
        // replace " by ' to handle the fact that " is not a valid character in URIs
        properties = Utils.parseURL(url.replace("\"", "'"));

        // other properties in params come from the initial call to connect(), they take priority
        for (String key : params.keySet())
            if (!URL_KEY.equals(key))
                properties.put(key, params.get(key));

        if (logger.isDebugEnabled())
            logger.debug("Final Properties to Connection: {}", properties);

        session = createSession(properties);
    }

    /** Indicates that a CassandraConnection has closed and stopped using this object. */
    void release() {
        int newRef;
        while (true) {
            int ref = references.get();
            // We set to -1 after the last release, to distinguish it from the initial state
            newRef = (ref == 1) ? -1 : ref - 1;
            if (references.compareAndSet(ref, newRef))
                break;
        }
        if (newRef == -1) {
            logger.debug("Released last reference to {}, closing Session", cacheKey.get(URL_KEY));
            dispose();
        } else {
            logger.debug("Released reference to {}, new count = {}", cacheKey.get(URL_KEY), newRef);
        }
    }

    /**
     * Called when a CassandraConnection tries to acquire a reference to this object.
     *
     * @return whether the reference was acquired successfully
     */
    boolean acquire() {
        while (true) {
            int ref = references.get();
            if (ref < 0) {
                // We raced with the release of the last reference, the caller will need to create a new session
                logger.debug("Failed to acquire reference to {}", cacheKey.get(URL_KEY));
                return false;
            }
            if (references.compareAndSet(ref, ref + 1)) {
                logger.debug("Acquired reference to {}, new count = {}", cacheKey.get(URL_KEY), ref + 1);
                return true;
            }
        }
    }

    @SuppressWarnings("resource")
	private Session createSession(Properties properties) throws SQLException {
        String hosts = properties.getProperty(TAG_SERVER_NAME);
        int port = Integer.parseInt(properties.getProperty(TAG_PORT_NUMBER));
        String keyspace = properties.getProperty(TAG_DATABASE_NAME);
        String username = properties.getProperty(TAG_USER, "");
        String password = properties.getProperty(TAG_PASSWORD, "");
        String loadBalancingPolicy = properties.getProperty(TAG_LOADBALANCING_POLICY, "");
        String retryPolicy = properties.getProperty(TAG_RETRY_POLICY, "");
        String reconnectPolicy = properties.getProperty(TAG_RECONNECT_POLICY, "");
        boolean debugMode = properties.getProperty(TAG_DEBUG, "").equals("true");


        Cluster.Builder builder = Cluster.builder();
        builder.addContactPoints(hosts.split("--")).withPort(port);
        builder.withSocketOptions(new SocketOptions().setKeepAlive(true));
        // Set credentials when applicable
        if (username.length() > 0) {
            builder.withCredentials(username, password);
        }

        if (loadBalancingPolicy.length() > 0) {
            // if load balancing policy has been given in the JDBC URL, parse it and add it to the cluster builder
            try {
                builder.withLoadBalancingPolicy(Utils.parseLbPolicy(loadBalancingPolicy));
            } catch (Exception e) {
                if (debugMode) {
                    throw new SQLNonTransientConnectionException(e);
                }
                logger.warn("Error occured while parsing load balancing policy :" + e.getMessage() + " / Forcing to TokenAwarePolicy...");
                builder.withLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
            }
        }

        if (retryPolicy.length() > 0) {
            // if retry policy has been given in the JDBC URL, parse it and add it to the cluster builder
            try {
                builder.withRetryPolicy(Utils.parseRetryPolicy(retryPolicy));
            } catch (Exception e) {
                if (debugMode) {
                    throw new SQLNonTransientConnectionException(e);
                }
                logger.warn("Error occured while parsing retry policy :" + e.getMessage() + " / skipping...");
            }
        }

        if (reconnectPolicy.length() > 0) {
            // if reconnection policy has been given in the JDBC URL, parse it and add it to the cluster builder
            try {
                builder.withReconnectionPolicy(Utils.parseReconnectionPolicy(reconnectPolicy));
            } catch (Exception e) {
                if (debugMode) {
                    throw new SQLNonTransientConnectionException(e);
                }
                logger.warn("Error occured while parsing reconnection policy :" + e.getMessage() + " / skipping...");
            }
        }

        Cluster cluster = null;
        try {
            cluster = builder.build();
            return cluster.connect(keyspace);
        } catch (DriverException e) {
            if (cluster != null)
                cluster.close();
            throw new SQLNonTransientConnectionException(e);
        }
    }

    private void dispose() {
        // No one else has a reference to the parent Cluster, and only one Session was created from it:
        session.getCluster().close();
        parentCache.invalidate(cacheKey);
    }
}
