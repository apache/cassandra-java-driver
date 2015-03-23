/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */
package com.datastax.driver.jdbc;

import static com.datastax.driver.jdbc.Utils.NOT_SUPPORTED;
import static com.datastax.driver.jdbc.Utils.PROTOCOL;
import static com.datastax.driver.jdbc.Utils.TAG_PASSWORD;
import static com.datastax.driver.jdbc.Utils.TAG_USER;

import java.sql.*;
import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import com.google.common.cache.*;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Class CassandraDriver.
 */
public class CassandraDriver implements Driver
{
    public static final int DVR_MAJOR_VERSION = 2;

    public static final int DVR_MINOR_VERSION = 1;

    public static final int DVR_PATCH_VERSION = 6;

    public static final String DVR_NAME = "Datastax JDBC Driver";

    private static final Logger logger = LoggerFactory.getLogger(CassandraDriver.class);

    static
    {
        // Register the CassandraDriver with DriverManager
        try
        {
            CassandraDriver driverInst = new CassandraDriver();
            DriverManager.registerDriver(driverInst);
        }
        catch (SQLException e)
        {
            throw new RuntimeException(e.getMessage());
        }
    }

    // Caches Sessions so that multiple CassandraConnections created with the same parameters use the same Session.
    private final LoadingCache<Map<String, String>, SessionHolder> sessionsCache = CacheBuilder.newBuilder()
        .build(new CacheLoader<Map<String, String>, SessionHolder>() {
                   @Override
                   public SessionHolder load(final Map<String, String> params) throws Exception {
                       return new SessionHolder(params, sessionsCache);
                   }
               }
        );

    /**
     * Method to validate whether provided connection url matches with pattern or not.
     */
    public boolean acceptsURL(String url) throws SQLException
    {
        return url.startsWith(PROTOCOL);
    }

    /**
     * Method to return connection instance for given connection url and connection props.
     */
    public Connection connect(String url, Properties props) throws SQLException
    {
        if (acceptsURL(url))
        {
            ImmutableMap.Builder<String, String> params = ImmutableMap.builder();

            Enumeration<Object> keys = props.keys();
            while (keys.hasMoreElements()) {
                String key = (String)keys.nextElement();
                params.put(key, props.getProperty(key));
            }
            params.put(SessionHolder.URL_KEY, url);

            Map<String, String> cacheKey = params.build();

            try {
                while (true) {
                    // Get (or create) the corresponding Session from the cache
                    SessionHolder sessionHolder = sessionsCache.get(cacheKey);

                    if (sessionHolder.acquire())
                        return new CassandraConnection(sessionHolder);
                    // If we failed to acquire, it means we raced with the release of the last reference to the session
                    // (which also removes it from the cache).
                    // Loop to try again, that will cause the cache to create a new instance.
                }
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof SQLException)
                    throw (SQLException)cause;
				throw new SQLNonTransientConnectionException("Unexpected error while creating connection", e);
            }
        }
		return null; // signal it is the wrong driver for this protocol:subprotocol
    }

    /**
     * Returns default major version.
     */
    public int getMajorVersion()
    {
        return DVR_MAJOR_VERSION;
    }

    /**
     * Returns default minor version.
     */
    public int getMinorVersion()
    {
        return DVR_MINOR_VERSION;
    }

    /**
     * Returns default driver property info object.
     */
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties props) throws SQLException
    {
        if (props == null) props = new Properties();

        DriverPropertyInfo[] info = new DriverPropertyInfo[2];

        info[0] = new DriverPropertyInfo(TAG_USER, props.getProperty(TAG_USER));
        info[0].description = "The 'user' property";

        info[1] = new DriverPropertyInfo(TAG_PASSWORD, props.getProperty(TAG_PASSWORD));
        info[1].description = "The 'password' property";

        return info;
    }

    /**
     * Returns true, if it is jdbc compliant.
     */
    public boolean jdbcCompliant()
    {
        return false;
    }
    
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException
    {
    	throw new SQLFeatureNotSupportedException(String.format(NOT_SUPPORTED));
    }
}
