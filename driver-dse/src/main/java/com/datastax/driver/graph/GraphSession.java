/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.graph;

import com.datastax.driver.core.Session;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * The GraphSession object allows to execute and prepare graph specific statements.
 * <p/>
 * You should use this object whenever the statement to execute is a graph query.
 * The object will make verifications before executing queries to make sure the
 * statement sent is valid.
 * This object also generates graph result sets.
 */
public class GraphSession {
    private final Session session;
    private final ConcurrentMap<String, String> defaultGraphPayload;

    // Static keys for the Custom Payload map
    final static String GRAPH_SOURCE_KEY = "graph-source";
    final static String GRAPH_LANGUAGE_KEY = "graph-language";
    final static String GRAPH_KEYSPACE_KEY = "graph-keyspace";
    final static String GRAPH_REBINDING_KEY = "graph-rebinding";

    // Add static DefaultPayload values for Graph
    final static String DEFAULT_GRAPH_LANGUAGE = "gremlin-groovy";
    final static String DEFAULT_GRAPH_SOURCE = "default";
    final static Map<String, String> DEFAULT_GRAPH_PAYLOAD = ImmutableMap.of(
            // For the first versions of the driver Gremlin-Groovy is the default language
            GRAPH_LANGUAGE_KEY, DEFAULT_GRAPH_LANGUAGE,

            //If not present, the default source configured for the Keyspace
            GRAPH_SOURCE_KEY, DEFAULT_GRAPH_SOURCE
    );

    final static ObjectMapper objectMapper = new ObjectMapper();

    /**
     * API
     */

    /**
     * Create a GraphSession object that wraps an underlying Java Driver Session.
     * The Java Driver session have to be initialised.
     *
     * @param session the session to wrap, basically the return of a cluster.connect() call.
     */
    public GraphSession(Session session) {
        this.session = session;
        this.defaultGraphPayload = new ConcurrentHashMap<String, String>(DEFAULT_GRAPH_PAYLOAD);
    }

    /**
     * Execute a graph statement.
     *
     * @param statement Any inherited class of {@link com.datastax.driver.graph.AbstractGraphStatement}. For now,
     *                  this covers execution of simple and bound graph statements. It is extensible in case
     *                  users would need to implement their own way {@link com.datastax.driver.graph.AbstractGraphStatement}.
     * @return A {@link GraphResultSet} object if the execution of the query was successful. This method can throw an exception
     * in case the execution did non complete successfully.
     */
    public GraphResultSet execute(AbstractGraphStatement statement) {
        return new GraphResultSet(session.execute(statement.configureAndGetWrappedStatement(getDefaultGraphOptions())));
    }

    /**
     * Execute a graph query.
     *
     * @param query This method will create a {@link GraphStatement} object internally, to be executed.
     *              The graph options included in this generated {@link GraphStatement} will be the default options set in the {@link GraphStatement}.
     * @return A {@link GraphResultSet} object if the execution of the query was successful. This method can throw an exception
     * in case the execution did non complete successfully.
     */
    public GraphResultSet execute(String query) {
        GraphStatement statement = new GraphStatement(query);
        return execute(statement);
    }

    /**
     * Prepare a graph statement.
     *
     * @param gst A simple {@link GraphStatement} object to be prepared.
     *
     * @return A {@link PreparedGraphStatement} object to be bound with values, and executed.
     */
//    public PreparedGraphStatement prepare(GraphStatement gst) {
//        return new PreparedGraphStatement(session.prepare(gst.configureAndGetWrappedStatement()), gst);
//    }

    /**
     * Prepare a graph statement.
     *
     * @param query This method will create a {@link GraphStatement} object internally, to be executed.
     *              The graph options included in this generated {@link GraphStatement} will be the default options set in the {@link GraphSession}.
     * @return A {@link PreparedGraphStatement} object to be bound with values, and executed.
     */
//    public PreparedGraphStatement prepare(String query) {
//        GraphStatement statement = newGraphStatement(query);
//        return prepare(statement);
//    }

    /**
     * Get the wrapped {@link com.datastax.driver.core.Session}.
     *
     * @return the wrapped {@link com.datastax.driver.core.Session}.
     */
    public Session getSession() {
        return this.session;
    }

    /**
     * Get the default graph options configured for this session.
     *
     * @return A Map of the options, encoded in bytes in a ByteBuffer.
     */
    public Map<String, String> getDefaultGraphOptions() {
        return this.defaultGraphPayload;
    }

    /**
     * Reset the default graph options for this {@link com.datastax.driver.graph.GraphSession}.
     */
    public boolean resetDefaultGraphOptions() {
        this.defaultGraphPayload.clear();
        this.defaultGraphPayload.putAll(DEFAULT_GRAPH_PAYLOAD);
        // Just a quick verification
        return this.defaultGraphPayload.hashCode() == DEFAULT_GRAPH_PAYLOAD.hashCode();
    }

    /**
     * Set the default Graph traversal source name on the graph side.
     * <p/>
     * The default value for this property is "default".
     *
     * @param input The graph traversal source's name to use.
     * @return This {@link com.datastax.driver.graph.GraphSession} instance to chain call.
     */
    public GraphSession setDefaultGraphSource(String input) {
        this.defaultGraphPayload.put(GRAPH_SOURCE_KEY, input);
        return this;
    }

    /**
     * Set the default Graph language to use in the query.
     * <p/>
     * The default value for this property is "gremlin-groovy".
     *
     * @param input The language used in queries.
     * @return This {@link com.datastax.driver.graph.GraphSession} instance to chain call.
     */
    public GraphSession setDefaultGraphLanguage(String input) {
        this.defaultGraphPayload.put(GRAPH_LANGUAGE_KEY, input);
        return this;
    }

    /**
     * Set the default Cassandra keyspace name storing the graph.
     *
     * @param input The Cassandra keyspace name to use.
     * @return This {@link com.datastax.driver.graph.GraphSession} instance to chain call.
     */
    public GraphSession setDefaultGraphKeyspace(String input) {
        this.defaultGraphPayload.put(GRAPH_KEYSPACE_KEY, input);
        return this;
    }

    /**
     * Set the default Graph rebinding name to use.
     * <p/>
     * The default value for this property is "default".
     *
     * @param input The graph traversal source's name to use.
     * @return This {@link com.datastax.driver.graph.GraphSession} instance to chain call.
     */
    public GraphSession setDefaultGraphRebinding(String input) {
        this.defaultGraphPayload.put(GRAPH_REBINDING_KEY, input);
        return this;
    }
}