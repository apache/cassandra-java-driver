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

import java.nio.ByteBuffer;
import java.util.Map;

import com.google.common.collect.Maps;

import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.InvalidQueryException;

abstract class AbstractGraphStatement<T extends Statement> {

    final Map<String, ByteBuffer> payload;
    T wrappedStatement;
    final GraphSession session;

    AbstractGraphStatement(GraphSession session) {
        this.session = session;
        this.payload = Maps.newHashMap(session.getDefaultGraphOptions());
    }

    /* TODO: eventually make more advanced checks on the statement
     *
     * As of right now, mandatory fields for DSE Graph are :
     * - graph-keyspace : Must be set by users
     * - graph-source : default is "default"
     * - graph-language : default is "gremlin-groovy"
     *
     */
    static boolean checkStatement(AbstractGraphStatement graphStatement) {
        return graphStatement.getGraphOptions().containsKey(GraphSession.GRAPH_SOURCE_KEY)
            && graphStatement.getGraphOptions().containsKey(GraphSession.GRAPH_LANGUAGE_KEY)
            && graphStatement.getGraphOptions().containsKey(GraphSession.GRAPH_SOURCE_KEY);

    }

    static void copyConfiguration(AbstractGraphStatement from, AbstractGraphStatement to) {
        to.wrappedStatement.setOutgoingPayload(from == null ? GraphSession.DEFAULT_GRAPH_PAYLOAD : from.getGraphOptions());
        // Maybe later additional stuff will need to be done.
    }

    abstract T configureAndGetWrappedStatement();

    void configure() {
        // Apply the graph specific operations here.
        this.wrappedStatement.setOutgoingPayload(this.payload);
    }

    GraphSession getSession() {
        return this.session;
    }

    /**
     * API
     */

    /**
     * Get the current graph options configured for this statement.
     *
     * @return A payload map containing the configured Graph options.
     */
    public Map<String, ByteBuffer> getGraphOptions() {
        return this.payload;
    }

    /**
     * Set the Graph language to use for this statement.
     *
     * @param language
     * @return This {@link com.datastax.driver.graph.AbstractGraphStatement} instance to allow chaining call.
     */
    public AbstractGraphStatement setGraphLanguage(String language) {
        this.payload.put(GraphSession.GRAPH_LANGUAGE_KEY, ByteBuffer.wrap(language.getBytes()));
        return this;
    }

    /**
     * Set the Graph keyspace to use for this statement.
     *
     * @param graphKeyspace The keyspace to set. Throws an exception if the parameter is null or empty since this parameter is mandatory.
     * @return This {@link com.datastax.driver.graph.AbstractGraphStatement} instance to allow chaining call.
     */
    public AbstractGraphStatement setGraphKeyspace(String graphKeyspace) {
        if (graphKeyspace == null || graphKeyspace.isEmpty()) {
            throw new InvalidQueryException("You cannot set null value or empty string to the keyspace for the Graph, this field is mandatory.");
        }
        this.payload.put(GraphSession.GRAPH_LANGUAGE_KEY, ByteBuffer.wrap(graphKeyspace.getBytes()));
        return this;
    }

    /**
     * Set the Graph traversal source to use for this statement.
     *
     * @param graphTraversalSource
     * @return This {@link com.datastax.driver.graph.AbstractGraphStatement} instance to allow chaining call.
     */
    public AbstractGraphStatement setGraphSource(String graphTraversalSource) {
        this.payload.put(GraphSession.GRAPH_SOURCE_KEY, ByteBuffer.wrap(graphTraversalSource.getBytes()));
        return this;
    }

    /**
     * Set the Graph rebinding name to use for this statement.
     *
     * @param graphRebinding
     * @return This {@link com.datastax.driver.graph.AbstractGraphStatement} instance to allow chaining call.
     */
    public AbstractGraphStatement setGraphRebinding(String graphRebinding) {
        this.payload.put(GraphSession.GRAPH_REBINDING_KEY, ByteBuffer.wrap(graphRebinding.getBytes()));
        return this;
    }

    /**
     * Set manually the custom payload to use for this statement.
     * Please see {@link com.datastax.driver.core.Statement#setOutgoingPayload(java.util.Map)} for more
     * information.
     *
     * @param payload The payload to set. Note that this will override the default values set in the {@link com.datastax.driver.graph.GraphSession}.
     * @return This {@link com.datastax.driver.graph.AbstractGraphStatement} instance to allow chaining call.
     */
    public AbstractGraphStatement setOutgoingPayload(Map<String, ByteBuffer> payload) {
        this.wrappedStatement.setOutgoingPayload(payload);
        return this;
    }
}

