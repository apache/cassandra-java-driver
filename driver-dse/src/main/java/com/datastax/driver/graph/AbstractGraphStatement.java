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

import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.google.common.collect.Maps;

import java.nio.ByteBuffer;
import java.util.Map;

abstract class AbstractGraphStatement<T extends Statement> {

    protected final Map<String, String> graphOptions;
    protected T wrappedStatement;

    AbstractGraphStatement() {
        this.graphOptions = Maps.newHashMap();
    }

    /* TODO: eventually make more advanced checks on the statement
     *
     * As of right now, mandatory fields for DSE Graph are :
     * - graph-keyspace : Must be set by users
     * - graph-source : default is "default"
     * - graph-language : default is "gremlin-groovy"
     *
     */
    static boolean checkOptions(Map<String, String> options) {
        return options.containsKey(GraphSession.GRAPH_SOURCE_KEY)
                && options.containsKey(GraphSession.GRAPH_LANGUAGE_KEY)
                && options.containsKey(GraphSession.GRAPH_KEYSPACE_KEY);

    }

    protected abstract T configureAndGetWrappedStatement(Map<String, String> sessionOptions);

    void configure(Map<String, String> sessionOptions) {
        // Apply the graph specific operations here.
        Map<String, String> mergedOptions = Maps.newHashMap(sessionOptions);
        mergedOptions.putAll(this.graphOptions);
        checkOptions(mergedOptions);
        Map<String, ByteBuffer> payload = Maps.newHashMap();
        for (Map.Entry<String, String> entry : mergedOptions.entrySet()) {
            payload.put(entry.getKey(), ByteBuffer.wrap(entry.getValue().getBytes()));
        }
        this.wrappedStatement.setOutgoingPayload(payload);
    }

    /**
     * API
     */

    /**
     * Get the current graph options configured for this statement.
     *
     * @return A payload map containing the configured Graph options.
     */
    public Map<String, String> getGraphOptions() {
        return this.graphOptions;
    }

    /**
     * Set the Graph language to use for this statement.
     *
     * @param language
     * @return This {@link com.datastax.driver.graph.AbstractGraphStatement} instance to allow chaining call.
     */
    public AbstractGraphStatement setGraphLanguage(String language) {
        this.graphOptions.put(GraphSession.GRAPH_LANGUAGE_KEY, language);
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
        this.graphOptions.put(GraphSession.GRAPH_KEYSPACE_KEY, graphKeyspace);
        return this;
    }

    /**
     * Set the Graph traversal source to use for this statement.
     *
     * @param graphTraversalSource
     * @return This {@link com.datastax.driver.graph.AbstractGraphStatement} instance to allow chaining call.
     */
    public AbstractGraphStatement setGraphSource(String graphTraversalSource) {
        this.graphOptions.put(GraphSession.GRAPH_SOURCE_KEY, graphTraversalSource);
        return this;
    }

    /**
     * Set the Graph rebinding name to use for this statement.
     *
     * @param graphRebinding
     * @return This {@link com.datastax.driver.graph.AbstractGraphStatement} instance to allow chaining call.
     */
    public AbstractGraphStatement setGraphRebinding(String graphRebinding) {
        this.graphOptions.put(GraphSession.GRAPH_REBINDING_KEY, graphRebinding);
        return this;
    }

    /**
     * Set manually the custom payload to use for this statement.
     * Please see {@link com.datastax.driver.core.Statement#setOutgoingPayload(java.util.Map)} for more
     * information.
     *
     * @param options The options to set. Note that this will override the default values set in the {@link com.datastax.driver.graph.GraphSession}.
     * @return This {@link com.datastax.driver.graph.AbstractGraphStatement} instance to allow chaining call.
     */
    public AbstractGraphStatement setGraphOptions(Map<String, String> options) {
        this.graphOptions.putAll(options);
        return this;
    }
}

