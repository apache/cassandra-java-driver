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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.InvalidQueryException;

abstract class AbstractGraphStatement<T extends Statement> {

    final Map<String, ByteBuffer> payload;
    T wrappedStatement;
    final GraphSession session;

    AbstractGraphStatement(GraphSession session) {
        this.session = session;
        this.payload = Maps.newHashMap(session.getCustomDefaultPayload());
    }

    /* TODO: eventually make more advanced checks on the statement
     *
     * As of right now, mandatory fields for DSE Graph are :
     * - graph-keyspace
     *
     */
    static boolean checkStatement(AbstractGraphStatement graphStatement) {
        return graphStatement.getOutgoingPayload().containsKey(GraphSession.GRAPH_LANGUAGE_KEY)
            && graphStatement.getOutgoingPayload().containsKey(GraphSession.GRAPH_LANGUAGE_KEY)
            && graphStatement.getOutgoingPayload().containsKey(GraphSession.GRAPH_SOURCE_KEY);

    }

    static void copyConfiguration(AbstractGraphStatement from, AbstractGraphStatement to) {
        to.wrappedStatement.setOutgoingPayload(from == null ? GraphSession.DEFAULT_GRAPH_PAYLOAD : from.getOutgoingPayload());
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

    public Map<String, ByteBuffer> getOutgoingPayload() {
        return this.payload;
    }

    public AbstractGraphStatement setGraphLanguage(String language) {
        this.payload.put(GraphSession.GRAPH_LANGUAGE_KEY, ByteBuffer.wrap(language.getBytes()));
        return this;
    }

    public AbstractGraphStatement setGraphKeyspace(String graphKeyspace) {
        if (graphKeyspace == null || graphKeyspace.isEmpty()) {
            // TODO: check to return another type of exception since IQE is not really appropriate.
            throw new InvalidQueryException("You cannot set null value or empty string to the keyspace for the Graph, this field is mandatory.");
        }
        this.payload.put(GraphSession.GRAPH_LANGUAGE_KEY, ByteBuffer.wrap(graphKeyspace.getBytes()));
        return this;
    }

    public AbstractGraphStatement setGraphSource(String graphTraversalSource) {
        this.payload.put(GraphSession.GRAPH_SOURCE_KEY, ByteBuffer.wrap(graphTraversalSource.getBytes()));
        return this;
    }

    public AbstractGraphStatement setGraphRebinding(String graphRebinding) {
        this.payload.put(GraphSession.GRAPH_REBINDING_KEY, ByteBuffer.wrap(graphRebinding.getBytes()));
        return this;
    }
}

