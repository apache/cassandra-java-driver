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
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.exceptions.InvalidQueryException;

abstract class AbstractGraphStatement extends RegularStatement {

    final Map<String, ByteBuffer> payload;

    // Add static DefaultPayload for Graph
    final static Map<String, ByteBuffer> DEFAULT_GRAPH_PAYLOAD;
    final static String DEFAULT_GRAPH_LANGUAGE;

    static {
        DEFAULT_GRAPH_LANGUAGE = "gremlin-groovy";
        DEFAULT_GRAPH_PAYLOAD = ImmutableMap.of(
            "graph-language", ByteBuffer.wrap(DEFAULT_GRAPH_LANGUAGE.getBytes()),
//            Cannot set a default on that
            "graph-keyspace", ByteBuffer.wrap("modern".getBytes()),
//            If not present, the default configured for the Keyspace
            "graph-source", ByteBuffer.wrap("default".getBytes())
        );
    }

    AbstractGraphStatement() {
        this.payload = Maps.newHashMap(DEFAULT_GRAPH_PAYLOAD);
        configure();
    }

    void configure() {
        this.setOutgoingPayload(this.payload);
    }

    static void configureFromStatement(AbstractGraphStatement from, AbstractGraphStatement to) {
        to.setOutgoingPayload(from == null ? AbstractGraphStatement.DEFAULT_GRAPH_PAYLOAD : from.getOutgoingPayload());
        // Maybe later additional stuff will need to be done.
    }

    public Map<String, ByteBuffer> getOutgoingPayload() {
        return this.payload;
    }

    public AbstractGraphStatement setGraphLanguage(String language) {
        this.payload.put("graph-language", ByteBuffer.wrap(language.getBytes()));
        return this;
    }

    public AbstractGraphStatement setGraphKeyspace(String graphKeyspace) {
        if (graphKeyspace == null || graphKeyspace.isEmpty()) {
            // TODO: check to return another type of exception since IQE is not really appropriate.
            throw new InvalidQueryException("You cannot set null value or empty string to the keyspace for the Graph, this field is mandatory");
        }
        this.payload.put("graph-keyspace", ByteBuffer.wrap(graphKeyspace.getBytes()));
        return this;
    }

    public AbstractGraphStatement setGraphTraversalSource(String graphTraversalSource) {
        this.payload.put("graph-source", ByteBuffer.wrap(graphTraversalSource.getBytes()));
        return this;
    }

    public AbstractGraphStatement setGraphRebinding(String graphRebinding) {
        this.payload.put("graph-rebinding", ByteBuffer.wrap(graphRebinding.getBytes()));
        return this;
    }
}

