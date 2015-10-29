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

import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.InvalidQueryException;

public class GraphSession {
    private final Session session;
    private final Map<String, ByteBuffer> defaultGraphPayload;

    // Static keys for the Custom Payload map
    final static String GRAPH_SOURCE_KEY;
    final static String GRAPH_LANGUAGE_KEY;
    final static String GRAPH_KEYPSACE_KEY;
    final static String GRAPH_REBINDING_KEY;

    // Add static DefaultPayload values for Graph
    final static String DEFAULT_GRAPH_LANGUAGE;
    final static String DEFAULT_GRAPH_SOURCE;
    final static Map<String, ByteBuffer> DEFAULT_GRAPH_PAYLOAD;

    static {
        GRAPH_SOURCE_KEY        = "graph-source";
        GRAPH_LANGUAGE_KEY      = "graph-language";
        GRAPH_KEYPSACE_KEY      = "graph-keyspace";
        GRAPH_REBINDING_KEY     = "graph-rebinding";


        DEFAULT_GRAPH_LANGUAGE  = "gremlin-groovy";
        DEFAULT_GRAPH_SOURCE    = "default";
        DEFAULT_GRAPH_PAYLOAD   = ImmutableMap.of(
            // For the first versions of the driver Gremlin-Groovy is the default language
            GRAPH_LANGUAGE_KEY, ByteBuffer.wrap(DEFAULT_GRAPH_LANGUAGE.getBytes()),

            //If not present, the default source configured for the Keyspace
            GRAPH_SOURCE_KEY, ByteBuffer.wrap(DEFAULT_GRAPH_SOURCE.getBytes())
        );
    }

    public GraphSession(Session session) {
        this.session = session;
        this.defaultGraphPayload = Maps.newHashMap(DEFAULT_GRAPH_PAYLOAD);
    }

    public GraphResultSet execute(AbstractGraphStatement statement) {
        if (!AbstractGraphStatement.checkStatement(statement)) {
            throw new InvalidQueryException("Invalid Graph Statement, you need to specify at least the keyspace containing the Graph data.");
        }
        return new GraphResultSet(session.execute(statement.configureAndGetWrappedStatement()));
    }

    public GraphResultSet execute(String query) {
        GraphStatement statement = newGraphStatement(query);
        return execute(statement);
    }

    public PreparedGraphStatement prepare(GraphStatement gst) {
        return new PreparedGraphStatement(session.prepare(gst.configureAndGetWrappedStatement()), gst);
    }

    public PreparedGraphStatement prepare(String query) {
        GraphStatement statement = newGraphStatement(query);
        return prepare(statement);
    }

    public Session getSession() {
        return this.session;
    }

    public GraphStatement newGraphStatement(String query, Object... objects) {
        return new GraphStatement(query, this, objects);
    }

    public Map<String, ByteBuffer> getCustomDefaultPayload() {
        return this.defaultGraphPayload;
    }

    public boolean resetDefaultPayload() {
        this.defaultGraphPayload.clear();
        this.defaultGraphPayload.putAll(DEFAULT_GRAPH_PAYLOAD);
        // Just a quick verification
        return this.defaultGraphPayload.hashCode() == DEFAULT_GRAPH_PAYLOAD.hashCode();
    }

    public GraphSession setDefaultGraphSource(String input) {
        this.defaultGraphPayload.put(GRAPH_SOURCE_KEY, ByteBuffer.wrap(input.getBytes()));
        return this;
    }

    public GraphSession setDefaultGraphLanguage(String input) {
        this.defaultGraphPayload.put(GRAPH_LANGUAGE_KEY, ByteBuffer.wrap(input.getBytes()));
        return this;
    }

    public GraphSession setDefaultGraphKeyspace(String input) {
        this.defaultGraphPayload.put(GRAPH_KEYPSACE_KEY, ByteBuffer.wrap(input.getBytes()));
        return this;
    }

    public GraphSession setDefaultGraphRebinding(String input) {
        this.defaultGraphPayload.put(GRAPH_REBINDING_KEY, ByteBuffer.wrap(input.getBytes()));
        return this;
    }
}