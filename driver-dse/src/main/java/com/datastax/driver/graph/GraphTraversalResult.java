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

import java.io.IOException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.DriverException;

public class GraphTraversalResult {
    private final String jsonString;

    private JsonNode rootNode;

    private Row row;

    GraphTraversalResult(Row row) {
        this.jsonString = row.getString("gremlin");
        this.row = row;
        try {
            this.rootNode = GraphSession.objectMapper.readTree(this.jsonString).get("result");
        } catch (IOException e) {
            throw new DriverException("Could not parse the result returned by the Graph server as a JSON string : " + this.jsonString);
        }
    }

    static GraphTraversalResult fromRow(Row row) {
        return row == null ? null : new GraphTraversalResult(row);
    }

    /**
     * API
     */

    /**
     * Get the {@link com.datastax.driver.graph.GraphData} object for the specified key.
     *
     * @param key the key corresponding to the desired object in the result.
     * @return A GraphData instance containing the value for the Json entity required.
     */
    public GraphData get(String key){
        return get().get(key);
    }

    /**
     * Get the {@link com.datastax.driver.graph.GraphData} object for the specified key.
     *
     * @param index the index corresponding to the desired object in the result if it is an array.
     * @return A GraphData instance containing the value for the Json entity required.
     */
    public GraphData get(int index) {
        return get().get(index);
    }

    /**
     * Return directly the root result in a {@link GraphData} object.
     *
     * @return A GraphData instance containing the root result.
     */
    public GraphData get() {
        // The key for the first result is 'result', we put it hardcoded because the GraphData needs it to inform user if an Exception.
        return new GraphData("result", this.rootNode);
    }

    /**
     * Get the raw JSON string returned by the server.
     *
     * @return the raw JSON string received from the server.
     */
    @Override
    public String toString() {
        return this.jsonString;
    }
}

