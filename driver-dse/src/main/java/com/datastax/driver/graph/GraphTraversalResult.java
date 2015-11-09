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
    private ObjectMapper objectMapper;

    private Row row;

    GraphTraversalResult(Row row, ObjectMapper objectMapper) {
        this.jsonString = row.getString("gremlin");
        this.row = row;
        this.objectMapper = objectMapper;
        try {
            this.rootNode = this.objectMapper.readTree(this.jsonString).get("result");
        } catch (IOException e) {
            throw new DriverException("Could not parse the result returned by the Graph server as a JSON string : " + this.jsonString);
        }
    }

    static GraphTraversalResult fromRow(Row row, ObjectMapper objectMapper) {
        return row == null ? null : new GraphTraversalResult(row, objectMapper);
    }

    /**
     * API
     */

    /**
     * Get the raw JSON string returned by the server.
     *
     * @return the raw JSON string received from the server.
     */
    public String getResultString() {
        return this.jsonString;
    }

    /**
     * Get the {@link com.datastax.driver.graph.GraphData} object for the specified key.
     *
     * @param keyOrIndex Can be either a String or a int/Integer. Must not be null, or an exception will be thrown.
     * @return A GraphData instance containing the value for the Json entity required.
     */
    public GraphData get(Object keyOrIndex) {
        if (keyOrIndex == null) {
            throw new DriverException("You must provide a valid key or index identifier in a get(Object) call, or use the GraphTraversalResult#get() method.");
        }
        if (keyOrIndex instanceof String) {
            return new GraphData(keyOrIndex, this.rootNode.get((String) keyOrIndex), this.objectMapper);
        }
        else if (keyOrIndex instanceof Integer) {
            return new GraphData(keyOrIndex, this.rootNode.get((Integer)keyOrIndex), this.objectMapper);
        } else {
            throw new DriverException("You must provide a valid key or index identifier in a get(Object) call.");
        }
    }

    /**
     * Return directly the root result in a {@link GraphData} object.
     *
     * @return A GraphData instance containing the root result.
     */
    public GraphData get() {
        // The key for the first result is 'result', we put it hardcoded because the GraphData needs it to inform user if an Exception.
        return new GraphData("result", this.rootNode, this.objectMapper);
    }

    @Override
    public String toString() {
        if (this.rootNode != null) {
            return this.rootNode.toString();
        }
        return null;
    }
}

