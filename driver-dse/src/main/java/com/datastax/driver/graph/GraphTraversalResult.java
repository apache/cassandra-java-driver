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

    GraphTraversalResult(String result) {
        this.jsonString = result;

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            this.rootNode = objectMapper.readTree(this.jsonString);
        } catch (IOException e) {
            throw new DriverException("Could not parse the result returned by the Graph server as a JSON string : " + this.jsonString);
        }
    }

    public String getResultString() {
        return this.jsonString;
    }

    public GraphData get(String name) {
        return new GraphData(this.rootNode.get(name));
    }

    public GraphData get() {
        return new GraphData(this.rootNode);
    }

    static public GraphTraversalResult fromRow(Row row) {
        return row == null ? null : new GraphTraversalResult(row.getString("gremlin"));
    }
}
