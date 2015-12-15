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

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

/**
 * A default representation of a Vertex in DSE Graph.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonDeserialize(using = DefaultVertexDeserializer.class)
public class Vertex {
    private GraphData id;
    private String label;
    private String type;

    private Map<String, GraphData> properties;

    public Vertex() {

    }

    public Vertex(GraphData id, String label, String type, Map<String, GraphData> properties) {
        this.id = id;
        this.label = label;
        this.type = type;
        this.properties = properties;
    }

    public void setId(GraphData id) {
        this.id = id;
    }

    public GraphData getId() {
        return this.id;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public String getLabel() {
        return this.label;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getType() {
        return this.type;
    }

    public void setProperties(Map<String, GraphData> properties) {
        this.properties = properties;
    }

    public Map<String, GraphData> getProperties() {
        return this.properties;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Graph Vertex [");
        sb.append(String.format("id = %s, label = %s", this.id.toString(), this.label));
        sb.append(", properties = {");
        int i = 0;
        for (Map.Entry<String, GraphData> entry : this.properties.entrySet()) {
            if (i > 0)
                sb.append(", ");
            sb.append(String.format("%s : %s", entry.getKey(), entry.getValue().toString()));
            i++;
        }
        sb.append("}");
        sb.append("]");
        return sb.toString();
    }
}
