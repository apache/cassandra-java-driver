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
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

/**
 * A default representation of an Edge in DSE Graph.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonDeserialize(using = DefaultEdgeDeserializer.class)
public class Edge {
    private int id;
    private String label;
    private String type;

    private int inV;
    private String inVLabel;
    private int outV;
    private String outVLabel;

    private Map<String, GraphData> properties;

    public Edge() {

    }

    public Edge(int id, String label, String type, Map<String, GraphData> properties, int inV, String inVLabel, int outV, String outVLabel) {
        this.id = id;
        this.label = label;
        this.type = type;
        this.properties = properties;

        this.inV = inV;
        this.inVLabel = inVLabel;
        this.outV = outV;
        this.outVLabel = outVLabel;
    }

    public void setId(int id) {
        this.id = id;
    }

    public Integer getId() {
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

    public int getInV() {
        return inV;
    }

    public void setInV(int inV) {
        this.inV = inV;
    }

    public String getInVLabel() {
        return inVLabel;
    }

    public void setInVLabel(String inVLabel) {
        this.inVLabel = inVLabel;
    }

    public int getOutV() {
        return outV;
    }

    public void setOutV(int outV) {
        this.outV = outV;
    }

    public String getOutVLabel() {
        return outVLabel;
    }

    public void setOutVLabel(String outVLabel) {
        this.outVLabel = outVLabel;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Graph Edge [");
        sb.append(String.format("id = %d, label = %s, inV = %d, inVLabel = %s, outV = %d, outVLabel = %s", this.id, this.label, this.inV, this.inVLabel, this.outV, this.outVLabel));
        sb.append(", properties = [");
        int i = 0;
        for (Map.Entry<String, GraphData> entry : this.properties.entrySet()) {
            if (i > 0)
                sb.append(", ");
            sb.append(String.format("%s : %s", entry.getKey(), entry.getValue().toString()));
            i++;
        }
        sb.append("]");
        sb.append("]");
        return sb.toString();
    }
}