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

import com.fasterxml.jackson.databind.JsonNode;

public class GraphData {

    private JsonNode jsonNode;

    public GraphData(JsonNode jsonNode) {
        this.jsonNode = jsonNode;
    }

    public GraphData get(Object what) {
        JsonNode jsN;
        if (what instanceof Integer) {
             jsN = this.jsonNode.get((Integer)what);
        } else {
            assert what instanceof String;
            jsN = this.jsonNode.get((String) what);
        }
        return new GraphData(jsN);
    }

    public Object getJsonObject() {
        return this.jsonNode;
    }

    public String asString() {
        return this.jsonNode.asText();
    }

    public int asInt() {
        return this.jsonNode.asInt();
    }

    public boolean asBool() {
        return this.jsonNode.asBoolean();
    }

    public Long asLong() {
        return this.jsonNode.asLong();
    }

    @Override
    public String toString() {
        return this.jsonNode.toString();
    }
}

