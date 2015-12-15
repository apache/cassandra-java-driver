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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * A default deserializer for graph results, creating Edge instances.
 */
public class DefaultEdgeDeserializer extends GraphJsonDeserializer<Edge> {
    @Override
    public Edge deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
        JsonNode jsonNode = jsonParser.getCodec().readTree(jsonParser);
        checkEdge(jsonNode);
        ObjectMapper objectMapper = new ObjectMapper();
        return new Edge(new GraphData("id", jsonNode.get("id"), objectMapper),
            jsonNode.get("label").asText(),
            jsonNode.get("type").asText(),
            transformEdgeProperties(jsonNode.get("properties")),
            new GraphData("inV", jsonNode.get("inV"), objectMapper),
            jsonNode.get("inVLabel").asText(),
            new GraphData("outV", jsonNode.get("inV"), objectMapper),
            jsonNode.get("outVLabel").asText());
    }
}