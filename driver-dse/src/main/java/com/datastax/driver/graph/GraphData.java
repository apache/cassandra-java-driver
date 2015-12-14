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

import com.datastax.driver.core.exceptions.DriverException;

/**
 * A result entity containing a graph query's result, wrapping a Json result.
 */
public class GraphData {

    private JsonNode jsonNode;

    private ObjectMapper objectMapper;

    private Object key;

    GraphData(Object key, JsonNode jsonNode, ObjectMapper objectMapper) {
        this.jsonNode = jsonNode;
        this.objectMapper = objectMapper;
        this.key = key;
    }

    /**
     * API
     */

    /**
     * This method allows digging through the result received from the graph server.
     *
     * @param keyOrIndex The key to get the value from, if keyOrIndex is a String. The index of the Array, if it is an Integer.
     * @return A new GraphData object containing the encapsulated result.
     */
    public GraphData get(Object keyOrIndex) {
        if (keyOrIndex == null) {
            throw new DriverException("You must provide a valid key or index identifier in a get() call, 'null' is not valid.");
        }
        if (this.jsonNode == null) {
            return new GraphData(keyOrIndex, null, this.objectMapper);
        }
        JsonNode jsN;
        if (keyOrIndex instanceof Integer) {
            jsN = this.jsonNode.get((Integer)keyOrIndex);
        } else {
            assert keyOrIndex instanceof String;
            jsN = this.jsonNode.get((String)keyOrIndex);
        }
        return new GraphData(keyOrIndex, jsN, this.objectMapper);
    }

    /**
     * Get the raw enclosed JSON object.
     *
     * @return The enclosed JSON object.
     */
    public JsonNode getJsonObject() {
        return this.jsonNode;
    }

    /**
     * Return the encapsulated result as a String.
     *
     * @return A String of the encapsulated result.
     */
    public String asString() {
        if (this.jsonNode != null)
            return this.jsonNode.asText();
        return null;
    }

    /**
     * Return the encapsulated result as an integer.
     *
     * @return An integer of the encapsulated result.
     */
    public Integer asInt() {
        if (this.jsonNode != null)
            return this.jsonNode.asInt();
        return null;
    }

    /**
     * Return the encapsulated result as a boolean.
     *
     * @return A boolean of the encapsulated result.
     */
    public Boolean asBool() {
        if (this.jsonNode != null)
            return this.jsonNode.asBoolean();
        return null;
    }

    /**
     * Return the encapsulated result as a long integer.
     *
     * @return A long integer of the encapsulated result.
     */
    public Long asLong() {
        if (this.jsonNode != null)
            return this.jsonNode.asLong();
        return null;
    }

    /**
     * Return the encapsulated result as a double.
     *
     * @return A double of the encapsulated result.
     */
    public Double asDouble() {
        if (this.jsonNode != null)
            return this.jsonNode.asDouble();
        return null;
    }

    /**
     * Return the encapsulated result deserialized in a T object that extends Vertex.
     *
     * The contained Json object will be parsed according to a class that must extend Vertex. Note that
     * a deserialiser must also be known for this class. It means that the deserialized class must have a
     * @JsonDeserialize and provide the name of the class able to deserialize this T object.
     *
     * @return A T object parsed from the result contained in the GraphData object. This method
     * can throw an exception if the deserializer is incorrect, or if the method
     * {@link com.datastax.driver.graph.GraphJsonDeserializer#checkVertex(com.fasterxml.jackson.databind.JsonNode)}
     * does not validate the result as a Vertex result.
     */
    public <T extends Vertex> T asVertex(Class<T> clas) {
        try {
            if (this.jsonNode != null) {
                return this.objectMapper.readValue(this.jsonNode.toString(), clas);
            }
            return null;

        } catch (IOException e) {
            throw new DriverException("Could not create a Edge object from the result due to a deserialisation exception. If you provided a custom implementation of the Vertex class deserializer, "
                + "please check the validity of this. Nested parsing exception : " + e);
        }
    }

    /**
     * Return the contained result as a {@link Vertex} object.
     *
     * @return The Vertex object.
     */
    public Vertex asVertex() {
        return asVertex(Vertex.class);
    }

    /**
     * Return the encapsulated result deserialized in a T object that extends Edge.
     * <p/>
     * The contained Json object will be parsed according to a class that must extend Edge. Note that
     * a deserializer must also be known for this class. It means that the deserialized class must have a
     *
     * @return A T object parsed from the result contained in the GraphData object. This method
     * can throw an exception if the deserializer is incorrect, or if the method
     * {@link com.datastax.driver.graph.GraphJsonDeserializer#checkEdge(com.fasterxml.jackson.databind.JsonNode)}
     * does not validate the result as a Edge result.
     * @JsonDeserialize and provide the name of the class able to deserialize this T object.
     */
    public <T extends Edge> T asEdge(Class<T> clas) {
        try {
            if (this.jsonNode != null) {
                return this.objectMapper.readValue(this.jsonNode.toString(), clas);
            }
            return null;
        } catch (IOException e) {
            throw new DriverException("Could not create a Edge object from the result due to a deserialisation exception. If you provided a custom implementation of the Edge class deserializer, "
                + "please check the validity of this. Nested parsing exception : " + e);
        }
    }

    /**
     * Return the contained result as a {@link Edge} object.
     *
     * @return The Edge object.
     */
    public Edge asEdge() {
        return asEdge(Edge.class);
    }

    @Override
    public String toString() {

        return this.jsonNode != null
            ? this.jsonNode.toString()
            : null;
    }
}

