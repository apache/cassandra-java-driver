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
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.exceptions.DriverException;

public class BoundGraphStatement extends AbstractGraphStatement {

    //  "name", "value"
    Map<String, Object> valuesMap;

    BoundStatement bs;

    BoundGraphStatement(BoundStatement bs) {
        super();
        this.bs = bs;
        this.valuesMap = new HashMap<String, Object>();
        //do stuff
    }

    /**
     * Bind Graph parameters values
     * @param name
     * @param value
     */
    public void set(String name, Object value) {
        this.valuesMap.put(name, value);
    }


    // Bind variable in the PreparedStatement
    BoundStatement boundStatement() {
        JsonNodeFactory factory = new JsonNodeFactory(false);
        JsonFactory jsonFactory = new JsonFactory();
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            int paramNumber = 0;
            for (Map.Entry<String, Object> param : this.valuesMap.entrySet()) {
                StringWriter stringWriter = new StringWriter();
                JsonGenerator generator = jsonFactory.createGenerator(stringWriter);
                ObjectNode parameter = factory.objectNode();
                String name = param.getKey();
                Object value = param.getValue();
                if (value instanceof Integer) {
                    parameter.put("name", name);
                    parameter.put("value", (Integer)value);
                } else if (value instanceof String) {
                    parameter.put("name", name);
                    parameter.put("value", (String)value);
                } else if (value instanceof Float) {
                    parameter.put("name", name);
                    parameter.put("value", (Float)value);
                } else if (value instanceof Double) {
                    parameter.put("name", name);
                    parameter.put("value", (Double)value);
                } else if (value instanceof Boolean) {
                    parameter.put("name", name);
                    parameter.put("value", (Boolean)value);
                } else if (value instanceof Long) {
                    parameter.put("name", name);
                    parameter.put("value", (Long)value);
                } else {
                    throw new DriverException("Parameter : " + value + ", is not in a valid format to be sent as Gremlin parameter.");
                }
                objectMapper.writeTree(generator, parameter);
                this.bs.setString(paramNumber++, stringWriter.toString());
            }
        } catch (IOException e) {
            throw new DriverException("Some values are not in a compatible type to be serialized in a Gremlin Query.");
        }
        return this.bs;
    }

    @Override
    public String getQueryString() {
        return this.bs.preparedStatement().getQueryString();
    }

    @Override
    public ByteBuffer[] getValues() {
        // TODO when Graph works: Do we want to expose that.
        return new ByteBuffer[0];
    }

    @Override
    public boolean hasValues() {
        // TODO when Graph works : Same as above
        return false;
    }

    @Override
    public ByteBuffer getRoutingKey() {
        // TODO: Use the graph routing key mechanism.
        return null;
    }

    @Override
    public String getKeyspace() {
        // Conflicting with the Graph keyspace property, this will not be used with Graph statements.
        return null;
    }
}
