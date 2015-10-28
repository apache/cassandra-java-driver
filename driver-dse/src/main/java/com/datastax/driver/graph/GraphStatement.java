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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.DriverException;

public class GraphStatement extends AbstractGraphStatement<SimpleStatement> {

    private final String query;
    private final Map<String, Object> valuesMap;
    private List<String> JsonParams;
    private int paramsHash;
    private volatile ByteBuffer routingKey;
    private final GraphSession session;

    protected GraphStatement(String query, GraphSession session, Object... values) {
        super();

        this.query = query;
        this.valuesMap = new HashMap<String, Object>();
        this.JsonParams = new ArrayList<String>();
        this.routingKey = null;

        addValuesMap(values);
        this.session = session;
    }

    public boolean hasValues() {
        return this.valuesMap.size() > 0;
    }

    /*
    Parameter values are supposed to be sent as JSON string in a particular format.
    The format is : {"parameterName":parameterValue}
     */
    private void processValues() {
        JsonNodeFactory factory = new JsonNodeFactory(false);
        JsonFactory jsonFactory = new JsonFactory();
        ObjectMapper objectMapper = new ObjectMapper();
        if (this.paramsHash == this.valuesMap.hashCode()) {
//            Avoids regenerating the Json params if the params haven't changed.
            return;
        }
        this.JsonParams.clear();
        try {
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
                this.JsonParams.add(stringWriter.toString());
            }
            this.paramsHash = this.valuesMap.hashCode();
        } catch (IOException e) {
            throw new DriverException("Some values are not in a compatible type to be serialized in a Gremlin Query.");
        }
    }

    private void addValuesMap(Object... values) {
        if (values == null || values.length == 0) {
            return;
        } else if ((values.length % 2) != 0) {
            throw new DriverException("Values in Graph statements must all be named");
        }
        for (int i = 0; i < values.length; i+=2) {
            this.valuesMap.put((String)values[i], values[i+1]);
        }
    }

    public void set(String name, Object value) {
        this.valuesMap.put(name, value);
    }

    @Override
    SimpleStatement configureAndGetWrappedStatement() {
        if (hasValues()) {
            processValues();
        }
        this.wrappedStatement = session.getSession().newSimpleStatement(this.query, this.JsonParams.toArray());
        configure();
        return this.wrappedStatement;
    }
}
