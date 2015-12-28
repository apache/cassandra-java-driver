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

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.exceptions.DriverException;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

/**
 * Bound graph statement produced from a prepared statement.
 */
public class BoundGraphStatement extends AbstractGraphStatement<BoundStatement> {

    //                "name", "value"
    private final Map<String, Object> valuesMap;

    private PreparedStatement ps;


    BoundGraphStatement(PreparedStatement ps, GraphStatement gst) {
        super();

        this.ps = ps;
        this.valuesMap = new HashMap<String, Object>();

        /*
        Need to keep the configuration from the Prepared statement to
        the created BoundStatement, so users don't have to re configure the payload and such.
         */
        this.graphOptions.putAll(gst.getGraphOptions());
    }

    // Bind variables in the PreparedStatement
    @Override
    protected BoundStatement configureAndGetWrappedStatement(Map<String, String> sessionOptions) {
        JsonFactory jsonFactory = new JsonFactory();
        JsonNodeFactory factory = new JsonNodeFactory(false);
        int paramNumber = 0;
        this.wrappedStatement = this.ps.bind();
        try {
            for (Map.Entry<String, Object> param : this.valuesMap.entrySet()) {
                StringWriter stringWriter = new StringWriter();
                JsonGenerator generator = jsonFactory.createGenerator(stringWriter);
                ObjectNode parameter = factory.objectNode();
                String name = param.getKey();
                Object value = param.getValue();
                if (value instanceof Integer) {
                    parameter.put("name", name);
                    parameter.put("value", (Integer) value);
                } else if (value instanceof String) {
                    parameter.put("name", name);
                    parameter.put("value", (String) value);
                } else if (value instanceof Float) {
                    parameter.put("name", name);
                    parameter.put("value", (Float) value);
                } else if (value instanceof Double) {
                    parameter.put("name", name);
                    parameter.put("value", (Double) value);
                } else if (value instanceof Boolean) {
                    parameter.put("name", name);
                    parameter.put("value", (Boolean) value);
                } else if (value instanceof Long) {
                    parameter.put("name", name);
                    parameter.put("value", (Long) value);
                } else {
                    throw new DriverException("Parameter : " + value + ", is not in a valid format to be sent as Gremlin parameter.");
                }
                GraphSession.objectMapper.writeTree(generator, parameter);
                this.wrappedStatement.setString(paramNumber++, stringWriter.toString());
            }
        } catch (IOException e) {
            throw new DriverException("Some values are not in a compatible type to be serialized in a Gremlin Query.");
        }

        configure(sessionOptions);
        return this.wrappedStatement;
    }

    /**
     * API
     */

    /**
     * Set a parameter value for the statement.
     * <p/>
     * Values can be any type supported in JSON.
     *
     * @param name  Name of the value, defined in the query. Parameters in Gremlin are named as variables, no
     *              need for a CQL syntax like the bind marker "?" or the identifier ":" in front of a parameter.
     *              Please refer to Gremlin's documentation for more information.
     * @param value Any object serializable in JSON. The type will be detected automatically at statement's execution.
     */
    public void set(String name, Object value) {
        this.valuesMap.put(name, value);
    }
}
