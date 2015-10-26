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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;


import com.datastax.driver.core.BoundStatement;

public class BoundGraphStatement extends AbstractGraphStatement {

    //  "name", "value"
    Map<String, String> arguments;

    BoundStatement bs;

    BoundGraphStatement(BoundStatement bs) {
        super();
        this.bs = bs;
        this.arguments = new HashMap<String, String>();
        //do stuff
    }

    String getJsonArgs() {
        // Constructs the JSON string argument according to the arguments Map
//        JSONObject obj = new JSONObject();
//        for (Map.Entry<String, String> argument : arguments.entrySet()) {
//            obj.put(argument.getKey(), argument.getValue());
//        }
//        return obj.toJSONString();
        return null;
    }

    /**
     * Bind Graph parameters values
     * @param name
     * @param value
     */
    public void setValue(String name, String value) {
        this.arguments.put(name, value);
    }


    // Bind variable in the PreparedStatement
    BoundStatement boundStatement() {
        String jsonArguments = this.getJsonArgs();

        // The name of the required parameter to use is not defined yet
        this.bs.setString("jsonArguments", jsonArguments);

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
        // TODO: find potential conflicts in using this option and the setGraphKeyspace()
        return null;
    }
}
