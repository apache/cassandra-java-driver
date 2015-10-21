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

import com.datastax.driver.core.*;

public class GraphStatement extends AbstractGraphStatement {

    private final String query;

    protected GraphStatement(String query) {
        super();
        this.query = query;

    }

    @Override
    public String getQueryString() {
        return this.query;
    }

    @Override
    public ByteBuffer[] getValues() {
        // TODO when Graph works: Do we want to expose that.
        // Need to check if binding values on simple GraphStatements is legit.
        return new ByteBuffer[0];
    }

    @Override
    public boolean hasValues() {
        // TODO when Graph works : Same as getValues()
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
