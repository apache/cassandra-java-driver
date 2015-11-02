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

import com.datastax.driver.core.Row;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.ParseException;

public class GraphTraversalResult {
    private final String jsonString;
    private final JSONObject jsonObject;

    // TODO: remove public (only made for temporary tests)
    public GraphTraversalResult(String result) {
        this.jsonString = result;
        this.jsonObject = (JSONObject)JSONValue.parse(this.jsonString);
    }

    public String getResultString() {
        return this.jsonString;
    }

    public GraphData get(String name) {
        Object result = this.jsonObject.get(name);
        return new GraphData(result);
    }

    public GraphData get() {
        return new GraphData(this.jsonObject);
    }

    static public GraphTraversalResult fromRow(Row row) {
        return row == null ? null : new GraphTraversalResult(row.getString("gremlin"));
    }
}
