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

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.datastax.driver.core.DataType;

public class GraphData {

    private Object maybeJsonObject;

    public GraphData(Object object) {
        this.maybeJsonObject = object;
    }

    public GraphData get(Object what) {
        if (this.maybeJsonObject instanceof JSONArray) {
            JSONArray jArray = (JSONArray) this.maybeJsonObject;
            return new GraphData(jArray.get((Integer)what));
        }
        if (this.maybeJsonObject instanceof JSONObject) {
            JSONObject jObj = (JSONObject) this.maybeJsonObject;
            GraphData gd = new GraphData(jObj.get(what));
            return new GraphData(jObj.get(what));
        } else {
            return null;
        }
    }

    public Object getObject() {
        return this.maybeJsonObject;
    }

    public String getAsString() {
        return (String) this.maybeJsonObject;
    }

    //TODO: Maybe add getAsVertex(), getAsEdge(), and stuff

    @Override
    public String toString() {
        return this.maybeJsonObject.toString();
    }
}
