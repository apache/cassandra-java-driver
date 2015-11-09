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

import com.datastax.driver.core.PreparedStatement;

public class PreparedGraphStatement {
    final PreparedStatement ps;
    final GraphStatement gst;

    PreparedGraphStatement(PreparedStatement ps, GraphStatement gst) {
        this.ps = ps;
        this.gst = gst;
    }

    /**
     * API
     */

    /**
     * Bind the prepared statement.
     *
     * @return A {@link com.datastax.driver.graph.BoundGraphStatement} instance on which we can bind values and execute.
     */
    public BoundGraphStatement bind() {
        return new BoundGraphStatement(ps.bind(), gst);
    }

    /**
     * Get the query string prepared.
     *
     * @return The query string prepared.
     */
    public String getQueryString() {
        return this.ps.getQueryString();
    }
}
