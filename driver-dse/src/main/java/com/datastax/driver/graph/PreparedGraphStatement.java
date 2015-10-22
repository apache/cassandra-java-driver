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

    PreparedGraphStatement(PreparedStatement ps) {
        this.ps = ps;
        this.gst = null;
    }

    PreparedGraphStatement(PreparedStatement ps, GraphStatement gst) {
        this.ps = ps;
        this.gst = gst;
    }

    BoundGraphStatement bind() {
        BoundGraphStatement bgs = new BoundGraphStatement(ps.bind());

        /*
        Need to keep the configuration from the Prepared statement to
        the created BoundStatement, so users don't have to re configure the payload and such
         */
        AbstractGraphStatement.configureFromStatement(gst, bgs);
        return bgs;
    }

    public String getQueryString() {
        return this.ps.getQueryString();
    }
}
