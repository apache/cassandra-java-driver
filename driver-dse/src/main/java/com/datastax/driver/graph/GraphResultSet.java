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

import java.util.Iterator;
import java.util.List;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

public class GraphResultSet implements Iterable<GraphTraversalResult> {
    private final ResultSet rs;
    private final ObjectMapper objectMapper;
    private JsonParser jp;

    GraphResultSet(ResultSet rs) {
        this.rs = rs;
        this.objectMapper = new ObjectMapper();
    }

    public boolean isExhausted() {
        return rs.isExhausted();
    }

    public GraphTraversalResult one() {
        GraphTraversalResult grs = GraphTraversalResult.fromRow(rs.one(), this.objectMapper);
        return grs;
//        Row row = rs.one();
//        String json = row.getString("gremlin");
//        return new GraphTraversalResult(row, this.objectMapper);
    }

    public List<GraphTraversalResult> all() {
        return Lists.transform(rs.all(), new Function<Row, GraphTraversalResult>() {
            @Override
            public GraphTraversalResult apply(Row input) {
                GraphTraversalResult grs = GraphTraversalResult.fromRow(rs.one(), objectMapper);
                return grs;            }
        });
    }

    public Iterator<GraphTraversalResult> iterator() {
        return new Iterator<GraphTraversalResult>() {
            @Override
            public boolean hasNext() {
                return !rs.isExhausted();
            }

            @Override
            public GraphTraversalResult next() {
                return GraphResultSet.this.one();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    public int getAvailableWithoutFetching() {
        return rs.getAvailableWithoutFetching();
    }

    public boolean isFullyFetched() {
        return rs.isFullyFetched();
    }

    public ListenableFuture<GraphResultSet> fetchMoreResults() {
        return Futures.transform(rs.fetchMoreResults(), new Function<ResultSet, GraphResultSet>() {
            @Override
            public GraphResultSet apply(ResultSet input) {
                return new GraphResultSet(input);
            }
        });
    }

    public ExecutionInfo getExecutionInfo() {
        return rs.getExecutionInfo();
    }

    public List<ExecutionInfo> getAllExecutionInfo() {
        return rs.getAllExecutionInfo();
    }

    public boolean wasApplied() {
        return rs.wasApplied();
    }
}
