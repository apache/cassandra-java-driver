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

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

public class GraphResultSet implements Iterable<GraphTraversalResult> {
    private final ResultSet rs;

    GraphResultSet(ResultSet rs) {
        this.rs = rs;
//        System.out.println("rs.one().getString(\"gremlin\") = " + );
    }


    public boolean isExhausted() {
        return rs.isExhausted();
    }

    public GraphTraversalResult one() {
        return GraphTraversalResult.fromRow(rs.one());
    }

    public List<GraphTraversalResult> all() {
        return Lists.transform(rs.all(), new Function<Row, GraphTraversalResult>() {
            @Override
            public GraphTraversalResult apply(Row input) {
                return GraphTraversalResult.fromRow(input);
            }
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
