/*
 * Copyright (C) 2012-2017 DataStax Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.driver.stress;

import com.datastax.driver.core.*;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

import java.util.Iterator;

public abstract class QueryGenerator implements Iterator<QueryGenerator.Request> {

    protected final int iterations;

    protected QueryGenerator(int iterations) {
        this.iterations = iterations;
    }

    public abstract int currentIteration();

    public int totalIterations() {
        return iterations;
    }

    public interface Builder {
        public String name();

        public OptionParser addOptions(OptionParser parser);

        public void prepare(OptionSet options, Session session);

        public QueryGenerator create(int id, int iterations, OptionSet options, Session session);
    }

    public interface Request {

        public ResultSet execute(Session session);

        public ResultSetFuture executeAsync(Session session);

        public static class SimpleQuery implements Request {

            private final Statement statement;

            public SimpleQuery(Statement statement) {
                this.statement = statement;
            }

            @Override
            public ResultSet execute(Session session) {
                return session.execute(statement);
            }

            @Override
            public ResultSetFuture executeAsync(Session session) {
                return session.executeAsync(statement);
            }
        }

        public static class PreparedQuery implements Request {

            private final BoundStatement query;

            public PreparedQuery(BoundStatement query) {
                this.query = query;
            }

            @Override
            public ResultSet execute(Session session) {
                return session.execute(query);
            }

            @Override
            public ResultSetFuture executeAsync(Session session) {
                return session.executeAsync(query);
            }
        }
    }
}
