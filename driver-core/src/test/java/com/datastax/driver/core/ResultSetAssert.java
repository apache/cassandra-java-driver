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
package com.datastax.driver.core;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.assertj.core.api.AbstractListAssert;
import org.assertj.core.data.Index;

import java.util.Arrays;
import java.util.List;

// better use ListAssert than IterableAssert to avoid attempting to consume the resultset more than once
public class ResultSetAssert extends AbstractListAssert<ResultSetAssert, List<Row>, Row> {

    // a helper assert object to simplify assertions on row contents
    // by considering a row as a mere tuple
    private final TupleListAssert helper;

    public ResultSetAssert(ResultSet actual) {
        super(actual.all(), ResultSetAssert.class);
        helper = new TupleListAssert(Lists.transform(this.actual, ROW_TO_TUPLE));
    }

    public static Tuple row(Object... cols) {
        return new Tuple(cols);
    }

    public ResultSetAssert contains(Tuple value, Index index) {
        helper.contains(value, index);
        return this;
    }

    public ResultSetAssert containsSubsequence(Tuple... sequence) {
        helper.containsSubsequence(sequence);
        return this;
    }

    public ResultSetAssert containsOnly(Tuple... values) {
        helper.containsOnly(values);
        return this;
    }

    public ResultSetAssert endsWith(Tuple... sequence) {
        helper.endsWith(sequence);
        return this;
    }

    public ResultSetAssert startsWith(Tuple... sequence) {
        helper.startsWith(sequence);
        return this;
    }

    public ResultSetAssert doesNotContain(Tuple value, Index index) {
        helper.doesNotContain(value, index);
        return this;
    }

    public ResultSetAssert doesNotContain(Tuple... values) {
        helper.doesNotContain(values);
        return this;
    }

    public ResultSetAssert containsExactly(Tuple... values) {
        helper.containsExactly(values);
        return this;
    }

    public ResultSetAssert containsOnlyOnce(Tuple... values) {
        helper.containsOnlyOnce(values);
        return this;
    }

    public ResultSetAssert contains(Tuple... values) {
        helper.contains(values);
        return this;
    }

    public ResultSetAssert containsSequence(Tuple... sequence) {
        helper.containsSequence(sequence);
        return this;
    }

    public static class Tuple {

        final Object[] cols;

        public Tuple(Object... cols) {
            this.cols = cols;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (getClass() != o.getClass()) return false;
            Tuple tuple = (Tuple) o;
            return Arrays.equals(cols, tuple.cols);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(cols);
        }

        @Override
        public String toString() {
            return '(' + Joiner.on(',').join(cols) + ')';
        }
    }

    private static final Function<Row, Tuple> ROW_TO_TUPLE = new Function<Row, Tuple>() {
        @Override
        public Tuple apply(Row input) {
            Object[] cols = new Object[input.getColumnDefinitions().size()];
            for (int i = 0; i < input.getColumnDefinitions().size(); i++) {
                cols[i] = input.getObject(i);
            }
            return new Tuple(cols);
        }
    };

    private static class TupleListAssert extends AbstractListAssert<TupleListAssert, List<Tuple>, Tuple> {

        private TupleListAssert(List<Tuple> rows) {
            super(rows, TupleListAssert.class);
        }
        
    }

}
