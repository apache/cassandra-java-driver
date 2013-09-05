/*
 *      Copyright (C) 2012 DataStax Inc.
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
package com.datastax.driver.core.querybuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.datastax.driver.core.TableMetadata;

/**
 * A built INSERT statement.
 */
public class Insert extends BuiltStatement {

    private final String keyspace;
    private final String table;
    private final List<Object> names = new ArrayList<Object>();
    private final List<Object> values = new ArrayList<Object>();
    private final Options usings;

    Insert(String keyspace, String table) {
        super();
        this.keyspace = keyspace;
        this.table = table;
        this.usings = new Options(this);
    }

    Insert(TableMetadata table) {
        super(table);
        this.keyspace = table.getKeyspace().getName();
        this.table = table.getName();
        this.usings = new Options(this);
    }

    @Override
    protected StringBuilder buildQueryString() {
        StringBuilder builder = new StringBuilder();

        builder.append("INSERT INTO ");
        if (keyspace != null)
            Utils.appendName(keyspace, builder).append(".");
        Utils.appendName(table, builder);
        builder.append("(");
        Utils.joinAndAppendNames(builder, ",", names);
        builder.append(") VALUES (");
        Utils.joinAndAppendValues(builder, ",", values);
        builder.append(")");

        if (!usings.usings.isEmpty()) {
            builder.append(" USING ");
            Utils.joinAndAppend(builder, " AND ", usings.usings);
        }

        return builder;
    }

    /**
     * Adds a column/value pair to the values inserted by this INSERT statement.
     *
     * @param name the name of the column to insert/update.
     * @param value the value to insert/update for {@code name}.
     * @return this INSERT statement.
     */
    public Insert value(String name, Object value) {
        names.add(name);
        values.add(value);
        setDirty();
        maybeAddRoutingKey(name, value);
        return this;
    }

    /**
     * Adds multiple column/value pairs to the values inserted by this INSERT statement.
     *
     * @param names a list of column names to insert/update.
     * @param values a list of values to insert/update. The {@code i}th
     * value in {@code values} will be inserted for the {@code i}th column
     * in {@code names}.
     * @return this INSERT statement.
     *
     * @throws IllegalArgumentException if {@code names.length != values.length}.
     */
    public Insert values(String[] names, Object[] values) {
        if (names.length != values.length)
            throw new IllegalArgumentException(String.format("Got %d names but %d values", names.length, values.length));
        this.names.addAll(Arrays.asList(names));
        this.values.addAll(Arrays.asList(values));
        setDirty();

        for (int i = 0; i < names.length; i++)
            maybeAddRoutingKey(names[i], values[i]);
        return this;
    }

    /**
     * Adds a new options for this INSERT statement.
     *
     * @param using the option to add.
     * @return the options of this INSERT statement.
     */
    public Options using(Using using) {
        return usings.and(using);
    }

    /**
     * The options of an INSERT statement.
     */
    public static class Options extends BuiltStatement.ForwardingStatement<Insert> {

        private final List<Using> usings = new ArrayList<Using>();

        Options(Insert st) {
            super(st);
        }

        /**
         * Adds the provided option.
         *
         * @param using an INSERT option.
         * @return this {@code Options} object.
         */
        public Options and(Using using) {
            usings.add(using);
            setDirty();
            return this;
        }

        /**
         * Adds a column/value pair to the values inserted by this INSERT statement.
         *
         * @param name the name of the column to insert/update.
         * @param value the value to insert/update for {@code name}.
         * @return the INSERT statement those options are part of.
         */
        public Insert value(String name, Object value) {
            return statement.value(name, value);
        }

        /**
         * Adds multiple column/value pairs to the values inserted by this INSERT statement.
         *
         * @param names a list of column names to insert/update.
         * @param values a list of values to insert/update. The {@code i}th
         * value in {@code values} will be inserted for the {@code i}th column
         * in {@code names}.
         * @return the INSERT statement those options are part of.
         *
         * @throws IllegalArgumentException if {@code names.length != values.length}.
         */
        public Insert values(String[] names, Object[] values) {
            return statement.values(names, values);
        }
    }
}
