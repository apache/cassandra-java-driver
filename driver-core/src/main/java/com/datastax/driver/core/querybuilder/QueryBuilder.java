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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;

/**
 * Static methods to build a CQL3 query.
 * <p>
 * The queries built by this builder will provide a value for the
 * {@link com.datastax.driver.core.Query#getRoutingKey} method only when a
 * {@link com.datastax.driver.core.TableMetadata} is provided to the builder.
 * It is thus advised to do so if a {@link com.datastax.driver.core.policies.TokenAwarePolicy}
 * is in use.
 * <p>
 * The provider builders perform very little validation of the built query.
 * There is thus no guarantee that a built query is valid, and it is
 * definitively possible to create invalid queries.
 * <p>
 * Note that it could be convenient to use an 'import static' to use the methods of this class.
 */
public final class QueryBuilder {

    static final Object BIND_MARKER = new Object() {};

    private QueryBuilder() {}

    /**
     * Start building a new SELECT query that selects the provided names.
     *
     * Note that {@code select(c1, c2)} is just a shortcut for {@code select().column(c1).column(c2) }.
     *
     * @param columns the columns names that should be selected by the query.
     * @return an in-construction SELECT query (you will need to provide at
     * least a FROM clause to complete the query).
     */
    public static Select.Builder select(String... columns) {
        return new Select.Builder(Arrays.asList(columns));
    }

    /**
     * Start building a new SELECT query.
     *
     * @return an in-construction SELECT query (you will need to provide a
     * column selection and at least a FROM clause to complete the query).
     */
    public static Select.Selection select() {
        return new Select.Selection();
    }

    /**
     * Start building a new INSERT query.
     *
     * @param table the name of the table in which to insert.
     * @return an in-construction INSERT query.
     */
    public static Insert insertInto(String table) {
        return new Insert(null, table);
    }

    /**
     * Start building a new INSERT query.
     *
     * @param keyspace the name of the keyspace to use.
     * @param table the name of the table to insert into.
     * @return an in-construction INSERT query.
     */
    public static Insert insertInto(String keyspace, String table) {
        return new Insert(keyspace, table);
    }

    /**
     * Start building a new INSERT query.
     *
     * @param table the name of the table to insert into.
     * @return an in-construction INSERT query.
     */
    public static Insert insertInto(TableMetadata table) {
        return new Insert(table);
    }

    /**
     * Start building a new UPDATE query.
     *
     * @param table the name of the table to update.
     * @return an in-construction UPDATE query (at least a SET and a WHERE
     * clause needs to be provided to complete the query).
     */
    public static Update update(String table) {
        return new Update(null, table);
    }

    /**
     * Start building a new UPDATE query.
     *
     * @param keyspace the name of the keyspace to use.
     * @param table the name of the table to update.
     * @return an in-construction UPDATE query (at least a SET and a WHERE
     * clause needs to be provided to complete the query).
     */
    public static Update update(String keyspace, String table) {
        return new Update(keyspace, table);
    }

    /**
     * Start building a new UPDATE query.
     *
     * @param table the name of the table to update.
     * @return an in-construction UPDATE query (at least a SET and a WHERE
     * clause needs to be provided to complete the query).
     */
    public static Update update(TableMetadata table) {
        return new Update(table);
    }

    /**
     * Start building a new DELETE query that deletes the provided names.
     *
     * @param columns the columns names that should be deleted by the query.
     * @return an in-construction DELETE query (At least a FROM and a WHERE
     * clause needs to be provided to complete the query).
     */
    public static Delete.Builder delete(String... columns) {
        return new Delete.Builder(Arrays.asList(columns));
    }

    /**
     * Start building a new DELETE query.
     *
     * @return an in-construction SELECT query (you will need to provide a
     * column selection and at least a FROM and a WHERE clause to complete the
     * query).
     */
    public static Delete.Selection delete() {
        return new Delete.Selection();
    }

    /**
     * Built a new BATCH query on the provided statement.
     *
     * @param statements the statements to batch.
     * @return a new {@code Statement} that batch {@code statements}.
     */
    public static Batch batch(Statement... statements) {
        return new Batch(statements);
    }

    /**
     * Quotes a columnName to make it case sensitive.
     *
     * @param columnName the column name to quote.
     * @return the quoted column name.
     */
    public static String quote(String columnName) {
        StringBuilder sb = new StringBuilder();
        sb.append("\"");
        Utils.appendName(columnName, sb);
        sb.append("\"");
        return sb.toString();
    }

    /**
     * The token of a column name.
     *
     * @param columnName the column name to take the token of.
     * @return {@code "token(" + columnName + ")"}.
     */
    public static String token(String columnName) {
        StringBuilder sb = new StringBuilder();
        sb.append("token(");
        Utils.appendName(columnName, sb);
        sb.append(")");
        return sb.toString();
    }

    /**
     * The token of column names.
     * <p>
     * This variant is most useful when the partition key is composite.
     *
     * @param columnNames the column names to take the token of.
     * @return a string reprensenting the token of the provided column names.
     */
    public static String token(String... columnNames) {
        StringBuilder sb = new StringBuilder();
        sb.append("token(");
        Utils.joinAndAppendNames(sb, ",", Arrays.asList(columnNames));
        sb.append(")");
        return sb.toString();
    }

    /**
     * Creates an "equal" where clause stating the provided column must be
     * equal to the provided value.
     *
     * @param name the column name
     * @param value the value
     * @return the corresponding where clause.
     */
    public static Clause eq(String name, Object value) {
        return new Clause.SimpleClause(name, "=", value);
    }

    /**
     * Create an "in" where clause stating the provided column must be equal
     * to one of the provided values.
     *
     * @param name the column name
     * @param values the values
     * @return the corresponding where clause.
     */
    public static Clause in(String name, Object... values) {
        return new Clause.InClause(name, Arrays.asList(values));
    }

    /**
     * Creates a "lesser than" where clause stating the provided column must be less than
     * the provided value.
     *
     * @param name the column name
     * @param value the value
     * @return the corresponding where clause.
     */
    public static Clause lt(String name, Object value) {
        return new Clause.SimpleClause(name, "<", value);
    }

    /**
     * Creates a "lesser than or equal" where clause stating the provided column must
     * be lesser than or equal to the provided value.
     *
     * @param name the column name
     * @param value the value
     * @return the corresponding where clause.
     */
    public static Clause lte(String name, Object value) {
        return new Clause.SimpleClause(name, "<=", value);
    }

    /**
     * Creates a "greater than" where clause stating the provided column must
     * be greater to the provided value.
     *
     * @param name the column name
     * @param value the value
     * @return the corresponding where clause.
     */
    public static Clause gt(String name, Object value) {
        return new Clause.SimpleClause(name, ">", value);
    }

    /**
     * Creates a "greater than or equal" where clause stating the provided
     * column must be greater than or equal to the provided value.
     *
     * @param name the column name
     * @param value the value
     * @return the corresponding where clause.
     */
    public static Clause gte(String name, Object value) {
        return new Clause.SimpleClause(name, ">=", value);
    }

    /**
     * Ascending ordering for the provided column.
     *
     * @param columnName the column name
     * @return the corresponding ordering
     */
    public static Ordering asc(String columnName) {
        return new Ordering(columnName, false);
    }

    /**
     * Descending ordering for the provided column.
     *
     * @param columnName the column name
     * @return the corresponding ordering
     */
    public static Ordering desc(String columnName) {
        return new Ordering(columnName, true);
    }

    /**
     * Option to set the timestamp for a modification query (insert, update or delete).
     *
     * @param timestamp the timestamp (in microseconds) to use.
     * @return the corresponding option
     *
     * @throws IllegalArgumentException if {@code timestamp &gt; 0}.
     */
    public static Using timestamp(long timestamp) {
        if (timestamp < 0)
            throw new IllegalArgumentException("Invalid timestamp, must be positive");

        return new Using("TIMESTAMP", timestamp);
    }

    /**
     * Option to set the ttl for a modification query (insert, update or delete).
     *
     * @param ttl the ttl (in seconds) to use.
     * @return the corresponding option
     *
     * @throws IllegalArgumentException if {@code ttl &gt; 0}.
     */
    public static Using ttl(int ttl) {
        if (ttl < 0)
            throw new IllegalArgumentException("Invalid ttl, must be positive");

        return new Using("TTL", ttl);
    }

    /**
     * Simple "set" assignement of a value to a column.
     * <p>
     * This will generate: {@code name = value}.
     *
     * @param name the column name
     * @param value the value to assign
     * @return the correspond assignment (to use in an update query)
     */
    public static Assignment set(String name, Object value) {
        return new Assignment.SetAssignment(name, value);
    }

    /**
     * Incrementation of a counter column.
     * <p>
     * This will generate: {@code name = name + 1}.
     *
     * @param name the column name to increment
     * @return the correspond assignment (to use in an update query)
     */
    public static Assignment incr(String name) {
        return incr(name, 1L);
    }

    /**
     * Incrementation of a counter column by a provided value.
     * <p>
     * This will generate: {@code name = name + value}.
     *
     * @param name the column name to increment
     * @param value the value by which to increment
     * @return the correspond assignment (to use in an update query)
     */
    public static Assignment incr(String name, long value) {
        return new Assignment.CounterAssignment(name, value, true);
    }

    /**
     * Decrementation of a counter column.
     * <p>
     * This will generate: {@code name = name - 1}.
     *
     * @param name the column name to decrement
     * @return the correspond assignment (to use in an update query)
     */
    public static Assignment decr(String name) {
        return decr(name, 1L);
    }

    /**
     * Decrementation of a counter column by a provided value.
     * <p>
     * This will generate: {@code name = name - value}.
     *
     * @param name the column name to decrement
     * @param value the value by which to decrement
     * @return the correspond assignment (to use in an update query)
     */
    public static Assignment decr(String name, long value) {
        return new Assignment.CounterAssignment(name, value, false);
    }

    /**
     * Prepend a value to a list column.
     * <p>
     * This will generate: {@code name = [ value ] + name}.
     *
     * @param name the column name (must be of type list).
     * @param value the value to prepend
     * @return the correspond assignment (to use in an update query)
     */
    public static Assignment prepend(String name, Object value) {
        return new Assignment.ListPrependAssignment(name, Collections.singletonList(value));
    }

    /**
     * Prepend a list of values to a list column.
     * <p>
     * This will generate: {@code name = list + name}.
     *
     * @param name the column name (must be of type list).
     * @param list the list of values to prepend
     * @return the correspond assignment (to use in an update query)
     */
    public static Assignment prependAll(String name, List<?> list) {
        return new Assignment.ListPrependAssignment(name, list);
    }

    /**
     * Append a value to a list column.
     * <p>
     * This will generate: {@code name = name + [value]}.
     *
     * @param name the column name (must be of type list).
     * @param value the value to append
     * @return the correspond assignment (to use in an update query)
     */
    public static Assignment append(String name, Object value) {
        return new Assignment.CollectionAssignment(name, Collections.singletonList(value), true);
    }

    /**
     * Append a list of values to a list column.
     * <p>
     * This will generate: {@code name = name + list}.
     *
     * @param name the column name (must be of type list).
     * @param list the list of values to append
     * @return the correspond assignment (to use in an update query)
     */
    public static Assignment appendAll(String name, List<?> list) {
        return new Assignment.CollectionAssignment(name, list, true);
    }

    /**
     * Discard a value from a list column.
     * <p>
     * This will generate: {@code name = name - [value]}.
     *
     * @param name the column name (must be of type list).
     * @param value the value to discard
     * @return the correspond assignment (to use in an update query)
     */
    public static Assignment discard(String name, Object value) {
        return new Assignment.CollectionAssignment(name, Collections.singletonList(value), false);
    }

    /**
     * Discard a list of values to a list column.
     * <p>
     * This will generate: {@code name = name - list}.
     *
     * @param name the column name (must be of type list).
     * @param list the list of values to discard
     * @return the correspond assignment (to use in an update query)
     */
    public static Assignment discardAll(String name, List<?> list) {
        return new Assignment.CollectionAssignment(name, list, false);
    }

    /**
     * Sets a list column value by index.
     * <p>
     * This will generate: {@code name[idx] = value}.
     *
     * @param name the column name (must be of type list).
     * @param idx the index to set
     * @param value the value to set
     * @return the correspond assignment (to use in an update query)
     */
    public static Assignment setIdx(String name, int idx, Object value) {
        return new Assignment.ListSetIdxAssignment(name, idx, value);
    }

    /**
     * Adds a value to a set column.
     * <p>
     * This will generate: {@code name = name + {value}}.
     *
     * @param name the column name (must be of type set).
     * @param value the value to add
     * @return the correspond assignment (to use in an update query)
     */
    public static Assignment add(String name, Object value) {
        return new Assignment.CollectionAssignment(name, Collections.singleton(value), true);
    }

    /**
     * Adds a set of values to a set column.
     * <p>
     * This will generate: {@code name = name + set}.
     *
     * @param name the column name (must be of type set).
     * @param set the set of values to append
     * @return the correspond assignment (to use in an update query)
     */
    public static Assignment addAll(String name, Set<?> set) {
        return new Assignment.CollectionAssignment(name, set, true);
    }

    /**
     * Remove a value from a set column.
     * <p>
     * This will generate: {@code name = name - {value}}.
     *
     * @param name the column name (must be of type set).
     * @param value the value to remove
     * @return the correspond assignment (to use in an update query)
     */
    public static Assignment remove(String name, Object value) {
        return new Assignment.CollectionAssignment(name, Collections.singleton(value), false);
    }

    /**
     * Remove a set of values from a set column.
     * <p>
     * This will generate: {@code name = name - set}.
     *
     * @param name the column name (must be of type set).
     * @param set the set of values to remove
     * @return the correspond assignment (to use in an update query)
     */
    public static Assignment removeAll(String name, Set<?> set) {
        return new Assignment.CollectionAssignment(name, set, false);
    }

    /**
     * Puts a new key/value pair to a map column.
     * <p>
     * This will generate: {@code name[key] = value}.
     *
     * @param name the column name (must be of type map).
     * @param key the key to put
     * @param value the value to put
     * @return the correspond assignment (to use in an update query)
     */
    public static Assignment put(String name, Object key, Object value) {
        return new Assignment.MapPutAssignment(name, key, value);
    }

    /**
     * Puts a map of new key/value paris to a map column.
     * <p>
     * This will generate: {@code name = name + map}.
     *
     * @param name the column name (must be of type map).
     * @param map the map of key/value pairs to put
     * @return the correspond assignment (to use in an update query)
     */
    public static Assignment putAll(String name, Map<?, ?> map) {
        return new Assignment.CollectionAssignment(name, map, true);
    }

    /**
     * An object representing a bind marker (a question mark).
     * <p>
     * This can be used wherever a value is expected. For instance, one can do:
     * <pre>
     * {@code
     *     Insert i = QueryBuilder.insertInto("test").value("k", 0)
     *                                               .value("c", QueryBuilder.bindMarker());
     *     PreparedState p = session.prepare(i.toString());
     * }
     * </pre>
     *
     * @return an object representing a bind marker.
     */
    public static Object bindMarker() {
        return BIND_MARKER;
    }
}
