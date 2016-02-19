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
package com.datastax.driver.core.querybuilder;

import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.exceptions.InvalidQueryException;

import java.util.*;

/**
 * Builds CQL3 query via a fluent API.
 * <p/>
 * The queries built by this builder will provide a value for the
 * {@link com.datastax.driver.core.Statement#getRoutingKey} method only when a
 * {@link com.datastax.driver.core.TableMetadata} is provided to the builder.
 * It is thus advised to do so if a {@link com.datastax.driver.core.policies.TokenAwarePolicy}
 * is in use.
 * <p/>
 * The provider builders perform very little validation of the built query.
 * There is thus no guarantee that a built query is valid, and it is
 * definitively possible to create invalid queries.
 * <p/>
 * Note that it could be convenient to use an 'import static' to bring the static methods of
 * this class into scope.
 */
public final class QueryBuilder {

    private QueryBuilder() {
    }

    /**
     * Start building a new SELECT query that selects the provided names.
     * <p/>
     * Note that {@code select(c1, c2)} is just a shortcut for {@code select().column(c1).column(c2) }.
     *
     * @param columns the columns names that should be selected by the query.
     * @return an in-construction SELECT query (you will need to provide at
     * least a FROM clause to complete the query).
     */
    public static Select.Builder select(String... columns) {
        return new Select.Builder(Arrays.asList((Object[]) columns));
    }

    /**
     * Start building a new SELECT query.
     *
     * @return an in-construction SELECT query (you will need to provide a
     * column selection and at least a FROM clause to complete the query).
     */
    public static Select.Selection select() {
        // Note: the fact we return Select.Selection as return type is on purpose.
        return new Select.SelectionOrAlias();
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
     * @param table    the name of the table to insert into.
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
     * @param table    the name of the table to update.
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
        return new Delete.Builder(columns);
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
     * Built a new BATCH query on the provided statements.
     * <p/>
     * This method will build a logged batch (this is the default in CQL3). To
     * create unlogged batches, use {@link #unloggedBatch}. Also note that
     * for convenience, if the provided statements are counter statements, this
     * method will create a COUNTER batch even though COUNTER batches are never
     * logged (so for counters, using this method is effectively equivalent to
     * using {@link #unloggedBatch}).
     *
     * @param statements the statements to batch.
     * @return a new {@code RegularStatement} that batch {@code statements}.
     */
    public static Batch batch(RegularStatement... statements) {
        return new Batch(statements, true);
    }

    /**
     * Built a new UNLOGGED BATCH query on the provided statements.
     * <p/>
     * Compared to logged batches (the default), unlogged batch don't
     * use the distributed batch log server side and as such are not
     * guaranteed to be atomic. In other words, if an unlogged batch
     * timeout, some of the batched statements may have been persisted
     * while some have not. Unlogged batch will however be slightly
     * faster than logged batch.
     * <p/>
     * If the statements added to the batch are counter statements, the
     * resulting batch will be a COUNTER one.
     *
     * @param statements the statements to batch.
     * @return a new {@code RegularStatement} that batch {@code statements} without
     * using the batch log.
     */
    public static Batch unloggedBatch(RegularStatement... statements) {
        return new Batch(statements, false);
    }

    /**
     * Creates a new TRUNCATE query.
     *
     * @param table the name of the table to truncate.
     * @return the truncation query.
     */
    public static Truncate truncate(String table) {
        return new Truncate(null, table);
    }

    /**
     * Creates a new TRUNCATE query.
     *
     * @param keyspace the name of the keyspace to use.
     * @param table    the name of the table to truncate.
     * @return the truncation query.
     */
    public static Truncate truncate(String keyspace, String table) {
        return new Truncate(keyspace, table);
    }

    /**
     * Creates a new TRUNCATE query.
     *
     * @param table the table to truncate.
     * @return the truncation query.
     */
    public static Truncate truncate(TableMetadata table) {
        return new Truncate(table);
    }

    /**
     * Quotes a columnName to make it case sensitive.
     *
     * @param columnName the column name to quote.
     * @return the quoted column name.
     */
    public static String quote(String columnName) {
        return '"' + columnName + '"';
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
        sb.append(')');
        return sb.toString();
    }

    /**
     * The token of column names.
     * <p/>
     * This variant is most useful when the partition key is composite.
     *
     * @param columnNames the column names to take the token of.
     * @return a string representing the token of the provided column names.
     */
    public static String token(String... columnNames) {
        StringBuilder sb = new StringBuilder();
        sb.append("token(");
        Utils.joinAndAppendNames(sb, null, ",", Arrays.asList((Object[]) columnNames));
        sb.append(')');
        return sb.toString();
    }

    /**
     * Creates an "equal" where clause stating the provided column must be
     * equal to the provided value.
     *
     * @param name  the column name
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
     * @param name   the column name
     * @param values the values
     * @return the corresponding where clause.
     */
    public static Clause in(String name, Object... values) {
        return new Clause.InClause(name, Arrays.asList(values));
    }

    /**
     * Create an "in" where clause stating the provided column must be equal
     * to one of the provided values.
     *
     * @param name   the column name
     * @param values the values
     * @return the corresponding where clause.
     */
    public static Clause in(String name, List<?> values) {
        return new Clause.InClause(name, values);
    }

    /**
     * Creates a "contains" where clause stating the provided column must contain
     * the value provided.
     *
     * @param name  the column name
     * @param value the value
     * @return the corresponding where clause.
     */
    public static Clause contains(String name, Object value) {
        return new Clause.ContainsClause(name, value);
    }

    /**
     * Creates a "contains key" where clause stating the provided column must contain
     * the key provided.
     *
     * @param name the column name
     * @param key  the key
     * @return the corresponding where clause.
     */
    public static Clause containsKey(String name, Object key) {
        return new Clause.ContainsKeyClause(name, key);
    }

    /**
     * Creates a "lesser than" where clause stating the provided column must be less than
     * the provided value.
     *
     * @param name  the column name
     * @param value the value
     * @return the corresponding where clause.
     */
    public static Clause lt(String name, Object value) {
        return new Clause.SimpleClause(name, "<", value);
    }

    /**
     * Creates a "lesser than" where clause for a group of clustering columns.
     * <p/>
     * For instance, {@code lt(Arrays.asList("a", "b"), Arrays.asList(2, "test"))}
     * will generate the CQL WHERE clause {@code (a, b) &lt; (2, 'test') }.
     * <p/>
     * Please note that this variant is only supported starting with Cassandra 2.0.6.
     *
     * @param names  the column names
     * @param values the values
     * @return the corresponding where clause.
     * @throws IllegalArgumentException if {@code names.size() != values.size()}.
     */
    public static Clause lt(List<String> names, List<?> values) {
        if (names.size() != values.size())
            throw new IllegalArgumentException(String.format("The number of names (%d) and values (%d) don't match", names.size(), values.size()));

        return new Clause.CompoundClause(names, "<", values);
    }

    /**
     * Creates a "lesser than or equal" where clause stating the provided column must
     * be lesser than or equal to the provided value.
     *
     * @param name  the column name
     * @param value the value
     * @return the corresponding where clause.
     */
    public static Clause lte(String name, Object value) {
        return new Clause.SimpleClause(name, "<=", value);
    }

    /**
     * Creates a "lesser than or equal" where clause for a group of clustering columns.
     * <p/>
     * For instance, {@code lte(Arrays.asList("a", "b"), Arrays.asList(2, "test"))}
     * will generate the CQL WHERE clause {@code (a, b) &lte; (2, 'test') }.
     * <p/>
     * Please note that this variant is only supported starting with Cassandra 2.0.6.
     *
     * @param names  the column names
     * @param values the values
     * @return the corresponding where clause.
     * @throws IllegalArgumentException if {@code names.size() != values.size()}.
     */
    public static Clause lte(List<String> names, List<?> values) {
        if (names.size() != values.size())
            throw new IllegalArgumentException(String.format("The number of names (%d) and values (%d) don't match", names.size(), values.size()));

        return new Clause.CompoundClause(names, "<=", values);
    }

    /**
     * Creates a "greater than" where clause stating the provided column must
     * be greater to the provided value.
     *
     * @param name  the column name
     * @param value the value
     * @return the corresponding where clause.
     */
    public static Clause gt(String name, Object value) {
        return new Clause.SimpleClause(name, ">", value);
    }

    /**
     * Creates a "greater than" where clause for a group of clustering columns.
     * <p/>
     * For instance, {@code gt(Arrays.asList("a", "b"), Arrays.asList(2, "test"))}
     * will generate the CQL WHERE clause {@code (a, b) &gt; (2, 'test') }.
     * <p/>
     * Please note that this variant is only supported starting with Cassandra 2.0.6.
     *
     * @param names  the column names
     * @param values the values
     * @return the corresponding where clause.
     * @throws IllegalArgumentException if {@code names.size() != values.size()}.
     */
    public static Clause gt(List<String> names, List<?> values) {
        if (names.size() != values.size())
            throw new IllegalArgumentException(String.format("The number of names (%d) and values (%d) don't match", names.size(), values.size()));

        return new Clause.CompoundClause(names, ">", values);
    }

    /**
     * Creates a "greater than or equal" where clause stating the provided
     * column must be greater than or equal to the provided value.
     *
     * @param name  the column name
     * @param value the value
     * @return the corresponding where clause.
     */
    public static Clause gte(String name, Object value) {
        return new Clause.SimpleClause(name, ">=", value);
    }

    /**
     * Creates a "greater than or equal" where clause for a group of clustering columns.
     * <p/>
     * For instance, {@code gte(Arrays.asList("a", "b"), Arrays.asList(2, "test"))}
     * will generate the CQL WHERE clause {@code (a, b) &gte; (2, 'test') }.
     * <p/>
     * Please note that this variant is only supported starting with Cassandra 2.0.6.
     *
     * @param names  the column names
     * @param values the values
     * @return the corresponding where clause.
     * @throws IllegalArgumentException if {@code names.size() != values.size()}.
     */
    public static Clause gte(List<String> names, List<?> values) {
        if (names.size() != values.size())
            throw new IllegalArgumentException(String.format("The number of names (%d) and values (%d) don't match", names.size(), values.size()));

        return new Clause.CompoundClause(names, ">=", values);
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
     * @throws IllegalArgumentException if {@code timestamp &lt; 0}.
     */
    public static Using timestamp(long timestamp) {
        if (timestamp < 0)
            throw new IllegalArgumentException("Invalid timestamp, must be positive");

        return new Using.WithValue("TIMESTAMP", timestamp);
    }

    /**
     * Option to prepare the timestamp (in microseconds) for a modification query (insert, update or delete).
     *
     * @param marker bind marker to use for the timestamp.
     * @return the corresponding option.
     */
    public static Using timestamp(BindMarker marker) {
        return new Using.WithMarker("TIMESTAMP", marker);
    }

    /**
     * Option to set the ttl for a modification query (insert, update or delete).
     *
     * @param ttl the ttl (in seconds) to use.
     * @return the corresponding option
     * @throws IllegalArgumentException if {@code ttl &lt; 0}.
     */
    public static Using ttl(int ttl) {
        if (ttl < 0)
            throw new IllegalArgumentException("Invalid ttl, must be positive");

        return new Using.WithValue("TTL", ttl);
    }

    /**
     * Option to prepare the ttl (in seconds) for a modification query (insert, update or delete).
     *
     * @param marker bind marker to use for the ttl.
     * @return the corresponding option
     */
    public static Using ttl(BindMarker marker) {
        return new Using.WithMarker("TTL", marker);
    }

    /**
     * Simple "set" assignment of a value to a column.
     * <p/>
     * This will generate: {@code name = value}.
     *
     * @param name  the column name
     * @param value the value to assign
     * @return the correspond assignment (to use in an update query)
     */
    public static Assignment set(String name, Object value) {
        return new Assignment.SetAssignment(name, value);
    }

    /**
     * Incrementation of a counter column.
     * <p/>
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
     * <p/>
     * This will generate: {@code name = name + value}.
     *
     * @param name  the column name to increment
     * @param value the value by which to increment
     * @return the correspond assignment (to use in an update query)
     */
    public static Assignment incr(String name, long value) {
        return new Assignment.CounterAssignment(name, value, true);
    }

    /**
     * Incrementation of a counter column by a provided value.
     * <p/>
     * This will generate: {@code name = name + value}.
     *
     * @param name  the column name to increment
     * @param value a bind marker representing the value by which to increment
     * @return the correspond assignment (to use in an update query)
     */
    public static Assignment incr(String name, BindMarker value) {
        return new Assignment.CounterAssignment(name, value, true);
    }

    /**
     * Decrementation of a counter column.
     * <p/>
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
     * <p/>
     * This will generate: {@code name = name - value}.
     *
     * @param name  the column name to decrement
     * @param value the value by which to decrement
     * @return the correspond assignment (to use in an update query)
     */
    public static Assignment decr(String name, long value) {
        return new Assignment.CounterAssignment(name, value, false);
    }

    /**
     * Decrementation of a counter column by a provided value.
     * <p/>
     * This will generate: {@code name = name - value}.
     *
     * @param name  the column name to decrement
     * @param value a bind marker representing the value by which to decrement
     * @return the correspond assignment (to use in an update query)
     */
    public static Assignment decr(String name, BindMarker value) {
        return new Assignment.CounterAssignment(name, value, false);
    }

    /**
     * Prepend a value to a list column.
     * <p/>
     * This will generate: {@code name = [ value ] + name}.
     *
     * @param name  the column name (must be of type list).
     * @param value the value to prepend. Using a BindMarker here is not supported.
     *              To use a BindMarker use {@code QueryBuilder#prependAll} with a
     *              singleton list.
     * @return the correspond assignment (to use in an update query)
     */
    public static Assignment prepend(String name, Object value) {
        if (value instanceof BindMarker) {
            throw new InvalidQueryException("binding a value in prepend() is not supported, use prependAll() and bind a singleton list");
        }
        return prependAll(name, Collections.singletonList(value));
    }

    /**
     * Prepend a list of values to a list column.
     * <p/>
     * This will generate: {@code name = list + name}.
     *
     * @param name the column name (must be of type list).
     * @param list the list of values to prepend.
     * @return the correspond assignment (to use in an update query)
     */
    public static Assignment prependAll(String name, List<?> list) {
        return new Assignment.ListPrependAssignment(name, list);
    }

    /**
     * Prepend a list of values to a list column.
     * <p/>
     * This will generate: {@code name = list + name}.
     *
     * @param name the column name (must be of type list).
     * @param list a bind marker representing the list of values to prepend.
     * @return the correspond assignment (to use in an update query)
     */
    public static Assignment prependAll(String name, BindMarker list) {
        return new Assignment.ListPrependAssignment(name, list);
    }

    /**
     * Append a value to a list column.
     * <p/>
     * This will generate: {@code name = name + [value]}.
     *
     * @param name  the column name (must be of type list).
     * @param value the value to append. Using a BindMarker here is not supported.
     *              To use a BindMarker use {@code QueryBuilder#appendAll} with a
     *              singleton list.
     * @return the correspond assignment (to use in an update query)
     */
    public static Assignment append(String name, Object value) {
        if (value instanceof BindMarker) {
            throw new InvalidQueryException("Binding a value in append() is not supported, use appendAll() and bind a singleton list");
        }
        return appendAll(name, Collections.singletonList(value));
    }

    /**
     * Append a list of values to a list column.
     * <p/>
     * This will generate: {@code name = name + list}.
     *
     * @param name the column name (must be of type list).
     * @param list the list of values to append
     * @return the correspond assignment (to use in an update query)
     */
    public static Assignment appendAll(String name, List<?> list) {
        return new Assignment.CollectionAssignment(name, list, true, false);
    }

    /**
     * Append a list of values to a list column.
     * <p/>
     * This will generate: {@code name = name + list}.
     *
     * @param name the column name (must be of type list).
     * @param list a bind marker representing the list of values to append
     * @return the correspond assignment (to use in an update query)
     */
    public static Assignment appendAll(String name, BindMarker list) {
        return new Assignment.CollectionAssignment(name, list, true, false);
    }

    /**
     * Discard a value from a list column.
     * <p/>
     * This will generate: {@code name = name - [value]}.
     *
     * @param name  the column name (must be of type list).
     * @param value the value to discard.  Using a BindMarker here is not supported.
     *              To use a BindMarker use {@code QueryBuilder#discardAll} with a singleton list.
     * @return the correspond assignment (to use in an update query)
     */
    public static Assignment discard(String name, Object value) {
        if (value instanceof BindMarker) {
            throw new InvalidQueryException("Binding a value in discard() is not supported, use discardAll() and bind a singleton list");
        }
        return discardAll(name, Collections.singletonList(value));
    }

    /**
     * Discard a list of values to a list column.
     * <p/>
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
     * Discard a list of values to a list column.
     * <p/>
     * This will generate: {@code name = name - list}.
     *
     * @param name the column name (must be of type list).
     * @param list a bind marker representing the list of values to discard
     * @return the correspond assignment (to use in an update query)
     */
    public static Assignment discardAll(String name, BindMarker list) {
        return new Assignment.CollectionAssignment(name, list, false);
    }

    /**
     * Sets a list column value by index.
     * <p/>
     * This will generate: {@code name[idx] = value}.
     *
     * @param name  the column name (must be of type list).
     * @param idx   the index to set
     * @param value the value to set
     * @return the correspond assignment (to use in an update query)
     */
    public static Assignment setIdx(String name, int idx, Object value) {
        return new Assignment.ListSetIdxAssignment(name, idx, value);
    }

    /**
     * Adds a value to a set column.
     * <p/>
     * This will generate: {@code name = name + {value}}.
     *
     * @param name  the column name (must be of type set).
     * @param value the value to add. Using a BindMarker here is not supported.
     *              To use a BindMarker use {@code QueryBuilder#addAll} with a
     *              singleton set.
     * @return the correspond assignment (to use in an update query)
     */
    public static Assignment add(String name, Object value) {
        if (value instanceof BindMarker) {
            throw new InvalidQueryException("Binding a value in add() is not supported, use addAll() and bind a singleton list");
        }
        return addAll(name, Collections.singleton(value));
    }

    /**
     * Adds a set of values to a set column.
     * <p/>
     * This will generate: {@code name = name + set}.
     *
     * @param name the column name (must be of type set).
     * @param set  the set of values to append
     * @return the correspond assignment (to use in an update query)
     */
    public static Assignment addAll(String name, Set<?> set) {
        return new Assignment.CollectionAssignment(name, set, true);
    }

    /**
     * Adds a set of values to a set column.
     * <p/>
     * This will generate: {@code name = name + set}.
     *
     * @param name the column name (must be of type set).
     * @param set  a bind marker representing the set of values to append
     * @return the correspond assignment (to use in an update query)
     */
    public static Assignment addAll(String name, BindMarker set) {
        return new Assignment.CollectionAssignment(name, set, true);
    }

    /**
     * Remove a value from a set column.
     * <p/>
     * This will generate: {@code name = name - {value}}.
     *
     * @param name  the column name (must be of type set).
     * @param value the value to remove. Using a BindMarker here is not supported.
     *              To use a BindMarker use {@code QueryBuilder#removeAll} with a singleton set.
     * @return the correspond assignment (to use in an update query)
     */
    public static Assignment remove(String name, Object value) {
        if (value instanceof BindMarker) {
            throw new InvalidQueryException("Binding a value in remove() is not supported, use removeAll() and bind a singleton set");
        }
        return removeAll(name, Collections.singleton(value));
    }

    /**
     * Remove a set of values from a set column.
     * <p/>
     * This will generate: {@code name = name - set}.
     *
     * @param name the column name (must be of type set).
     * @param set  the set of values to remove
     * @return the correspond assignment (to use in an update query)
     */
    public static Assignment removeAll(String name, Set<?> set) {
        return new Assignment.CollectionAssignment(name, set, false);
    }

    /**
     * Remove a set of values from a set column.
     * <p/>
     * This will generate: {@code name = name - set}.
     *
     * @param name the column name (must be of type set).
     * @param set  a bind marker representing the set of values to remove
     * @return the correspond assignment (to use in an update query)
     */
    public static Assignment removeAll(String name, BindMarker set) {
        return new Assignment.CollectionAssignment(name, set, false);
    }

    /**
     * Puts a new key/value pair to a map column.
     * <p/>
     * This will generate: {@code name[key] = value}.
     *
     * @param name  the column name (must be of type map).
     * @param key   the key to put
     * @param value the value to put
     * @return the correspond assignment (to use in an update query)
     */
    public static Assignment put(String name, Object key, Object value) {
        return new Assignment.MapPutAssignment(name, key, value);
    }

    /**
     * Puts a map of new key/value pairs to a map column.
     * <p/>
     * This will generate: {@code name = name + map}.
     *
     * @param name the column name (must be of type map).
     * @param map  the map of key/value pairs to put
     * @return the correspond assignment (to use in an update query)
     */
    public static Assignment putAll(String name, Map<?, ?> map) {
        return new Assignment.CollectionAssignment(name, map, true);
    }

    /**
     * Puts a map of new key/value pairs to a map column.
     * <p/>
     * This will generate: {@code name = name + map}.
     *
     * @param name the column name (must be of type map).
     * @param map  a bind marker representing the map of key/value pairs to put
     * @return the correspond assignment (to use in an update query)
     */
    public static Assignment putAll(String name, BindMarker map) {
        return new Assignment.CollectionAssignment(name, map, true);
    }

    /**
     * An object representing an anonymous bind marker (a question mark).
     * <p/>
     * This can be used wherever a value is expected. For instance, one can do:
     * <pre>
     * {@code
     *     Insert i = QueryBuilder.insertInto("test").value("k", 0)
     *                                               .value("c", QueryBuilder.bindMarker());
     *     PreparedState p = session.prepare(i.toString());
     * }
     * </pre>
     *
     * @return a new bind marker.
     */
    public static BindMarker bindMarker() {
        return BindMarker.ANONYMOUS;
    }

    /**
     * An object representing a named bind marker.
     * <p/>
     * This can be used wherever a value is expected. For instance, one can do:
     * <pre>
     * {@code
     *     Insert i = QueryBuilder.insertInto("test").value("k", 0)
     *                                               .value("c", QueryBuilder.bindMarker("c_val"));
     *     PreparedState p = session.prepare(i.toString());
     * }
     * </pre>
     * <p/>
     * Please note that named bind makers are only supported starting with Cassandra 2.0.1.
     *
     * @param name the name for the bind marker.
     * @return an object representing a bind marker named {@code name}.
     */
    public static BindMarker bindMarker(String name) {
        return new BindMarker(name);
    }

    /**
     * Protects a value from any interpretation by the query builder.
     * <p/>
     * The following table exemplify the behavior of this function:
     * <table border=1>
     * <caption>Examples of use</caption>
     * <tr><th>Code</th><th>Resulting query string</th></tr>
     * <tr><td>{@code select().from("t").where(eq("c", "C'est la vie!")); }</td><td>{@code "SELECT * FROM t WHERE c='C''est la vie!';"}</td></tr>
     * <tr><td>{@code select().from("t").where(eq("c", raw("C'est la vie!"))); }</td><td>{@code "SELECT * FROM t WHERE c=C'est la vie!;"}</td></tr>
     * <tr><td>{@code select().from("t").where(eq("c", raw("'C'est la vie!'"))); }</td><td>{@code "SELECT * FROM t WHERE c='C'est la vie!';"}</td></tr>
     * <tr><td>{@code select().from("t").where(eq("c", "now()")); }</td><td>{@code "SELECT * FROM t WHERE c='now()';"}</td></tr>
     * <tr><td>{@code select().from("t").where(eq("c", raw("now()"))); }</td><td>{@code "SELECT * FROM t WHERE c=now();"}</td></tr>
     * </table>
     * <i>Note: the 2nd and 3rd examples in this table are not a valid CQL3 queries.</i>
     * <p/>
     * The use of that method is generally discouraged since it lead to security risks. However,
     * if you know what you are doing, it allows to escape the interpretations done by the
     * QueryBuilder.
     *
     * @param str the raw value to use as a string
     * @return the value but protected from being interpreted/escaped by the query builder.
     */
    public static Object raw(String str) {
        return new Utils.RawString(str);
    }

    /**
     * Creates a function call.
     *
     * @param name       the name of the function to call.
     * @param parameters the parameters for the function.
     * @return the function call.
     */
    public static Object fcall(String name, Object... parameters) {
        return new Utils.FCall(name, parameters);
    }

    /**
     * Creates a {@code now()} function call.
     *
     * @return the function call.
     */
    public static Object now() {
        return new Utils.FCall("now");
    }

    /**
     * Creates a {@code uuid()} function call.
     *
     * @return the function call.
     */
    public static Object uuid() {
        return new Utils.FCall("uuid");
    }

    /**
     * Declares that the name in argument should be treated as a column name.
     * <p/>
     * This mainly meant for use with {@link Select.Selection#fcall} when a
     * function should apply to a column name, not a string value.
     *
     * @param name the name of the column.
     * @return the name as a column name.
     */
    public static Object column(String name) {
        return new Utils.CName(name);
    }
}
