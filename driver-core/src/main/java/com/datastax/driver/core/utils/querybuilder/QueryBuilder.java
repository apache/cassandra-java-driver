package com.datastax.driver.core.utils.querybuilder;

import com.datastax.driver.core.CQLStatement;
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
public abstract class QueryBuilder {

    private QueryBuilder() {}

    private static final String[] ALL = new String[0];
    private static final String[] COUNT_ALL = new String[]{ "count(*)" };

    /**
     * Start building a new SELECT query.
     *
     * @param columns the columns names that should be selected by the query.
     * If empty, all columns are selected (it's a 'SELECT * ...'), but you can
     * alternatively use {@link #all} to achieve the same effect (select all
     * columns).
     * @return an in-construction SELECT query (you will need to provide at
     * least a FROM clause to complete the query).
     */
    public static Select.Builder select(String... columns) {
        return new Select.Builder(columns);
    }

    /**
     * Represents the selection of all columns (for either a SELECT or a DELETE query).
     *
     * @return an empty array.
     */
    public static String[] all() {
        return ALL;
    }

    /**
     * Count the returned rows in a SELECT query.
     *
     * @return an array containing "count(*)" as sole element.
     */
    public static String[] count() {
        return COUNT_ALL;
    }

    /**
     * Select the write time of the provided column.
     *
     * @param columnName the name of the column for which to select the write
     * time.
     * @return {@code "writeTime(" + columnName + ")"}.
     */
    public static String writeTime(String columnName) {
        StringBuilder sb = new StringBuilder();
        sb.append("writetime(");
        Utils.appendName(columnName, sb);
        sb.append(")");
        return sb.toString();
    }

    /**
     * Select the ttl of the provided column.
     *
     * @param columnName the name of the column for which to select the ttl.
     * @return {@code "ttl(" + columnName + ")"}.
     */
    public static String ttl(String columnName) {
        StringBuilder sb = new StringBuilder();
        sb.append("ttl(");
        Utils.appendName(columnName, sb);
        sb.append(")");
        return sb.toString();
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
     * @param columnName the column names to take the token of.
     * @return a string reprensenting the token of the provided column names.
     */
    public static String token(String... columnNames) {
        StringBuilder sb = new StringBuilder();
        sb.append("token(");
        Utils.joinAndAppendNames(sb, ",", columnNames);
        sb.append(")");
        return sb.toString();
    }

    /**
     * Start building a new INSERT query.
     *
     * @param columns the columns names that should be inserted by the query.
     * @return an in-construction INSERT query (At least a FROM and a VALUES
     * clause needs to be provided to complete the query).
     *
     * @throws IllegalArgumentException if {@code columns} is empty.
     */
    public static Insert.Builder insert(String... columns) {
        return new Insert.Builder(columns);
    }

    /**
     * Start building a new UPDATE query.
     *
     * @param table the name of the table to update.
     * @return an in-construction UPDATE query (At least a SET and a WHERE
     * clause needs to be provided to complete the query).
     */
    public static Update.Builder update(String table) {
        return new Update.Builder(null, table);
    }

    /**
     * Start building a new UPDATE query.
     *
     * @param keyspace the name of the keyspace to use.
     * @param table the name of the table to update.
     * @return an in-construction UPDATE query (At least a SET and a WHERE
     * clause needs to be provided to complete the query).
     */
    public static Update.Builder update(String keyspace, String table) {
        return new Update.Builder(keyspace, table);
    }

    /**
     * Start building a new UPDATE query.
     *
     * @param table the name of the table to update.
     * @return an in-construction UPDATE query (At least a SET and a WHERE
     * clause needs to be provided to complete the query).
     */
    public static Update.Builder update(TableMetadata table) {
        return new Update.Builder(table);
    }

    /**
     * Start building a new DELETE query.
     *
     * @param columns the columns names that should be deleted by the query.
     * If empty, all columns are deleted, but you can alternatively use {@link #all}
     * to achieve the same effect (delete all columns).
     * @return an in-construction DELETE query (At least a FROM and a WHERE
     * clause needs to be provided to complete the query).
     */
    public static Delete.Builder delete(String... columns) {
        return new Delete.Builder(columns);
    }

    /**
     * Selects an element of a list by index.
     *
     * @param columnName the name of the list column.
     * @param idx the index to select.
     * @return {@code columnName[idx]}.
     */
    public static String listElt(String columnName, int idx) {
        StringBuilder sb = new StringBuilder();
        Utils.appendName(columnName, sb);
        return sb.append("[").append(idx).append("]").toString();
    }

    /**
     * Selects an element of a map given a key.
     *
     * @param columnName the name of the map column.
     * @param key the key to select with.
     * @return {@code columnName[key]}.
     */
    public static String mapElt(String columnName, Object key) {
        StringBuilder sb = new StringBuilder();
        Utils.appendName(columnName, sb);
        sb.append("[");
        Utils.appendFlatValue(key, sb);
        return sb.append("]").toString();
    }

    /**
     * Built a new BATCH query on the provided statement.
     *
     * @param statements the statements to batch.
     * @return a new {@code CQLStatement} that batch {@code statements}.
     */
    public static Batch batch(CQLStatement... statements) {
        return new Batch(statements);
    }
}
