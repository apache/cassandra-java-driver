package com.datastax.driver.core.utils.querybuilder;

import com.datastax.driver.core.CQLStatement;
import com.datastax.driver.core.TableMetadata;

public abstract class QueryBuilder {

    private QueryBuilder() {}

    private static final String[] ALL = new String[0];
    private static final String[] COUNT_ALL = new String[]{ "count(*)" };

    public static Select.Builder select(String... columns) {
        return new Select.Builder(columns);
    }

    public static String writeTime(String columnName) {
        StringBuilder sb = new StringBuilder();
        sb.append("writetime(");
        Utils.appendName(columnName, sb);
        sb.append(")");
        return sb.toString();
    }

    public static String[] all() {
        return ALL;
    }

    public static String[] count() {
        return COUNT_ALL;
    }

    public static String ttl(String columnName) {
        StringBuilder sb = new StringBuilder();
        sb.append("ttl(");
        Utils.appendName(columnName, sb);
        sb.append(")");
        return sb.toString();
    }

    public static String quote(String columnName) {
        StringBuilder sb = new StringBuilder();
        sb.append("\"");
        Utils.appendName(columnName, sb);
        sb.append("\"");
        return sb.toString();
    }

    public static String token(String columnName) {
        StringBuilder sb = new StringBuilder();
        sb.append("token(");
        Utils.appendName(columnName, sb);
        sb.append(")");
        return sb.toString();
    }

    public static String token(String... columnNames) {
        StringBuilder sb = new StringBuilder();
        sb.append("token(");
        Utils.joinAndAppendNames(sb, ",", columnNames);
        sb.append(")");
        return sb.toString();
    }

    public static Insert.Builder insert(String... columns) {
        return new Insert.Builder(columns);
    }

    public static Update.Builder update(String table) {
        return new Update.Builder(null, table);
    }

    public static Update.Builder update(String keyspace, String table) {
        return new Update.Builder(keyspace, table);
    }

    public static Update.Builder update(TableMetadata table) {
        return new Update.Builder(table);
    }

    public static Delete.Builder delete(String... columns) {
        return new Delete.Builder(columns);
    }

    public static String listElt(String columnName, int idx) {
        StringBuilder sb = new StringBuilder();
        Utils.appendName(columnName, sb);
        return sb.append("[").append(idx).append("]").toString();
    }

    public static String mapElt(String columnName, Object key) {
        StringBuilder sb = new StringBuilder();
        Utils.appendName(columnName, sb);
        sb.append("[");
        Utils.appendFlatValue(key, sb);
        return sb.append("]").toString();
    }

    public static Batch batch(CQLStatement... statements) {
        return new Batch(statements);
    }
}
