package com.datastax.driver.mapping;

import java.lang.reflect.Field;
import java.util.*;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.Select;
import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

abstract class EntityMapper<T> {

    public enum QueryType { SAVE, GET, DEL }

    public final Class<T> entityClass;
    public final String keyspace;
    public final String table;

    public final List<ColumnMapper<T>> partitionKeys = new ArrayList<ColumnMapper<T>>();
    public final List<ColumnMapper<T>> clusteringColumns = new ArrayList<ColumnMapper<T>>();
    public final List<ColumnMapper<T>> regularColumns = new ArrayList<ColumnMapper<T>>();

    private final List<ColumnMapper<T>> allColumns = new ArrayList<ColumnMapper<T>>();

    private volatile Map<QueryType, PreparedStatement> preparedQueries = new EnumMap<QueryType, PreparedStatement>(QueryType.class);

    protected EntityMapper(Class<T> entityClass, String keyspace, String table) {
        this.entityClass = entityClass;
        this.keyspace = keyspace;
        this.table = table;
    }

    public int primaryKeySize() {
        return partitionKeys.size() + clusteringColumns.size();
    }

    public ColumnMapper<T> getPrimaryKeyColumn(int i) {
        return i < partitionKeys.size() ? partitionKeys.get(i) : clusteringColumns.get(i - partitionKeys.size());
    }

    public void addColumns(List<ColumnMapper<T>> pks, List<ColumnMapper<T>> ccs, List<ColumnMapper<T>> rgs) {
        partitionKeys.addAll(pks);
        allColumns.addAll(pks);

        clusteringColumns.addAll(ccs);
        allColumns.addAll(ccs);

        regularColumns.addAll(rgs);
        allColumns.addAll(rgs);
    }

    public abstract T newEntity();

    public List<ColumnMapper<T>> allColumns() {
        return allColumns;
    }

    private String makePreparedQueryString(QueryType type) {
        switch (type) {
            case SAVE:
                Insert insert = insertInto(keyspace, table);
                for (ColumnMapper<T> cm : allColumns())
                    insert.value(cm.getColumnName(), bindMarker());
                return insert.toString();
            case GET:
                Select select = select().all().from(keyspace, table);
                Select.Where sWhere = select.where();
                for (int i = 0; i < primaryKeySize(); i++)
                    sWhere.and(eq(getPrimaryKeyColumn(i).columnName, bindMarker()));
                return select.toString();
            case DEL:
                Delete delete = delete().all().from(keyspace, table);
                Delete.Where dWhere = delete.where();
                for (int i = 0; i < primaryKeySize(); i++)
                    dWhere.and(eq(getPrimaryKeyColumn(i).columnName, bindMarker()));
                return delete.toString();
        }
        throw new AssertionError();
    }

    public PreparedStatement getPreparedQuery(Session session, QueryType type) {
        PreparedStatement stmt = preparedQueries.get(type);
        if (stmt == null) {
            synchronized (preparedQueries) {
                stmt = preparedQueries.get(type);
                if (stmt == null) {
                    stmt = session.prepare(makePreparedQueryString(type));
                    Map<QueryType, PreparedStatement> newQueries = new EnumMap<QueryType, PreparedStatement>(preparedQueries);
                    newQueries.put(type, stmt);
                    preparedQueries = newQueries;
                }
            }
        }
        return stmt;
    }

    interface Factory {
        public <T> EntityMapper<T> create(Class<T> entityClass, String keyspace, String table);
        public <T> ColumnMapper<T> createColumnMapper(Class<T> entityClass, Field field, int position);
    }
}
