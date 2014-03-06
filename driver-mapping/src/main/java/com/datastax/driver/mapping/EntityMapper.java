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

    public static class QueryType {
        private enum Kind { SAVE, GET, DEL, SLICE, REVERSED_SLICE };
        private final Kind kind;

        // For slices
        private final int startBoundSize;
        private final boolean startInclusive;
        private final int endBoundSize;
        private final boolean endInclusive;

        public static final QueryType save = new QueryType(Kind.SAVE);
        public static final QueryType del = new QueryType(Kind.DEL);
        public static final QueryType get = new QueryType(Kind.GET);

        private QueryType(Kind kind) {
            this(kind, 0, false, 0, false);
        }

        private QueryType(Kind kind, int startBoundSize, boolean startInclusive, int endBoundSize, boolean endInclusive) {
            this.kind = kind;
            this.startBoundSize = startBoundSize;
            this.startInclusive = startInclusive;
            this.endBoundSize = endBoundSize;
            this.endInclusive = endInclusive;
        }

        public static QueryType slice(int startBoundSize, boolean startInclusive, int endBoundSize, boolean endInclusive, boolean reversed) {
            return new QueryType(reversed ? Kind.REVERSED_SLICE : Kind.SLICE, startBoundSize, startInclusive, endBoundSize, endInclusive);
        }

        private String makePreparedQueryString(EntityMapper<T> mapper) {
            switch (kind) {
                case SAVE:
                    Insert insert = insertInto(mapper.keyspace, mapper.table);
                    for (ColumnMapper<T> cm : mapper.allColumns())
                        insert.value(cm.getColumnName(), bindMarker());
                    return insert.toString();
                case GET:
                    Select select = select().all().from(mapper.keyspace, mapper.table);
                    Select.Where gWhere = select.where();
                    for (int i = 0; i < mapper.primaryKeySize(); i++)
                        gWhere.and(eq(mapper.getPrimaryKeyColumn(i).columnName, bindMarker()));
                    return select.toString();
                case DEL:
                    Delete delete = delete().all().from(mapper.keyspace, mapper.table);
                    Delete.Where dWhere = delete.where();
                    for (int i = 0; i < mapper.primaryKeySize(); i++)
                        dWhere.and(eq(mapper.getPrimaryKeyColumn(i).columnName, bindMarker()));
                    return delete.toString();
                case SLICE:
                case REVERSED_SLICE:
                    Select select = select().all().from(mapper.keyspace, mapper.table);
                    Select.Where sWhere = select.where();
                    for (int i = 0; i < mapper.partitionKeys.size(); i++)
                        sWhere.and(eq(mapper.partitionKeys.get(i).columnName, bindMarker()));

                    for (int i = 0; i < slicePrefix; i++)
                        sWhere.and(eq);

                    return select.toString();
            }
            throw new AssertionError();
        }
    }

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
