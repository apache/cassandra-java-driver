package com.datastax.driver.mapping;

import java.lang.reflect.Field;
import java.util.*;

abstract class EntityMapper<T> {

    public final Class<T> entityClass;
    public final String keyspace;
    public final String table;

    public final List<ColumnMapper<T>> partitionKeys = new ArrayList<ColumnMapper<T>>();
    public final List<ColumnMapper<T>> clusteringColumns = new ArrayList<ColumnMapper<T>>();
    public final List<ColumnMapper<T>> regularColumns = new ArrayList<ColumnMapper<T>>();

    private final List<ColumnMapper<T>> allColumns = new ArrayList<ColumnMapper<T>>();

    protected EntityMapper(Class<T> entityClass, String keyspace, String table) {

        this.entityClass = entityClass;
        this.keyspace = keyspace;
        this.table = table;
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

    interface Factory {
        public <T> EntityMapper<T> create(Class<T> entityClass, String keyspace, String table);
        public <T> ColumnMapper<T> createColumnMapper(Class<T> entityClass, Field field, int position);
    }
}
