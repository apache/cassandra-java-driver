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
package com.datastax.driver.mapping;

import com.datastax.driver.core.ConsistencyLevel;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.datastax.driver.core.querybuilder.QueryBuilder.quote;

abstract class EntityMapper<T> {

    final Class<T> entityClass;
    private final String keyspace;
    private final String table;

    final ConsistencyLevel writeConsistency;
    final ConsistencyLevel readConsistency;

    final List<ColumnMapper<T>> partitionKeys = new ArrayList<ColumnMapper<T>>();
    final List<ColumnMapper<T>> clusteringColumns = new ArrayList<ColumnMapper<T>>();
    final List<ColumnMapper<T>> regularColumns = new ArrayList<ColumnMapper<T>>();

    private final List<ColumnMapper<T>> allColumns = new ArrayList<ColumnMapper<T>>();

    protected EntityMapper(Class<T> entityClass, String keyspace, String table, ConsistencyLevel writeConsistency, ConsistencyLevel readConsistency) {
        this.entityClass = entityClass;
        this.keyspace = keyspace;
        this.table = table;
        this.writeConsistency = writeConsistency;
        this.readConsistency = readConsistency;
    }

    String getKeyspace() {
        return quote(keyspace);
    }

    String getTable() {
        return quote(table);
    }

    int primaryKeySize() {
        return partitionKeys.size() + clusteringColumns.size();
    }

    ColumnMapper<T> getPrimaryKeyColumn(int i) {
        return i < partitionKeys.size() ? partitionKeys.get(i) : clusteringColumns.get(i - partitionKeys.size());
    }

    void addColumns(List<ColumnMapper<T>> pks, List<ColumnMapper<T>> ccs, List<ColumnMapper<T>> rgs) {
        partitionKeys.addAll(pks);
        allColumns.addAll(pks);

        clusteringColumns.addAll(ccs);
        allColumns.addAll(ccs);

        addColumns(rgs);
    }

    void addColumns(List<ColumnMapper<T>> rgs) {
        regularColumns.addAll(rgs);
        allColumns.addAll(rgs);
    }

    abstract T newEntity();

    List<ColumnMapper<T>> allColumns() {
        return allColumns;
    }

    interface Factory {
        public <T> EntityMapper<T> create(Class<T> entityClass, String keyspace, String table, ConsistencyLevel writeConsistency, ConsistencyLevel readConsistency);

        public <T> ColumnMapper<T> createColumnMapper(Class<T> componentClass, Field field, int position, MappingManager mappingManager, AtomicInteger columnCounter);
    }
}
