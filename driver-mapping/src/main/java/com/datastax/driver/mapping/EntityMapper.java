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

import java.util.ArrayList;
import java.util.List;

import static com.datastax.driver.core.querybuilder.QueryBuilder.quote;

class EntityMapper<T> {

    private final Class<T> entityClass;
    private final String keyspace;
    private final String table;

    final ConsistencyLevel writeConsistency;
    final ConsistencyLevel readConsistency;

    final List<PropertyMapper> partitionKeys = new ArrayList<PropertyMapper>();
    final List<PropertyMapper> clusteringColumns = new ArrayList<PropertyMapper>();

    final List<PropertyMapper> allColumns = new ArrayList<PropertyMapper>();

    EntityMapper(Class<T> entityClass, String keyspace, String table, ConsistencyLevel writeConsistency, ConsistencyLevel readConsistency) {
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

    PropertyMapper getPrimaryKeyColumn(int i) {
        return i < partitionKeys.size() ? partitionKeys.get(i) : clusteringColumns.get(i - partitionKeys.size());
    }

    void addColumns(List<PropertyMapper> pks, List<PropertyMapper> ccs, List<PropertyMapper> rgs) {
        partitionKeys.addAll(pks);
        clusteringColumns.addAll(ccs);
        allColumns.addAll(pks);
        allColumns.addAll(ccs);
        allColumns.addAll(rgs);
    }

    T newEntity() {
        return ReflectionUtils.newInstance(entityClass);
    }

}
