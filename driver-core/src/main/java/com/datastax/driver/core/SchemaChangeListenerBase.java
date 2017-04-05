/*
 * Copyright (C) 2012-2017 DataStax Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.driver.core;

/**
 * Base implementation for {@link SchemaChangeListener}.
 */
public abstract class SchemaChangeListenerBase implements SchemaChangeListener {

    @Override
    public void onKeyspaceAdded(KeyspaceMetadata keyspace) {

    }

    @Override
    public void onKeyspaceRemoved(KeyspaceMetadata keyspace) {

    }

    @Override
    public void onKeyspaceChanged(KeyspaceMetadata current, KeyspaceMetadata previous) {

    }

    @Override
    public void onTableAdded(TableMetadata table) {

    }

    @Override
    public void onTableRemoved(TableMetadata table) {

    }

    @Override
    public void onTableChanged(TableMetadata current, TableMetadata previous) {

    }

    @Override
    public void onUserTypeAdded(UserType type) {

    }

    @Override
    public void onUserTypeRemoved(UserType type) {

    }

    @Override
    public void onUserTypeChanged(UserType current, UserType previous) {

    }

    @Override
    public void onFunctionAdded(FunctionMetadata function) {

    }

    @Override
    public void onFunctionRemoved(FunctionMetadata function) {

    }

    @Override
    public void onFunctionChanged(FunctionMetadata current, FunctionMetadata previous) {

    }

    @Override
    public void onAggregateAdded(AggregateMetadata aggregate) {

    }

    @Override
    public void onAggregateRemoved(AggregateMetadata aggregate) {

    }

    @Override
    public void onAggregateChanged(AggregateMetadata current, AggregateMetadata previous) {

    }

    @Override
    public void onMaterializedViewAdded(MaterializedViewMetadata view) {

    }

    @Override
    public void onMaterializedViewRemoved(MaterializedViewMetadata view) {

    }

    @Override
    public void onMaterializedViewChanged(MaterializedViewMetadata current, MaterializedViewMetadata previous) {

    }

    @Override
    public void onRegister(Cluster cluster) {

    }

    @Override
    public void onUnregister(Cluster cluster) {

    }
}
