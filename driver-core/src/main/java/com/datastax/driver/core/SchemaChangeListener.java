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
package com.datastax.driver.core;

/**
 * Interface for objects that are interested in tracking schema change events in the cluster.
 * <p/>
 * An implementation of this interface can be registered against a Cluster
 * object through the {@link com.datastax.driver.core.Cluster#register(SchemaChangeListener)} method.
 * <p/>
 * Note that the methods defined by this interface will be executed by internal driver threads, and are
 * therefore expected to have short execution times. If you need to perform long computations or blocking
 * calls in response to schema change events, it is strongly recommended to schedule them asynchronously
 * on a separate thread provided by our application code.
 */
public interface SchemaChangeListener {

    /**
     * Called when a keyspace has been added.
     *
     * @param keyspace the keyspace that has been added.
     */
    void onKeyspaceAdded(KeyspaceMetadata keyspace);

    /**
     * Called when a keyspace has been removed.
     *
     * @param keyspace the keyspace that has been removed.
     */
    void onKeyspaceRemoved(KeyspaceMetadata keyspace);

    /**
     * Called when a keyspace has changed.
     *
     * @param current  the keyspace that has changed, in its current form (after the change).
     * @param previous the keyspace that has changed, in its previous form (before the change).
     */
    void onKeyspaceChanged(KeyspaceMetadata current, KeyspaceMetadata previous);

    /**
     * Called when a table has been added.
     *
     * @param table the table that has been newly added.
     */
    void onTableAdded(TableMetadata table);

    /**
     * Called when a table has been removed.
     *
     * @param table the table that has been removed.
     */
    void onTableRemoved(TableMetadata table);

    /**
     * Called when a table has changed.
     *
     * @param current  the table that has changed, in its current form (after the change).
     * @param previous the table that has changed, in its previous form (before the change).
     */
    void onTableChanged(TableMetadata current, TableMetadata previous);

    /**
     * Called when a user-defined type has been added.
     *
     * @param type the type that has been newly added.
     */
    void onUserTypeAdded(UserType type);

    /**
     * Called when a user-defined type has been removed.
     *
     * @param type the type that has been removed.
     */
    void onUserTypeRemoved(UserType type);

    /**
     * Called when a user-defined type has changed.
     *
     * @param current  the type that has changed, in its current form (after the change).
     * @param previous the type that has changed, in its previous form (before the change).
     */
    void onUserTypeChanged(UserType current, UserType previous);

    /**
     * Called when a user-defined function has been added.
     *
     * @param function the function that has been newly added.
     */
    void onFunctionAdded(FunctionMetadata function);

    /**
     * Called when a user-defined function has been removed.
     *
     * @param function the function that has been removed.
     */
    void onFunctionRemoved(FunctionMetadata function);

    /**
     * Called when a user-defined function has changed.
     *
     * @param current  the function that has changed, in its current form (after the change).
     * @param previous the function that has changed, in its previous form (before the change).
     */
    void onFunctionChanged(FunctionMetadata current, FunctionMetadata previous);

    /**
     * Called when a user-defined aggregate has been added.
     *
     * @param aggregate the aggregate that has been newly added.
     */
    void onAggregateAdded(AggregateMetadata aggregate);

    /**
     * Called when a user-defined aggregate has been removed.
     *
     * @param aggregate the aggregate that has been removed.
     */
    void onAggregateRemoved(AggregateMetadata aggregate);

    /**
     * Called when a user-defined aggregate has changed.
     *
     * @param current  the aggregate that has changed, in its current form (after the change).
     * @param previous the aggregate that has changed, in its previous form (before the change).
     */
    void onAggregateChanged(AggregateMetadata current, AggregateMetadata previous);

    /**
     * Called when a materialized view has been added.
     *
     * @param view the materialized view that has been newly added.
     */
    void onMaterializedViewAdded(MaterializedViewMetadata view);

    /**
     * Called when a materialized view has been removed.
     *
     * @param view the materialized view that has been removed.
     */
    void onMaterializedViewRemoved(MaterializedViewMetadata view);

    /**
     * Called when a materialized view has changed.
     *
     * @param current  the materialized view that has changed, in its current form (after the change).
     * @param previous the materialized view that has changed, in its previous form (before the change).
     */
    void onMaterializedViewChanged(MaterializedViewMetadata current, MaterializedViewMetadata previous);

    /**
     * Gets invoked when the listener is registered with a cluster.
     *
     * @param cluster the cluster that this tracker is registered with.
     */
    void onRegister(Cluster cluster);

    /**
     * Gets invoked when the listener is unregistered from a cluster, or at cluster shutdown if
     * the tracker was not unregistered.
     *
     * @param cluster the cluster that this tracker was registered with.
     */
    void onUnregister(Cluster cluster);
}
