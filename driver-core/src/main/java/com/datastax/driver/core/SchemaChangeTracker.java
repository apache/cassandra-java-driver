/*
 *      Copyright (C) 2012 DataStax Inc.
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
 * <p>
 * An implementation of this interface can be registered against a Cluster
 * object through the {@link com.datastax.driver.core.Cluster#register} method.
 */
public interface SchemaChangeTracker {

    /**
     * Called when a keyspace has been created.
     *
     * @param keyspace the name of the keyspace that has been newly added.
     */
    public void onKeyspaceCreated(String keyspace);

    /**
     * Called when a keyspace has been dropped.
     *
     * @param keyspace the name of the keyspace that has been dropped.
     */
    public void onKeyspaceDropped(String keyspace);

    /**
     * Called when a keyspace has been updated.
     *
     * @param keyspace the name of the keyspace that has been updated.
     */
    public void onKeyspaceUpdated(String keyspace);

    /**
     * Called when a table has been created.
     *
     * @param keyspace the name of the table's keyspace.
     * @param table the name of the table that has been newly added.
     */
    public void onTableOrTypeCreated(String keyspace, String table);

    /**
     * Called when a table has been dropped.
     *
     * @param keyspace the name of the table's keyspace.
     * @param table the name of the table that has been dropped.
     */
    public void onTableOrTypeDropped(String keyspace, String table);

    /**
     * Called when a table has been updated.
     *
     * @param keyspace the name of the table's keyspace.
     * @param table the name of the table that has been updated.
     */
    public void onTableOrTypeUpdated(String keyspace, String table);
}
