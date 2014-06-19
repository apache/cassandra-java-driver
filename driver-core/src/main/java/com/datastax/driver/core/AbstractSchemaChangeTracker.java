/*
 *      Copyright (C) 2014 Robert Stupp, Koeln, Germany, robert-stupp.de
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
 * Contains all noop methods - use as a base class for your implementations.
 */
public abstract class AbstractSchemaChangeTracker implements SchemaChangeTracker {
    @Override public void onKeyspaceCreated(String keyspace) {

    }

    @Override public void onKeyspaceDropped(String keyspace) {

    }

    @Override public void onKeyspaceUpdated(String keyspace) {

    }

    @Override public void onTableCreated(String keyspace, String table) {

    }

    @Override public void onTableDropped(String keyspace, String table) {

    }

    @Override public void onTableUpdated(String keyspace, String table) {

    }

    @Override public void onTypeCreated(String keyspace, String type) {

    }

    @Override public void onTypeDropped(String keyspace, String type) {

    }

    @Override public void onTypeUpdated(String keyspace, String type) {

    }
}
