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
package com.datastax.driver.core.exceptions;

/**
 * Exception thrown when a query attempts to create a keyspace or table that already exists.
 */
public class AlreadyExistsException extends QueryValidationException {

    private final String keyspace;
    private final String table;

    public AlreadyExistsException(String keyspace, String table) {
        super(makeMsg(keyspace, table));
        this.keyspace = keyspace;
        this.table = table;
    }

    private AlreadyExistsException(String msg, Throwable cause, String keyspace, String table) {
        super(msg, cause);
        this.keyspace = keyspace;
        this.table = table;
    }

    private static String makeMsg(String keyspace, String table) {
        if (table.isEmpty())
            return String.format("Keyspace %s already exists", keyspace);
        else
            return String.format("Table %s.%s already exists", keyspace, table);
    }

    /**
     * Returns whether the query yielding this exception was a table creation
     * attempt.
     *
     * @return {@code true} if this exception is raised following a table
     * creation attempt, {@code false} if it was a keyspace creation attempt.
     */
    public boolean wasTableCreation() {
        return !table.isEmpty();
    }

    /**
     * The name of keyspace that either already exists or is home to the table
     * that already exists.
     *
     * @return a keyspace name that is either the keyspace whose creation
     * attempt failed because a keyspace of the same name already exists (in
     * that case, {@link #table} will return {@code null}), or the keyspace of
     * the table creation attempt (in which case {@link #table} will return the
     * name of said table).
     */
    public String getKeyspace() {
        return keyspace;
    }

    /**
     * If the failed creation was a table creation, the name of the table that already exists.
     *
     * @return the name of table whose creation attempt failed because a table
     * of this name already exists, or {@code null} if the query was a keyspace
     * creation query.
     */
    public String getTable() {
        return table.isEmpty() ? null : table;
    }

    public DriverException copy() {
        return new AlreadyExistsException(getMessage(), this, keyspace, table);
    }
}
