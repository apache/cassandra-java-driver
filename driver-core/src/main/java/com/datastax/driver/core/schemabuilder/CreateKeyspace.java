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
package com.datastax.driver.core.schemabuilder;

/**
 * A built CREATE KEYSPACE statement.
 */
public class CreateKeyspace {

    static final String command = "CREATE KEYSPACE";

    private final String keyspaceName;
    private boolean ifNotExists;

    public CreateKeyspace(String keyspaceName) {
        this.keyspaceName = keyspaceName;
        this.ifNotExists = false;
    }

    public CreateKeyspace ifNotExists() {
        this.ifNotExists = true;
        return this;
    }

    /**
     * Add options for this CREATE KEYSPACE statement.
     *
     * @return the options of this CREATE KEYSPACE statement.
     */
    public KeyspaceOptions with() {
        return new KeyspaceOptions(buildCommand(), keyspaceName);
    }

    String buildCommand() {
        StringBuilder createStatement = new StringBuilder();
        createStatement.append(command);
        if (ifNotExists) {
            createStatement.append(" IF NOT EXISTS");
        }
        return createStatement.toString();
    }

}
