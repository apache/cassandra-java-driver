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
 * A built DROP KEYSPACE statement.
 */
public class DropKeyspace extends SchemaStatement {

    private final String keyspaceName;
    private boolean ifExists;

    public DropKeyspace(String keyspaceName) {
        this.keyspaceName = keyspaceName;
        this.ifExists = false;
        validateNotEmpty(keyspaceName, "Keyspace name");
        validateNotKeyWord(keyspaceName,
                String.format("The keyspace name '%s' is not allowed because it is a reserved keyword", keyspaceName));
    }

    /**
     * Add the 'IF EXISTS' condition to this DROP statement.
     *
     * @return this statement.
     */
    public DropKeyspace ifExists() {
        this.ifExists = true;
        return this;
    }

    @Override
    public String buildInternal() {
        StringBuilder dropStatement = new StringBuilder("DROP KEYSPACE ");
        if (ifExists) {
            dropStatement.append("IF EXISTS ");
        }
        dropStatement.append(keyspaceName);
        return dropStatement.toString();
    }

}
