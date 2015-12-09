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
package com.datastax.driver.core.schemabuilder;

import com.google.common.base.Optional;

import static com.datastax.driver.core.schemabuilder.SchemaStatement.*;

/**
 * A built CREATE INDEX statement.
 */
public class CreateIndex implements StatementStart {

    private String indexName;
    private boolean ifNotExists = false;
    private Optional<String> keyspaceName = Optional.absent();
    private String tableName;
    private String columnName;
    private boolean keys;

    CreateIndex(String indexName) {
        validateNotEmpty(indexName, "Index name");
        validateNotKeyWord(indexName, String.format("The index name '%s' is not allowed because it is a reserved keyword", indexName));
        this.indexName = indexName;
    }

    /**
     * Add the 'IF NOT EXISTS' condition to this CREATE INDEX statement.
     *
     * @return this CREATE INDEX statement.
     */
    public CreateIndex ifNotExists() {
        this.ifNotExists = true;
        return this;
    }

    /**
     * Specify the keyspace and table to create the index on.
     *
     * @param keyspaceName the keyspace name.
     * @param tableName    the table name.
     * @return a {@link CreateIndexOn} that will allow the specification of the column.
     */
    public CreateIndexOn onTable(String keyspaceName, String tableName) {
        validateNotEmpty(keyspaceName, "Keyspace name");
        validateNotEmpty(tableName, "Table name");
        validateNotKeyWord(keyspaceName, String.format("The keyspace name '%s' is not allowed because it is a reserved keyword", keyspaceName));
        validateNotKeyWord(tableName, String.format("The table name '%s' is not allowed because it is a reserved keyword", tableName));
        this.keyspaceName = Optional.fromNullable(keyspaceName);
        this.tableName = tableName;
        return new CreateIndexOn();
    }

    /**
     * Specify the table to create the index on.
     *
     * @param tableName the table name.
     * @return a {@link CreateIndexOn} that will allow the specification of the column.
     */
    public CreateIndexOn onTable(String tableName) {
        validateNotEmpty(tableName, "Table name");
        validateNotKeyWord(tableName, String.format("The table name '%s' is not allowed because it is a reserved keyword", tableName));
        this.tableName = tableName;
        return new CreateIndexOn();
    }

    public class CreateIndexOn {
        /**
         * Specify the column to create the index on.
         *
         * @param columnName the column name.
         * @return the final CREATE INDEX statement.
         */
        public SchemaStatement andColumn(String columnName) {
            validateNotEmpty(columnName, "Column name");
            validateNotKeyWord(columnName, String.format("The column name '%s' is not allowed because it is a reserved keyword", columnName));
            CreateIndex.this.columnName = columnName;
            return SchemaStatement.fromQueryString(buildInternal());
        }

        /**
         * Create an index on the keys of the given map column.
         *
         * @param columnName the column name.
         * @return the final CREATE INDEX statement.
         */
        public SchemaStatement andKeysOfColumn(String columnName) {
            validateNotEmpty(columnName, "Column name");
            validateNotKeyWord(columnName, String.format("The column name '%s' is not allowed because it is a reserved keyword", columnName));
            CreateIndex.this.columnName = columnName;
            CreateIndex.this.keys = true;
            return SchemaStatement.fromQueryString(buildInternal());
        }
    }

    @Override
    public String buildInternal() {
        StringBuilder createStatement = new StringBuilder(STATEMENT_START).append("CREATE INDEX ");

        if (ifNotExists) {
            createStatement.append("IF NOT EXISTS ");
        }

        createStatement.append(indexName).append(" ON ");

        if (keyspaceName.isPresent()) {
            createStatement.append(keyspaceName.get()).append(".");
        }
        createStatement.append(tableName);

        createStatement.append("(");
        if (keys) {
            createStatement.append("KEYS(");
        }

        createStatement.append(columnName);

        if (keys) {
            createStatement.append(")");
        }
        createStatement.append(")");

        return createStatement.toString();
    }
}
