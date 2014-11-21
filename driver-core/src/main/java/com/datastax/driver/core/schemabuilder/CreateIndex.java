/*
 *      Copyright (C) 2012-2014 DataStax Inc.
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

public class CreateIndex extends SchemaStatement {

    private String indexName;
    private boolean ifNotExists = false;
    private Optional<String> keyspaceName = Optional.absent();
    private String tableName;
    private String columnName;


    CreateIndex(String indexName) {
        validateNotEmpty(indexName, "Index name");
        validateNotKeyWord(indexName, String.format("The index name '%s' is not allowed because it is a reserved keyword", indexName));
        this.indexName = indexName;
    }

    /**
     * Use 'IF NOT EXISTS' CAS condition for the index creation.
     *
     * @return this CREATE INDEX statement.
     */
    public CreateIndex ifNotExists() {
        this.ifNotExists = true;
        return this;
    }

    /**
     * Create index on the given keyspace and table
     * @param keyspaceName
     * @param tableName
     * @return this CREATE INDEX statement
     */
    public CreateIndexOn onTable(String keyspaceName, String tableName) {
        validateNotEmpty(keyspaceName, "Keyspace name");
        validateNotEmpty(tableName, "Table name");
        validateNotKeyWord(keyspaceName, String.format("The keyspace name '%s' is not allowed because it is a reserved keyword", keyspaceName));
        validateNotKeyWord(tableName, String.format("The table name '%s' is not allowed because it is a reserved keyword", tableName));
        this.keyspaceName = Optional.fromNullable(keyspaceName);
        ;
        this.tableName = tableName;
        return new CreateIndexOn();
    }

    /**
     * Create index on the given table
     * @param tableName
     * @return this CREATE INDEX statement
     */
    public CreateIndexOn onTable(String tableName) {
        validateNotEmpty(tableName, "Table name");
        validateNotKeyWord(tableName, String.format("The table name '%s' is not allowed because it is a reserved keyword", tableName));
        this.tableName = tableName;
        return new CreateIndexOn();
    }

    public class CreateIndexOn {
        /**
         * Create index on the given column
         * @param columnName
         * @return the final CREATE INDEX statement
         */
        public String andColumn(String columnName) {
            validateNotEmpty(tableName, "Column name");
            validateNotKeyWord(columnName, String.format("The column name '%s' is not allowed because it is a reserved keyword", columnName));
            CreateIndex.this.columnName = columnName;
            return buildInternal();
        }
    }


    @Override
    String buildInternal() {
        StringBuilder createStatement = new StringBuilder(NEW_LINE).append(TAB).append(CREATE_INDEX).append(SPACE);

        if (ifNotExists) {
            createStatement.append(IF_NOT_EXISTS).append(SPACE);
        }

        createStatement.append(indexName).append(SPACE).append(ON).append(SPACE);

        if (keyspaceName.isPresent()) {
            createStatement.append(keyspaceName.get()).append(DOT);
        }
        createStatement.append(tableName);

        return createStatement.append(OPEN_PAREN).append(columnName).append(CLOSE_PAREN).toString();
    }
}
