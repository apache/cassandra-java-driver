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

import org.assertj.core.api.AbstractAssert;

import static org.assertj.core.api.Assertions.assertThat;

public class TableMetadataAssert extends AbstractAssert<TableMetadataAssert, TableMetadata> {
    protected TableMetadataAssert(TableMetadata actual) {
        super(actual, TableMetadataAssert.class);
    }

    public TableMetadataAssert hasName(String name) {
        assertThat(actual.getName()).isEqualTo(name);
        return this;
    }

    public TableMetadataAssert isInKeyspace(String keyspaceName) {
        assertThat(actual.getKeyspace().getName()).isEqualTo(keyspaceName);
        return this;
    }

    public TableMetadataAssert hasColumn(String columnName) {
        assertThat(actual.getColumn(columnName)).isNotNull();
        return this;
    }

    public TableMetadataAssert hasColumn(String columnName, DataType dataType) {
        ColumnMetadata column = actual.getColumn(columnName);
        assertThat(column).isNotNull();
        assertThat(column.getType()).isEqualTo(dataType);
        return this;
    }

    public TableMetadataAssert hasNoColumn(String columnName) {
        assertThat(actual.getColumn(columnName)).isNull();
        return this;
    }

    public TableMetadataAssert hasComment(String comment) {
        assertThat(actual.getOptions().getComment()).isEqualTo(comment);
        return this;
    }

    public TableMetadataAssert doesNotHaveComment(String comment) {
        assertThat(actual.getOptions().getComment()).isNotEqualTo(comment);
        return this;
    }

    public TableMetadataAssert doesNotHaveColumn(String columnName) {
        ColumnMetadata column = actual.getColumn(columnName);
        assertThat(column).isNull();
        return this;
    }

    public TableMetadataAssert isCompactStorage() {
        assertThat(actual.getOptions().isCompactStorage()).isTrue();
        return this;
    }

    public TableMetadataAssert isNotCompactStorage() {
        assertThat(actual.getOptions().isCompactStorage()).isFalse();
        return this;
    }

    public TableMetadataAssert hasNumberOfColumns(int expected) {
        assertThat(actual.getColumns().size()).isEqualTo(expected);
        return this;
    }

    public TableMetadataAssert hasMaterializedView(MaterializedViewMetadata expected) {
        assertThat(actual.getView(Metadata.quote(expected.getName()))).isNotNull().isEqualTo(expected);
        return this;
    }

    public TableMetadataAssert hasIndex(IndexMetadata index) {
        assertThat(actual.getIndexes()).contains(index);
        return this;
    }
}
