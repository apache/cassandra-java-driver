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

import org.assertj.core.api.AbstractAssert;

import static org.assertj.core.api.Assertions.assertThat;

public class ColumnMetadataAssert extends AbstractAssert<ColumnMetadataAssert, ColumnMetadata> {

    protected ColumnMetadataAssert(ColumnMetadata actual) {
        super(actual, ColumnMetadataAssert.class);
    }

    public ColumnMetadataAssert hasType(DataType dataType) {
        assertThat(actual.getType()).isEqualTo(dataType);
        return this;
    }

    public ColumnMetadataAssert hasName(String name) {
        assertThat(actual.getName()).isEqualTo(name);
        return this;
    }

    public ColumnMetadataAssert isPrimaryKey() {
        assertThat(actual.getTable().getPrimaryKey().contains(actual));
        return this;
    }

    public ColumnMetadataAssert isPartitionKey() {
        assertThat(actual.getTable().getPartitionKey().contains(actual));
        return this;
    }

    public ColumnMetadataAssert isClusteringColumn() {
        assertThat(actual.getTable().getClusteringColumns().contains(actual));
        return this;
    }

    public ColumnMetadataAssert isRegularColumn() {
        assertThat(!actual.getTable().getPrimaryKey().contains(actual));
        return this;
    }

    public ColumnMetadataAssert hasClusteringOrder(TableMetadata.Order order) {
        assertThat(actual.getTable().getClusteringOrder().get(actual.getTable().getClusteringColumns().indexOf(actual))).isEqualTo(order);
        return this;
    }

    public ColumnMetadataAssert isStatic() {
        assertThat(actual.isStatic()).isTrue();
        return this;
    }

    public ColumnMetadataAssert isNotStatic() {
        assertThat(actual.isStatic()).isFalse();
        return this;
    }

    public ColumnMetadataAssert hasIndex(IndexMetadata index) {
        assertThat(actual.getIndex(index.getName())).isEqualTo(index);
        return this;
    }

    public ColumnMetadataAssert hasSingleIndex(IndexMetadata index) {
         assertThat(actual.getIndexes()).containsOnly(index);
        return this;
    }

}
