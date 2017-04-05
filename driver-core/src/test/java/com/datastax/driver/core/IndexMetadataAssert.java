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

import com.datastax.driver.core.IndexMetadata.Kind;
import org.assertj.core.api.AbstractAssert;

import static org.assertj.core.api.Assertions.assertThat;

public class IndexMetadataAssert extends AbstractAssert<IndexMetadataAssert, IndexMetadata> {

    public IndexMetadataAssert(IndexMetadata actual) {
        super(actual, IndexMetadataAssert.class);
    }

    public IndexMetadataAssert hasName(String name) {
        assertThat(actual.getName()).isEqualTo(name);
        return this;
    }

    public IndexMetadataAssert hasParent(TableMetadata parent) {
        assertThat(actual.getTable()).isEqualTo(parent);
        return this;
    }

    public IndexMetadataAssert hasOption(String name, String value) {
        assertThat(actual.getOption(name)).isEqualTo(value);
        return this;
    }

    public IndexMetadataAssert asCqlQuery(String cqlQuery) {
        assertThat(actual.asCQLQuery()).isEqualTo(cqlQuery);
        return this;
    }

    public IndexMetadataAssert isCustomIndex() {
        assertThat(actual.isCustomIndex()).isTrue();
        return this;
    }

    public IndexMetadataAssert isNotCustomIndex() {
        assertThat(actual.isCustomIndex()).isFalse();
        return this;
    }

    public IndexMetadataAssert hasTarget(String target) {
        assertThat(actual.getTarget()).isEqualTo(target);
        return this;
    }

    public IndexMetadataAssert hasKind(Kind kind) {
        assertThat(actual.getKind()).isEqualTo(kind);
        return this;
    }
}
