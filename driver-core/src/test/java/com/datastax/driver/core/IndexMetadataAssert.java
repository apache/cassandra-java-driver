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

import static org.assertj.core.api.Assertions.assertThat;

import org.assertj.core.api.AbstractAssert;

import com.datastax.driver.core.ColumnMetadata.IndexMetadata;

public class IndexMetadataAssert extends AbstractAssert<IndexMetadataAssert, IndexMetadata> {

    public IndexMetadataAssert(IndexMetadata actual) {
        super(actual, IndexMetadataAssert.class);
    }

    public IndexMetadataAssert hasName(String name) {
        assertThat(actual.getName()).isEqualTo(name);
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
    
    public IndexMetadataAssert isKeys(){
        assertThat(actual.isKeys()).isTrue();
        return this;
    }
    
    public IndexMetadataAssert isNotKeys(){
        assertThat(actual.isKeys()).isFalse();
        return this;
    }
    
    public IndexMetadataAssert isFull() {
        assertThat(actual.isFull()).isTrue();
        return this;
    }
    
    public IndexMetadataAssert isNotFull() {
        assertThat(actual.isFull()).isFalse();
        return this;
    }
    
    public IndexMetadataAssert isEntries() {
        assertThat(actual.isEntries()).isTrue();
        return this;
    }
    
    public IndexMetadataAssert isNotEntries() {
        assertThat(actual.isEntries()).isFalse();
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
    
}
