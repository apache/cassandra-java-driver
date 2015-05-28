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
