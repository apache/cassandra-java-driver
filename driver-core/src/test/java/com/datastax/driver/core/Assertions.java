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

/**
 * Augment AssertJ with custom assertions for the Java driver.
 */
public class Assertions extends org.assertj.core.api.Assertions {
    public static ClusterAssert assertThat(Cluster cluster) {
        return new ClusterAssert(cluster);
    }

    public static HostAssert assertThat(Host host) {
        return new HostAssert(host);
    }

    public static SessionAssert assertThat(Session session) {
        return new SessionAssert(session);
    }

    public static TokenRangeAssert assertThat(TokenRange range) {
        return new TokenRangeAssert(range);
    }

    public static DataTypeAssert assertThat(DataType type) {
        return new DataTypeAssert(type);
    }

    /**
     * This method is here only to disambiguate
     * calls to assertThat with a UserType instance,
     * because UserType also implements Iterable.
     */
    public static DataTypeAssert assertThat(UserType type) {
        return new DataTypeAssert(type);
    }

    public static LocalDateAssert assertThat(LocalDate localDate) {
        return new LocalDateAssert(localDate);
    }

    public static KeyspaceMetadataAssert assertThat(KeyspaceMetadata metadata) {
        return new KeyspaceMetadataAssert(metadata);
    }

    public static TableMetadataAssert assertThat(TableMetadata table) {
        return new TableMetadataAssert(table);
    }

    public static ColumnMetadataAssert assertThat(ColumnMetadata column) {
        return new ColumnMetadataAssert(column);
    }

    public static FunctionMetadataAssert assertThat(FunctionMetadata function) {
        return new FunctionMetadataAssert(function);
    }

    public static AggregateMetadataAssert assertThat(AggregateMetadata aggregate) {
        return new AggregateMetadataAssert(aggregate);
    }

    public static IndexMetadataAssert assertThat(IndexMetadata index) {
        return new IndexMetadataAssert(index);
    }

    public static <T> TypeCodecAssert<T> assertThat(TypeCodec<T> codec) {
        return new TypeCodecAssert<T>(codec);
    }

    public static MaterializedViewMetadataAssert assertThat(MaterializedViewMetadata view) {
        return new MaterializedViewMetadataAssert(view);
    }

    public static VersionNumberAssert assertThat(VersionNumber actual) {
        return new VersionNumberAssert(actual);
    }
    public static ResultSetAssert assertThat(ResultSet rows) {
        return new ResultSetAssert(rows);
    }

}
