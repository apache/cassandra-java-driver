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
package com.datastax.driver.mapping;

import com.datastax.driver.core.CCMConfig;
import com.datastax.driver.core.CCMTestsSupport;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.CassandraVersion;
import com.datastax.driver.mapping.annotations.*;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

@CassandraVersion("2.1.0")
@CCMConfig(createCluster = false)
@SuppressWarnings("unused")
public class UDTFieldMapperTest extends CCMTestsSupport {

    @Test(groups = "short")
    public void udt_and_tables_with_ks_created_in_another_session_should_be_mapped() {
        Cluster cluster1 = register(Cluster.builder()
                .addContactPoints(getContactPoints().get(0))
                .withPort(ccm().getBinaryPort())
                .build());
        Session session1 = cluster1.connect();
        // Create type and table
        session1.execute("create schema if not exists java_509 " +
                "with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        session1.execute("create type java_509.my_tuple (" +
                "type text, " +
                "value text);");
        session1.execute("create table java_509.my_hash (" +
                "key int primary key, " +
                "properties map<text, frozen<java_509.my_tuple>>);");
        cluster1.close();

        // Create entities with another connection
        Cluster cluster2 = register(Cluster.builder()
                .addContactPoints(getContactPoints().get(0))
                .withPort(ccm().getBinaryPort())
                .build());
        Session session2 = cluster2.newSession();
        Mapper<MyHashWithKeyspace> hashMapper = new MappingManager(session2).mapper(MyHashWithKeyspace.class);
        hashMapper.save(new MyHashWithKeyspace(
                1,
                ImmutableMap.of("key-1", new MyTupleWithKeyspace("first-half-1", "second-half-1"))));
        hashMapper.save(new MyHashWithKeyspace(
                2,
                ImmutableMap.of("key-2", new MyTupleWithKeyspace("first-half-2", null))));
        hashMapper.save(new MyHashWithKeyspace(
                3,
                ImmutableMap.of("key-3", new MyTupleWithKeyspace(null, "second-half-3"))));
        hashMapper.save(new MyHashWithKeyspace(
                4,
                new HashMap<String, MyTupleWithKeyspace>()));
        hashMapper.save(new MyHashWithKeyspace(
                5,
                null));
    }

    @Test(groups = "short")
    public void udt_and_tables_without_ks_created_in_another_session_should_be_mapped() {
        Cluster cluster1 = register(Cluster.builder()
                .addContactPoints(getContactPoints().get(0))
                .withPort(ccm().getBinaryPort())
                .build());
        Session session1 = cluster1.connect();
        session1.execute("create schema if not exists java_509b " +
                "with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        session1.execute("use java_509b");
        session1.execute("create type my_tuple (" +
                "type text, " +
                "value text);");
        session1.execute("create table my_hash (" +
                "key int primary key, " +
                "properties map<text, frozen<my_tuple>>);");
        cluster1.close();

        // Create entities with another connection
        Cluster cluster2 = register(Cluster.builder()
                .addContactPoints(getContactPoints().get(0))
                .withPort(ccm().getBinaryPort())
                .build());
        Session session2 = cluster2.newSession();

        session2.execute("use java_509b");
        Mapper<MyHash> hashMapper = new MappingManager(session2).mapper(MyHash.class);
        hashMapper.save(new MyHash(
                1,
                ImmutableMap.of("key-1", new MyTuple("first-half-1", "second-half-1"))));
        hashMapper.save(new MyHash(
                2,
                ImmutableMap.of("key-2", new MyTuple("first-half-2", null))));
        hashMapper.save(new MyHash(
                3,
                ImmutableMap.of("key-3", new MyTuple(null, "second-half-3"))));
        hashMapper.save(new MyHash(
                4,
                new HashMap<String, MyTuple>()));
        hashMapper.save(new MyHash(
                5,
                null));
    }

    @UDT(name = "my_tuple")
    static class MyTuple {
        @Field(name = "type")
        private String type;
        @Field(name = "value")
        private String value;

        public MyTuple() {
        }

        public MyTuple(String type, String value) {
            this.type = type;
            this.value = value;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }

    @Table(name = "my_hash")
    static class MyHash {
        @PartitionKey
        @Column(name = "key")
        private int key;

        @FrozenValue
        @Column(name = "properties")
        private Map<String, MyTuple> properties;

        public MyHash() {

        }

        public MyHash(int key, Map<String, MyTuple> properties) {

            this.key = key;
            this.properties = properties;
        }

        public int getKey() {
            return key;
        }

        public void setKey(int key) {
            this.key = key;
        }

        public Map<String, MyTuple> getProperties() {
            return properties;
        }

        public void setProperties(Map<String, MyTuple> properties) {
            this.properties = properties;
        }
    }

    @UDT(name = "my_tuple", keyspace = "java_509")
    static class MyTupleWithKeyspace {
        @Field(name = "type")
        private String type;
        @Field(name = "value")
        private String value;

        public MyTupleWithKeyspace() {
        }

        public MyTupleWithKeyspace(String type, String value) {
            this.type = type;
            this.value = value;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }

    @Table(name = "my_hash", keyspace = "java_509")
    static class MyHashWithKeyspace {
        @PartitionKey
        @Column(name = "key")
        private int key;

        @FrozenValue
        @Column(name = "properties")
        private Map<String, MyTupleWithKeyspace> properties;

        public MyHashWithKeyspace() {
        }

        public MyHashWithKeyspace(int key, Map<String, MyTupleWithKeyspace> properties) {
            this.key = key;
            this.properties = properties;
        }

        public int getKey() {
            return key;
        }

        public void setKey(int key) {
            this.key = key;
        }

        public Map<String, MyTupleWithKeyspace> getProperties() {
            return properties;
        }

        public void setProperties(Map<String, MyTupleWithKeyspace> properties) {
            this.properties = properties;
        }
    }
}
