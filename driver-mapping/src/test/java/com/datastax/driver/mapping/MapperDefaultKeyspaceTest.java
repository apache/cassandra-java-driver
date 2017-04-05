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

import com.datastax.driver.core.CCMTestsSupport;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.CassandraVersion;
import com.datastax.driver.core.utils.MoreObjects;
import com.datastax.driver.core.utils.UUIDs;
import com.datastax.driver.mapping.annotations.*;
import org.testng.annotations.Test;

import java.util.UUID;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

/**
 * Tests usage of mapping annotations without specifying a keyspace.
 */
@CassandraVersion("2.1.0")
@SuppressWarnings("unused")
public class MapperDefaultKeyspaceTest extends CCMTestsSupport {

    private static final String KEYSPACE = "mapper_default_keyspace_test_ks";

    @Override
    public void onTestContextInitialized() {
        execute(
                String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }", KEYSPACE),
                String.format("CREATE TYPE IF NOT EXISTS %s.group_name (name text)", KEYSPACE),
                String.format("CREATE TABLE IF NOT EXISTS %s.groups (group_id uuid PRIMARY KEY, name text)", KEYSPACE),
                String.format("CREATE TABLE IF NOT EXISTS %s.groups2 (group_id uuid PRIMARY KEY, name frozen<group_name>)", KEYSPACE));
    }

    @Test(groups = "short")
    public void testTableWithDefaultKeyspace() throws Exception {
        // Ensure that the test session is logged into the keyspace.
        session().execute("USE " + KEYSPACE);

        MappingManager manager = new MappingManager(session());
        Mapper<Group> m = manager.mapper(Group.class);
        Group group = new Group("testGroup");
        UUID groupId = group.getGroupId();

        // Check the save operation.
        m.save(group);

        // Check the select operation.
        Group selectedGroup = m.get(groupId);
        assertEquals(selectedGroup.getGroupId(), groupId);

        // Check the delete operation.
        m.delete(group);
        assertNull(m.get(groupId));
    }

    @Test(groups = "short")
    public void testUDTWithDefaultKeyspace() throws Exception {
        // Ensure that the test session is logged into the keyspace.
        session().execute("USE " + KEYSPACE);

        MappingManager manager = new MappingManager(session());
        Mapper<Group2> m = manager.mapper(Group2.class);
        Group2 group = new Group2(new GroupName("testGroup"));
        UUID groupId = group.getGroupId();

        // Check the save operation.
        m.save(group);

        // Check the select operation.
        Group2 selectedGroup = m.get(groupId);
        assertEquals(selectedGroup.getGroupId(), groupId);

        // Check the delete operation.
        m.delete(group);
        assertNull(m.get(groupId));
    }

    @Test(groups = "short",
            expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "Error creating mapper for class com.datastax.driver.mapping.MapperDefaultKeyspaceTest\\$Group, the @Table annotation declares no default keyspace, and the session is not currently logged to any keyspace")
    public void should_throw_a_meaningful_error_message_when_no_default_table_keyspace_and_session_not_logged() {
        Session session2 = cluster().connect();
        MappingManager manager = new MappingManager(session2);
        manager.mapper(Group.class);
    }

    @Test(groups = "short",
            expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "Error creating UDT codec for class com.datastax.driver.mapping.MapperDefaultKeyspaceTest\\$GroupName, the @UDT annotation declares no default keyspace, and the session is not currently logged to any keyspace")
    public void should_throw_a_meaningful_error_message_when_no_default_udt_keyspace_and_session_not_logged() {
        Session session2 = cluster().connect();
        MappingManager manager = new MappingManager(session2);
        manager.udtCodec(GroupName.class);
    }


    /*
     * An entity that does not specify a keyspace in its @Table annotation. When a keyspace is
     * not specified, the mapper uses the session's logged in keyspace.
     */
    @Table(name = "groups")
    public static class Group {

        @PartitionKey
        @Column(name = "group_id")
        private UUID groupId;

        private String name;

        public Group() {
        }

        public Group(String name) {
            this.name = name;
            this.groupId = UUIDs.random();
        }

        public UUID getGroupId() {
            return groupId;
        }

        public void setGroupId(UUID groupId) {
            this.groupId = groupId;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        @Override
        public boolean equals(Object other) {
            if (other == null || other.getClass() != this.getClass())
                return false;

            Group that = (Group) other;
            return MoreObjects.equal(groupId, that.groupId)
                    && MoreObjects.equal(name, that.name);
        }

        @Override
        public int hashCode() {
            return MoreObjects.hashCode(groupId, name);
        }
    }

    @Table(keyspace = KEYSPACE, name = "groups2")
    public static class Group2 {

        @PartitionKey
        @Column(name = "group_id")
        private UUID groupId;

        @Frozen
        private GroupName name;

        public Group2() {
        }

        public Group2(GroupName name) {
            this.name = name;
            this.groupId = UUIDs.random();
        }

        public UUID getGroupId() {
            return groupId;
        }

        public void setGroupId(UUID groupId) {
            this.groupId = groupId;
        }

        public GroupName getName() {
            return name;
        }

        public void setName(GroupName name) {
            this.name = name;
        }

        @Override
        public boolean equals(Object other) {
            if (other == null || other.getClass() != this.getClass())
                return false;

            Group2 that = (Group2) other;
            return MoreObjects.equal(groupId, that.groupId)
                    && MoreObjects.equal(name, that.name);
        }

        @Override
        public int hashCode() {
            return MoreObjects.hashCode(groupId, name);
        }
    }

    /*
     * User defined type without a keyspace specified. The mapper will use the session's logged
     * keyspace when a keyspace is not specified in the @UDT annotation.
     */
    @UDT(name = "group_name")
    public static class GroupName {
        private String name;

        public GroupName() {
        }

        public GroupName(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        @Override
        public boolean equals(Object other) {
            if (other instanceof GroupName) {
                GroupName that = (GroupName) other;
                return this.name.equals(that.name);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return MoreObjects.hashCode(name);
        }
    }

}
