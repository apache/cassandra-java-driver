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
package com.datastax.driver.mapping;

import java.util.*;

import com.datastax.driver.mapping.annotations.*;
import com.google.common.base.Objects;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import com.datastax.driver.core.CCMBridge;
import com.datastax.driver.core.utils.UUIDs;

/**
 * Tests usage of mapping annotations without specifying a keyspace.
 */
public class MapperDefaultKeyspaceTest extends CCMBridge.PerClassSingleNodeCluster {
    protected Collection<String> getTableDefinitions() {
        return Arrays.asList("CREATE TABLE groups (group_id uuid PRIMARY KEY, name text)",
                             "CREATE TYPE group_name (name text)",
                             "CREATE TABLE groups2 (group_id uuid PRIMARY KEY, name frozen<group_name>)");
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

        public Group() {}

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

            Group that = (Group)other;
            return Objects.equal(groupId, that.groupId)
                && Objects.equal(name, that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(groupId, name);
        }
    }

    @Test(groups = "short")
    public void testTableWithDefaultKeyspace() throws Exception {
        // Ensure that the test session is logged into the "ks" keyspace.
        session.execute("USE ks");

        MappingManager manager = new MappingManager(session);
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

    @Table(keyspace = "ks", name = "groups2")
    public static class Group2 {

        @PartitionKey
        @Column(name = "group_id")
        private UUID groupId;

        @Frozen
        private GroupName name;

        public Group2() {}

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

            Group that = (Group)other;
            return Objects.equal(groupId, that.groupId)
                && Objects.equal(name, that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(groupId, name);
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
                return this.name == that.name;
            }
            return false;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(name);
        }
    }

    @Test(groups = "short")
    public void testUDTWithDefaultKeyspace() throws Exception {
        // Ensure that the test session is logged into the "ks" keyspace.
        session.execute("USE ks");

        MappingManager manager = new MappingManager(session);
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
}
