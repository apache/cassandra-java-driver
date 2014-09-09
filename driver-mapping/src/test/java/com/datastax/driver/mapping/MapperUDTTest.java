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

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.datastax.driver.core.CCMBridge;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.utils.UUIDs;
import com.datastax.driver.mapping.annotations.*;

public class MapperUDTTest extends CCMBridge.PerClassSingleNodeCluster {

    protected Collection<String> getTableDefinitions() {
        return Arrays.asList("CREATE TYPE address (street text, city text, zip_code int, phones set<text>)",
                             "CREATE TABLE users (user_id uuid PRIMARY KEY, name text, main_address frozen<address>, other_addresses map<text,frozen<address>>)",
                             "CREATE TYPE sub(i int)",
                             "CREATE TABLE collection_examples (id int PRIMARY KEY, l list<frozen<sub>>, s set<frozen<sub>>, m1 map<int,frozen<sub>>, m2 map<frozen<sub>,int>, m3 map<frozen<sub>,frozen<sub>>)",
                             "CREATE TYPE group_name (name text)",
                             "CREATE TABLE groups (group_id uuid PRIMARY KEY, name frozen<group_name>)");
    }

    @Table(keyspace = "ks", name = "users",
           readConsistency = "QUORUM",
           writeConsistency = "QUORUM")
    public static class User {
        @PartitionKey
        @Column(name = "user_id")
        private UUID userId;

        private String name;

        @Column(name = "main_address")
        @Frozen
        private Address mainAddress;

        @Column(name = "other_addresses")
        @FrozenValue
        private Map<String, Address> otherAddresses;

        public User() {
        }

        public User(String name, Address address) {
            this.userId = UUIDs.random();
            this.name = name;
            this.mainAddress = address;
            this.otherAddresses = new HashMap<String, Address>();
        }

        public UUID getUserId() {
            return userId;
        }

        public void setUserId(UUID userId) {
            this.userId = userId;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Address getMainAddress() {
            return mainAddress;
        }

        public void setMainAddress(Address address) {
            this.mainAddress = address;
        }

        public Map<String, Address> getOtherAddresses() {
            return otherAddresses;
        }

        public void setOtherAddresses(Map<String, Address> otherAddresses) {
            this.otherAddresses = otherAddresses;
        }

        public void addOtherAddress(String name, Address address) {
            this.otherAddresses.put(name, address);
        }

        @Override
        public boolean equals(Object other) {
            if (other instanceof User) {
                User that = (User) other;
                return Objects.equal(this.userId, that.userId) &&
                       Objects.equal(this.name, that.name) &&
                       Objects.equal(this.mainAddress, that.mainAddress) &&
                       Objects.equal(this.otherAddresses, that.otherAddresses);
            }
            return false;
        }
    }

    @UDT(keyspace = "ks", name = "address")
    public static class Address {
        private String street;

        private String city;

        @Field(name = "zip_code")
        private int zipCode;

        private Set<String> phones;

        public Address() {
        }

        public Address(String street, String city, int zipCode, String... phones) {
            this.street = street;
            this.city = city;
            this.zipCode = zipCode;
            this.phones = new HashSet<String>();
            for (String phone : phones) {
                this.phones.add(phone);
            }
        }

        public String getStreet() {
            return street;
        }

        public void setStreet(String street) {
            this.street = street;
        }

        public String getCity() {
            return city;
        }

        public void setCity(String city) {
            this.city = city;
        }

        public int getZipCode() {
            return zipCode;
        }

        public void setZipCode(int zipCode) {
            this.zipCode = zipCode;
        }

        public Set<String> getPhones() {
            return phones;
        }

        public void setPhones(Set<String> phones) {
            this.phones = phones;
        }

        @Override
        public boolean equals(Object other) {
            if (other instanceof Address) {
                Address that = (Address) other;
                return Objects.equal(this.street, that.street) &&
                       Objects.equal(this.city, that.city) &&
                       Objects.equal(this.zipCode, that.zipCode) &&
                       Objects.equal(this.phones, that.phones);
            }
            return false;
        }
    }

    @Accessor
    public interface UserAccessor {
        @Query("SELECT * FROM ks.users WHERE user_id=:userId")
        User getOne(@Param("userId") UUID userId);

        @Query("UPDATE ks.users SET other_addresses[:name]=:address WHERE user_id=:id")
        ResultSet addAddress(@Param("id") UUID id, @Param("name") String addressName, @Param("address") Address address);

        @Query("SELECT * FROM ks.users")
        public Result<User> getAll();
    }

    @Test(groups = "short")
    public void testSimpleEntity() throws Exception {
        Mapper<User> m = new MappingManager(session).mapper(User.class);

        User u1 = new User("Paul", new Address("12 4th Street", "Springfield", 12345, "12341343", "435423245"));
        u1.addOtherAddress("work", new Address("5 Main Street", "Springfield", 12345, "23431342"));
        m.save(u1);

        assertEquals(m.get(u1.getUserId()), u1);
    }

    @Test(groups = "short")
    public void testAccessor() throws Exception {
        MappingManager manager = new MappingManager(session);

        Mapper<User> m = new MappingManager(session).mapper(User.class);
        User u1 = new User("Paul", new Address("12 4th Street", "Springfield", 12345, "12341343", "435423245"));
        m.save(u1);

        UserAccessor userAccessor = manager.createAccessor(UserAccessor.class);

        Address workAddress = new Address("5 Main Street", "Springfield", 12345, "23431342");
        userAccessor.addAddress(u1.getUserId(), "work", workAddress);

        User u2 = userAccessor.getOne(u1.getUserId());
        assertEquals(workAddress, u2.getOtherAddresses().get("work"));

        // No argument call
        Result<User> u = userAccessor.getAll();
        assertEquals(u.one(), u2);
        assertTrue(u.isExhausted());
    }

    @UDT(keyspace = "ks", name = "sub")
    public static class Sub {
        private int i;

        public Sub() {
        }

        public Sub(int i) {
            this.i = i;
        }

        public int getI() {
            return i;
        }

        public void setI(int i) {
            this.i = i;
        }

        @Override
        public boolean equals(Object other) {
            if (other instanceof Sub) {
                Sub that = (Sub) other;
                return this.i == that.i;
            }
            return false;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(i);
        }
    }

    @Table(keyspace = "ks", name = "collection_examples")
    public static class CollectionExamples {
        @PartitionKey
        private int id;

        @FrozenValue
        private List<Sub> l;

        @FrozenValue
        private Set<Sub> s;

        @FrozenValue
        private Map<Integer, Sub> m1;

        @FrozenKey
        private Map<Sub, Integer> m2;

        @FrozenKey
        @FrozenValue
        private Map<Sub, Sub> m3;

        public CollectionExamples() {
        }

        public CollectionExamples(int id, int value) {
            this.id = id;
            // Just fill the collections with random values
            Sub sub1 = new Sub(value);
            Sub sub2 = new Sub(value + 1);
            this.l = Lists.newArrayList(sub1, sub2);
            this.s = Sets.newHashSet(sub1, sub2);
            this.m1 = ImmutableMap.of(1, sub1, 2, sub2);
            this.m2 = ImmutableMap.of(sub1, 1, sub2, 2);
            this.m3 = ImmutableMap.of(sub1, sub1, sub2, sub2);
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public List<Sub> getL() {
            return l;
        }

        public void setL(List<Sub> l) {
            this.l = l;
        }

        public Set<Sub> getS() {
            return s;
        }

        public void setS(Set<Sub> s) {
            this.s = s;
        }

        public Map<Integer, Sub> getM1() {
            return m1;
        }

        public void setM1(Map<Integer, Sub> m1) {
            this.m1 = m1;
        }

        public Map<Sub, Integer> getM2() {
            return m2;
        }

        public void setM2(Map<Sub, Integer> m2) {
            this.m2 = m2;
        }

        public Map<Sub, Sub> getM3() {
            return m3;
        }

        public void setM3(Map<Sub, Sub> m3) {
            this.m3 = m3;
        }

        @Override
        public boolean equals(Object other) {
            if (other instanceof CollectionExamples) {
                CollectionExamples that = (CollectionExamples) other;
                return Objects.equal(this.id, that.id) &&
                       Objects.equal(this.l, that.l) &&
                       Objects.equal(this.s, that.s) &&
                       Objects.equal(this.m1, that.m1) &&
                       Objects.equal(this.m2, that.m2) &&
                       Objects.equal(this.m3, that.m3);
            }
            return false;
        }
    }

    @Test(groups = "short")
    public void testCollections() throws Exception {
        Mapper<CollectionExamples> m = new MappingManager(session).mapper(CollectionExamples.class);

        CollectionExamples c = new CollectionExamples(1, 1);
        m.save(c);

        assertEquals(m.get(c.getId()), c);
    }

    @Table(keyspace = "ks", name = "groups")
    public static class Group {

        @PartitionKey
        @Column(name = "group_id")
        private UUID groupId;

        @Frozen
        private GroupName name;

        public Group() {}

        public Group(GroupName name) {
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
        Mapper<Group> m = manager.mapper(Group.class);
        Group group = new Group(new GroupName("testGroup"));
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
}
