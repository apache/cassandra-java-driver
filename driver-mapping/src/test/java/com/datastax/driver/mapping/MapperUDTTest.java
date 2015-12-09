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
package com.datastax.driver.mapping;

import com.datastax.driver.core.CCMBridge;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.utils.UUIDs;
import com.datastax.driver.mapping.annotations.*;
import com.google.common.base.Objects;
import com.google.common.collect.Maps;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.*;

public class MapperUDTTest extends CCMBridge.PerClassSingleNodeCluster {

    protected Collection<String> getTableDefinitions() {
        return Arrays.asList("CREATE TYPE address (street text, city text, zip_code int, phones set<text>)",
                "CREATE TABLE users (user_id uuid PRIMARY KEY, name text, mainaddress frozen<address>, other_addresses map<text,frozen<address>>)");
    }

    @BeforeMethod(groups = "short")
    public void clean() {
        session.execute("TRUNCATE users");
    }

    @Table(name = "users",
            readConsistency = "QUORUM",
            writeConsistency = "QUORUM")
    public static class User {
        @PartitionKey
        @Column(name = "user_id")
        private UUID userId;

        private String name;

        @Frozen
        private Address mainAddress;

        @Column(name = "other_addresses")
        @FrozenValue
        private Map<String, Address> otherAddresses = Maps.newHashMap();

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

    @UDT(name = "address")
    public static class Address {

        // Dummy constant to test that static fields are properly ignored
        public static final int FOO = 1;

        private String street;

        @Field // not strictly required, but we want to check that the annotation works without a name
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
        @Query("SELECT * FROM users WHERE user_id=:userId")
        User getOne(@Param("userId") UUID userId);

        @Query("UPDATE users SET other_addresses[:name]=:address WHERE user_id=:id")
        ResultSet addAddress(@Param("id") UUID id, @Param("name") String addressName, @Param("address") Address address);

        @Query("UPDATE users SET other_addresses=:addresses where user_id=:id")
        ResultSet setOtherAddresses(@Param("id") UUID id, @Param("addresses") Map<String, Address> addresses);

        @Query("SELECT * FROM users")
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
    public void should_handle_null_UDT_value() throws Exception {
        Mapper<User> m = new MappingManager(session).mapper(User.class);

        User u1 = new User("Paul", null);
        m.save(u1);

        assertNull(m.get(u1.getUserId()).getMainAddress());
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

        // Adding a null value should remove it from the list.
        userAccessor.addAddress(u1.getUserId(), "work", null);
        User u3 = userAccessor.getOne(u1.getUserId());
        assertThat(u3.getOtherAddresses()).doesNotContainKey("work");

        // Add a bunch of other addresses
        Map<String, Address> otherAddresses = Maps.newHashMap();
        otherAddresses.put("work", workAddress);
        otherAddresses.put("cabin", new Address("42 Middle of Nowhere", "Lake of the Woods", 49553, "8675309"));

        userAccessor.setOtherAddresses(u1.getUserId(), otherAddresses);
        User u4 = userAccessor.getOne(u1.getUserId());
        assertThat(u4.getOtherAddresses()).isEqualTo(otherAddresses);

        // Nullify other addresses
        userAccessor.setOtherAddresses(u1.getUserId(), null);
        User u5 = userAccessor.getOne(u1.getUserId());
        assertThat(u5.getOtherAddresses()).isEmpty();

        // No argument call
        Result<User> u = userAccessor.getAll();
        assertEquals(u.one(), u5);
        assertTrue(u.isExhausted());
    }
}
