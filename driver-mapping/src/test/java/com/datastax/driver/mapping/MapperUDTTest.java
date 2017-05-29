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

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.CodecNotFoundException;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.datastax.driver.core.utils.CassandraVersion;
import com.datastax.driver.core.utils.MoreObjects;
import com.datastax.driver.core.utils.UUIDs;
import com.datastax.driver.mapping.annotations.*;
import com.google.common.collect.Maps;
import org.assertj.core.data.MapEntry;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.*;

import static com.datastax.driver.core.CreateCCM.TestMode.PER_METHOD;
import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.testng.Assert.*;

@SuppressWarnings({"unused", "WeakerAccess"})
@CassandraVersion("2.1.0")
@CreateCCM(PER_METHOD)
public class MapperUDTTest extends CCMTestsSupport {

    @BeforeMethod(groups = "short")
    public void createObjects() {
        execute("CREATE TYPE address (street text, city text, \"ZIP_code\" int, phones set<text>)",
                "CREATE TABLE users (user_id uuid PRIMARY KEY, name text, mainaddress frozen<address>, other_addresses map<text,frozen<address>>)");
    }

    @AfterMethod(groups = "short")
    public void deleteObjects() {
        execute("DROP TABLE IF EXISTS users",
                "DROP TYPE  IF EXISTS address");
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
            if (this == other)
                return true;
            if (other instanceof User) {
                User that = (User) other;
                return MoreObjects.equal(this.userId, that.userId) &&
                        MoreObjects.equal(this.name, that.name) &&
                        MoreObjects.equal(this.mainAddress, that.mainAddress) &&
                        MoreObjects.equal(this.otherAddresses, that.otherAddresses);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return MoreObjects.hashCode(this.userId, this.name, this.mainAddress, this.otherAddresses);
        }

        @Override
        public String toString() {
            return String.format("User(userId=%s, name=%s, mainAddress=%s, otherAddresses=%s)",
                    userId, name, mainAddress, otherAddresses);
        }
    }

    @UDT(name = "address")
    public static class Address {

        // Dummy constant to test that static fields are properly ignored
        public static final int FOO = 1;

        @Field // not strictly required, but we want to check that the annotation works without a name
        private String city;

        // Declared out of order compared to the UDT definition, to make sure that we serialize fields in the correct order (JAVA-884)
        private String street;

        @Field(name = "ZIP_code", caseSensitive = true)
        private int zipCode;

        private Set<String> phones;

        public Address() {
        }

        public Address(String street, String city, int zipCode, String... phones) {
            this.street = street;
            this.city = city;
            this.zipCode = zipCode;
            this.phones = new HashSet<String>();
            Collections.addAll(this.phones, phones);
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
            if (this == other)
                return true;
            if (other instanceof Address) {
                Address that = (Address) other;
                return MoreObjects.equal(this.street, that.street) &&
                        MoreObjects.equal(this.city, that.city) &&
                        MoreObjects.equal(this.zipCode, that.zipCode) &&
                        MoreObjects.equal(this.phones, that.phones);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return MoreObjects.hashCode(this.street, this.city, this.zipCode, this.phones);
        }

        @Override
        public String toString() {
            return String.format("Address(street=%s, city=%s, zip=%d, phones=%s)",
                    street, city, zipCode, phones);
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
        Result<User> getAll();
    }

    @Test(groups = "short")
    public void testSimpleEntity() throws Exception {
        Mapper<User> m = new MappingManager(session()).mapper(User.class);

        User u1 = new User("Paul", new Address("12 4th Street", "Springfield", 12345, "12341343", "435423245"));
        u1.addOtherAddress("work", new Address("5 Main Street", "Springfield", 12345, "23431342"));
        m.save(u1);

        assertEquals(m.get(u1.getUserId()), u1);
    }

    @Test(groups = "short")
    public void should_handle_null_UDT_value() throws Exception {
        Mapper<User> m = new MappingManager(session()).mapper(User.class);

        User u1 = new User("Paul", null);
        m.save(u1);

        assertNull(m.get(u1.getUserId()).getMainAddress());
    }

    @Test(groups = "short")
    public void testAccessor() throws Exception {
        MappingManager manager = new MappingManager(session());

        Mapper<User> m = new MappingManager(session()).mapper(User.class);
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

    @Test(groups = "short")
    public void should_be_able_to_use_udtCodec_standalone() {
        // Create a separate Cluster/Session to start with a CodecRegistry from scratch (so not already registered).
        Cluster cluster = register(Cluster.builder()
                .addContactPoints(getContactPoints())
                .withPort(ccm().getBinaryPort())
                .build());
        CodecRegistry registry = cluster.getConfiguration().getCodecRegistry();
        Session session = cluster.connect(keyspace);

        UUID userId = UUIDs.random();

        // Create a user.
        session.execute("update users SET other_addresses['condo']={street: '101 Ocean Ln', city: 'Jacksonville, FL', \"ZIP_code\": 89898, phones: {'8675309'}} " +
                " WHERE user_id=" + TypeCodec.uuid().format(userId));
        session.execute("update users SET mainaddress={street: '42 Middle of Nowhere', city: 'Lake of the Woods', \"ZIP_code\": 49553, phones: {'8675039'}} " +
                " WHERE user_Id=" + TypeCodec.uuid().format(userId));

        // Get the user.
        Row row = session.execute("select * from users where user_id=?", userId).one();

        UDTValue udtValue = row.getUDTValue("mainaddress");
        assertThat(udtValue.getString("street")).isEqualTo("42 Middle of Nowhere");

        Object udtObject = row.getObject("mainaddress");
        assertThat(udtObject).isEqualTo(udtValue);

        // There shouldn't be a codec for address.
        try {
            assertThat(registry.codecFor(udtValue.getType(), Address.class));
            fail("Didn't expect to find codec for udtType <-> Address");
        } catch (CodecNotFoundException e) {
            // expected.
        }

        // Expect a this_udt <-> UDTValue codec to exist.  This is a pretty safe bet or else we wouldn't get
        // this value back.
        TypeCodec<UDTValue> udtCodec = registry.codecFor(udtValue.getType());
        assertThat(udtCodec.getCqlType()).isEqualTo(udtValue.getType());
        assertThat(udtCodec.getJavaType().getRawType()).isEqualTo(UDTValue.class);

        // Retrieve codec for Address, if it can be mapped it will be created, if already registered it'll be used.
        TypeCodec<Address> codec = new MappingManager(session).udtCodec(Address.class);

        // The codec should be registered after we call udtCodec.
        assertThat(registry.codecFor(udtValue.getType(), Address.class)).isEqualTo(codec);

        // Should be able to retrieve as an Address.
        Address mainAddress = row.get("mainaddress", Address.class);
        assertThat(mainAddress.getCity()).isEqualTo("Lake of the Woods");
        assertThat(mainAddress.getStreet()).isEqualTo("42 Middle of Nowhere");
        assertThat(mainAddress.getZipCode()).isEqualTo(49553);
        assertThat(mainAddress.getPhones()).containsExactly("8675039");

        // Should be able to retrieve within a Map.
        Address expectedOtherAddress = new Address();
        expectedOtherAddress.setStreet("101 Ocean Ln");
        expectedOtherAddress.setCity("Jacksonville, FL");
        expectedOtherAddress.setZipCode(89898);
        expectedOtherAddress.setPhones(Collections.singleton("8675309"));

        Map<String, Address> otherAddresses = row.getMap("other_addresses", String.class, Address.class);
        assertThat(otherAddresses).containsOnly(MapEntry.entry("condo", expectedOtherAddress));
    }

    /**
     * Ensures that if a table is altered after a {@link Mapper} is created that it continues to work as long as
     * the change is compatible.  A new column is added to the table in this case, which is a compatible change.
     * <p/>
     * It also ensures that after the change is made that requesting a new {@link Mapper} returns a different instance
     * instead of the existing one.
     *
     * @jira_ticket JAVA-1126
     * @test_category object_mapper
     */
    @Test(groups = "short")
    public void should_work_normally_when_when_table_is_altered_but_remains_compatible() {
        MappingManager manager = new MappingManager(session());
        User expected = new User("Paul", new Address("12 4th Street", "Springfield", 12345, "12341343", "435423245"));
        Mapper<User> m = manager.mapper(User.class);
        Mapper<User> m1 = manager.mapper(User.class);
        assertThat(m1).isSameAs(m);
        session().execute("ALTER TABLE users ADD foo text");
        m.save(expected);
        User retrieved = m.get(expected.getUserId());
        assertThat(retrieved).isEqualTo(expected);

        // Mapper should be recreated since table changed.
        Mapper<User> m2 = manager.mapper(User.class);
        assertThat(m2).isNotSameAs(m);
    }

    /**
     * Ensures that if a UDT is altered after a {@link Mapper} is created that has a UDT that it continues to work as
     * long as the change is compatible.  A new field is added to the table in this case, which is a compatible change.
     * <p/>
     * It also ensures that after the change is made that requesting a new {@link TypeCodec} for that UDT and a new
     * {@link Mapper} for a table using that UDT returns different instances instead of the existing ones.
     *
     * @jira_ticket JAVA-1126
     * @test_category object_mapper
     */
    @Test(groups = "short")
    public void should_work_normally_when_udt_is_altered_but_remains_compatible() {
        MappingManager manager = new MappingManager(session());
        User expected = new User("Paul", new Address("12 4th Street", "Springfield", 12345, "12341343", "435423245"));
        Mapper<User> m = manager.mapper(User.class);
        TypeCodec c = manager.udtCodec(Address.class);
        TypeCodec c1 = manager.udtCodec(Address.class);
        assertThat(c1).isSameAs(c);
        session().execute("ALTER TYPE address ADD foo text");
        m.save(expected);
        User retrieved = m.get(expected.getUserId());
        assertThat(retrieved).isEqualTo(expected);

        // Codec should be recreated when requested since UDT and thus the table changed.
        Mapper m2 = manager.mapper(User.class);
        assertThat(m2).isNotSameAs(m);

        // Codec should be recreated when requested since UDT changed.
        TypeCodec c2 = manager.udtCodec(Address.class);
        assertThat(c2).isNotSameAs(c);
    }

    /**
     * Ensures that if a table is dropped after a {@link Mapper} is created that the {@link Mapper} can no longer
     * successfully make queries and that attempting to create a new {@link Mapper} throws an
     * {@link IllegalArgumentException}.
     * <p/>
     * It also ensures that requesting a {@link Mapper} fails as the previous {@link Mapper} was evicted and a new
     * one cannot be created as the table has been dropped.
     *
     * @jira_ticket JAVA-1126
     * @test_category object_mapper
     */
    @Test(groups = "short")
    public void should_throw_error_when_table_is_dropped() {
        User user = new User("Paul", new Address("12 4th Street", "Springfield", 12345, "12341343", "435423245"));
        MappingManager manager = new MappingManager(session());
        Mapper<User> mapper = manager.mapper(User.class);
        session().execute("DROP TABLE users");
        // usage of stale mapper
        try {
            mapper.save(user);
            fail("Expected InvalidQueryException");
        } catch (InvalidQueryException e) {
            assertThat(e.getMessage()).contains("unconfigured", "users");
        }
        try {
            mapper.get(user.getUserId());
            fail("Expected InvalidQueryException");
        } catch (InvalidQueryException e) {
            assertThat(e.getMessage()).contains("unconfigured", "users");
        }
        // trying to use a new mapper
        try {
            manager.mapper(User.class);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage()).isEqualTo("Table or materialized view users does not exist in keyspace \"" + keyspace + "\"");
        }
    }

    /**
     * Ensures that if a table is altered after a {@link Mapper} is created that it no longer continues to work if
     * the change is not compatible.  A column is dropped from the table in this case, which is an incompatible change.
     * <p/>
     * It also ensures that requesting a {@link Mapper} fails as the previous {@link Mapper} was evicted and a new
     * one cannot be created as a declared column is missing from the schema but is present in the class definition.
     *
     * @jira_ticket JAVA-1126
     * @test_category object_mapper
     */
    @Test(groups = "short")
    public void should_throw_error_when_table_is_altered_and_is_not_compatible_anymore() {
        User user = new User("Paul", new Address("12 4th Street", "Springfield", 12345, "12341343", "435423245"));
        MappingManager manager = new MappingManager(session());
        Mapper<User> mapper = manager.mapper(User.class);
        session().execute("ALTER TABLE users DROP mainaddress");
        // usage of stale mapper
        try {
            mapper.save(user);
            fail("Expected InvalidQueryException");
        } catch (InvalidQueryException e) {
            // Error message varies by C* version.
            assertThat(e.getMessage()).isIn("Unknown identifier mainaddress", "Undefined column name mainaddress");
        }
        try {
            mapper.get(user.getUserId());
            fail("Expected InvalidQueryException");
        } catch (InvalidQueryException e) {
            // Error message varies by C* version.
            assertThat(e.getMessage()).isIn("Undefined name mainaddress in selection clause", "Undefined column name mainaddress");
        }
        // trying to use a new mapper
        try {
            manager.mapper(User.class);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage()).isIn(String.format("Column mainaddress does not exist in table \"%s\".users", keyspace));
        }
    }

    /**
     * Ensures that if a UDT is altered after a {@link Mapper} is created that it no longer continues to work if
     * the change made to the UDT is not compatible.  A field is renamed in the UDT in this case, which is not a
     * compatible change.
     * <p/>
     * It also ensures that requesting a {@link Mapper} fails as the previous {@link Mapper} was evicted and a new
     * one cannot be created as a declared field is missing from the schema but is present in the class definition.
     * <p/>
     * Also verifies that a {@link TypeCodec} cannot be requested for the UDT as well.
     *
     * @jira_ticket JAVA-1126
     * @test_category object_mapper
     */
    @Test(groups = "short")
    public void should_throw_error_when_udt_is_altered_and_is_not_compatible_anymore() {
        User user = new User("Paul", new Address("12 4th Street", "Springfield", 12345, "12341343", "435423245"));
        MappingManager manager = new MappingManager(session());
        Mapper<User> mapper = manager.mapper(User.class);
        session().execute("ALTER TYPE address RENAME \"ZIP_code\" to zip_code");
        // usage of stale mapper
        try {
            mapper.save(user);
            fail("Expected CodecNotFoundException");
        } catch (CodecNotFoundException e) {
            // ok, codec could not be created
        }
        // insert manually to be able to test retrieval
        session().execute("INSERT INTO users (user_id, name, mainaddress) VALUES (?, 'Paul', { street : '12 4th Street', zip_code : 12345 })", user.getUserId());
        try {
            mapper.get(user.getUserId());
            fail("Expected CodecNotFoundException");
        } catch (CodecNotFoundException e) {
            // ok, codec could not be created
        }
        // trying to use a new mapper
        try {
            manager.mapper(User.class);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage()).isEqualTo(String.format("Field \"ZIP_code\" does not exist in type \"%s\".address", keyspace));
        }
        try {
            manager.udtCodec(Address.class);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage()).isEqualTo(String.format("Field \"ZIP_code\" does not exist in type \"%s\".address", keyspace));
        }
    }

    @UDT(name = "address")
    public static class AddressUnknownField {

        private String city;

        private String street;

        @Field(name = "ZIP_code", caseSensitive = true)
        private int zipCode;

        private Set<String> phones;

        public String province;

        public AddressUnknownField() {
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

        public String getProvince() {
            return province;
        }

        public void setProvince(String province) {
            this.province = province;
        }
    }

    /**
     * Ensures that when attempting to create a {@link TypeCodec} from a class that has a field that does not exist in
     * the UDT that an {@link IllegalArgumentException} is thrown.
     *
     * @jira_ticket JAVA-1126
     * @test_category object_mapper
     */
    @Test(groups = "short", expectedExceptions = IllegalArgumentException.class)
    public void should_fail_to_create_codec_if_class_has_field_not_in_udt() {
        MappingManager manager = new MappingManager(session());
        manager.getUDTCodec(AddressUnknownField.class);
    }

    @UDT(name = "nonexistent")
    public static class NonExistentUDT {
        public String name;

        public void setName(String name) {
            this.name = name;
        }

        public String getName() {
            return this.name;
        }
    }

    /**
     * Ensures that when attempting to create a {@link TypeCodec} from a class that has a {@link UDT} annotation with
     * a name that doesn't exist in the current keyspace that an {@link IllegalArgumentException} is thrown.
     *
     * @jira_ticket JAVA-1126
     * @test_category object_mapper
     */
    @Test(groups = "short", expectedExceptions = IllegalArgumentException.class)
    public void should_fail_to_create_codec_if_udt_does_not_exist() {
        MappingManager manager = new MappingManager(session());
        manager.getUDTCodec(NonExistentUDT.class);
    }

    @Table(name = "users")
    public static class UserWithAddressUnknownField {
        @PartitionKey
        @Column(name = "user_id")
        private UUID userId;

        private String name;

        @Frozen
        private AddressUnknownField mainAddress;

        @Column(name = "other_addresses")
        @FrozenValue
        private Map<String, Address> otherAddresses = Maps.newHashMap();

        public UserWithAddressUnknownField() {
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

        public AddressUnknownField getMainAddress() {
            return mainAddress;
        }

        public void setMainAddress(AddressUnknownField address) {
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
    }

    /**
     * Ensures that when attempting to create a {@link Mapper} from a class that has a field that is a class mapping to
     * a user type that has a field that does not exist in that UDT that an {@link IllegalArgumentException} is thrown.
     *
     * @jira_ticket JAVA-1126
     * @test_category object_mapper
     */
    @Test(groups = "short", expectedExceptions = IllegalArgumentException.class)
    public void should_fail_to_create_mapper_if_class_has_udt_field_class_that_has_field_not_in_udt() {
        MappingManager manager = new MappingManager(session());
        manager.mapper(UserWithAddressUnknownField.class);
    }

    /**
     * Ensures that MappedUDTCodec is able to properly format UDTs when printing the query string of a BuiltStatement.
     *
     * @jira_ticket JAVA-1272
     * @test_category object_mapper
     */
    @Test(groups = "short")
    public void should_format_mapped_udt() throws Exception {
        MappingManager manager = new MappingManager(session());
        UUID uuid = UUIDs.random();
        BuiltStatement update =
                update("users")
                        .with(set("mainaddress", new Address("12 4th Street", "Springfield", 12345, "12341343", "435423245")))
                        .where(eq("user_id", uuid));
        CodecRegistry codecRegistry = cluster().getConfiguration().getCodecRegistry();
        codecRegistry.register(manager.udtCodec(Address.class));
        String queryString = update.getQueryString(codecRegistry);
        assertThat(queryString)
                .isEqualTo("UPDATE users SET mainaddress=? WHERE user_id=?;");
        update.setForceNoValues(true);
        queryString = update.getQueryString(codecRegistry);
        assertThat(queryString).isEqualTo(
                "UPDATE users " +
                        "SET mainaddress={street:'12 4th Street',city:'Springfield',\"ZIP_code\":12345,phones:{'435423245','12341343'}} " +
                        "WHERE user_id=" + uuid + ";");
        // check that the query string is valid
        session().execute(queryString);
    }

    /**
     * Ensures that MappedUDTCodec is able to properly parse UDTs.
     *
     * @jira_ticket JAVA-1272
     * @test_category object_mapper
     */
    @Test(groups = "short")
    public void should_parse_mapped_udt() throws Exception {
        MappingManager manager = new MappingManager(session());
        TypeCodec<Address> codec = manager.udtCodec(Address.class);
        Address actual = codec.parse("{street:'12 4th Street',city:'Springfield',\"ZIP_code\":12345,phones:{'435423245','12341343'}}");
        assertThat(actual).isEqualTo(new Address("12 4th Street", "Springfield", 12345, "12341343", "435423245"));
    }

}
