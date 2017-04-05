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
import com.datastax.driver.core.TestUtils;
import com.datastax.driver.core.utils.CassandraVersion;
import com.datastax.driver.mapping.annotations.*;
import com.google.common.base.Objects;
import org.testng.annotations.Test;

import static com.datastax.driver.core.Metadata.quote;
import static org.assertj.core.api.Assertions.assertThat;

@CCMConfig(createKeyspace = false)
@CassandraVersion("2.1.0")
public class MapperCaseSensitivityTest extends CCMTestsSupport {

    static final String KS = "ks_MapperCaseSensitivityTest";
    static final String TABLE = "table_MapperCaseSensitivityTest";
    static final String TYPE = "udt_MapperCaseSensitivityTest";

    UserNoKeyspace user = new UserNoKeyspace("id", new Address("street", "zip"));

    @Table(keyspace = KS, name = TABLE, caseSensitiveKeyspace = true, caseSensitiveTable = true)
    static class User {

        @PartitionKey
        @Column(name = "userId", caseSensitive = true)
        private String userId;

        @Column(name = "Address", caseSensitive = true)
        private Address address;

        public User() {
        }

        public User(String userId, Address address) {
            this.userId = userId;
            this.address = address;
        }

        public String getUserId() {
            return userId;
        }

        public void setUserId(String userId) {
            this.userId = userId;
        }

        public Address getAddress() {
            return address;
        }

        public void setAddress(Address address) {
            this.address = address;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || !(o instanceof User)) return false;
            User user = (User) o;
            return Objects.equal(userId, user.userId) &&
                    Objects.equal(address, user.address);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(userId, address);
        }
    }

    @UDT(keyspace = KS, name = TYPE, caseSensitiveKeyspace = true, caseSensitiveType = true)
    static class Address {

        @Field(name = "Street", caseSensitive = true)
        private String street;

        @Field(name = "zipCode", caseSensitive = true)
        private String zipCode;

        public Address() {
        }

        public Address(String street, String zipCode) {
            this.street = street;
            this.zipCode = zipCode;
        }

        public String getStreet() {
            return street;
        }

        public void setStreet(String street) {
            this.street = street;
        }

        public String getZipCode() {
            return zipCode;
        }

        public void setZipCode(String zipCode) {
            this.zipCode = zipCode;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || !(o instanceof Address)) return false;
            Address address = (Address) o;
            return Objects.equal(street, address.street) &&
                    Objects.equal(zipCode, address.zipCode);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(getStreet(), getZipCode());
        }
    }

    @Table(name = TABLE, caseSensitiveKeyspace = true, caseSensitiveTable = true)
    static class UserNoKeyspace extends User {

        public UserNoKeyspace() {
        }

        public UserNoKeyspace(String id, Address address) {
            super(id, address);
        }

    }

    @Override
    public void onTestContextInitialized() {
        execute(String.format(TestUtils.CREATE_KEYSPACE_SIMPLE_FORMAT, quote(KS), 1));
        useKeyspace(quote(KS));
        execute(
                "CREATE TYPE " + quote(TYPE) + " (\"Street\" text, \"zipCode\" text)",
                "CREATE TABLE " + quote(TABLE) + " (\"userId\" text PRIMARY KEY, \"Address\" frozen<" + quote(TYPE) + ">)");
    }

    /**
     * Validates that case sensitive identifiers for fields, types, columns, tables and keyspaces work with the mapper
     * when the keyspace name is specified on the mapped class.
     *
     * @jira_ticket JAVA-564
     * @test_category object_mapper
     */
    @Test(groups = "short")
    public void should_handle_case_sensitive_identifiers_when_keyspace_specified() throws Exception {
        MappingManager mappingManager = new MappingManager(session());
        Mapper<User> mapper = mappingManager.mapper(User.class);
        mapper.save(user);
        assertThat(mapper.get(user.getUserId())).isEqualTo(user);
    }

    /**
     * Validates that case sensitive identifiers for fields, types, columns, tables and keyspaces work with the mapper
     * when the keyspace name is *not* specified on the mapped class.
     *
     * @jira_ticket JAVA-564
     * @test_category object_mapper
     */
    @Test(groups = "short")
    public void should_handle_case_sensitive_identifiers_without_keyspace_specified() throws Exception {
        MappingManager mappingManager = new MappingManager(session());
        Mapper<UserNoKeyspace> mapper = mappingManager.mapper(UserNoKeyspace.class);
        mapper.save(user);
        assertThat(mapper.get(user.getUserId())).isEqualTo(user);
    }

}
