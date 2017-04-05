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

import com.datastax.driver.core.utils.CassandraVersion;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@CassandraVersion("2.1.0")
public class TypeCodecUDTIntegrationTest extends CCMTestsSupport {

    private final String insertQuery = "INSERT INTO users (id, name, address) VALUES (?, ?, ?)";
    private final String selectQuery = "SELECT id, name, address FROM users WHERE id = ?";

    private final UUID uuid = UUID.randomUUID();

    private final Phone phone1 = new Phone("1234567", Sets.newHashSet("home", "iphone"));
    private final Phone phone2 = new Phone("2345678", Sets.newHashSet("work"));
    private final Address address = new Address("blah", 75010, Lists.newArrayList(phone1, phone2));

    private UserType addressType;
    private UserType phoneType;

    private UDTValue addressValue;

    @Override
    public void onTestContextInitialized() {
        execute(
                "CREATE TYPE IF NOT EXISTS \"phone\" (number text, tags set<text>)",
                "CREATE TYPE IF NOT EXISTS \"address\" (street text, zipcode int, phones list<frozen<phone>>)",
                "CREATE TABLE IF NOT EXISTS \"users\" (id uuid PRIMARY KEY, name text, address frozen<address>)"
        );
    }

    @Test(groups = "short")
    public void should_handle_udts_with_default_codecs() {
        setUpUserTypes(cluster());
        // simple statement
        session().execute(insertQuery, uuid, "John Doe", addressValue);
        ResultSet rows = session().execute(selectQuery, uuid);
        Row row = rows.one();
        assertRow(row);
        // prepared + values
        PreparedStatement ps = session().prepare(insertQuery);
        session().execute(ps.bind(uuid, "John Doe", addressValue));
        rows = session().execute(selectQuery, uuid);
        row = rows.one();
        assertRow(row);
        // bound with setUDTValue
        session().execute(ps.bind().setUUID(0, uuid).setString(1, "John Doe").setUDTValue(2, addressValue));
        rows = session().execute(selectQuery, uuid);
        row = rows.one();
        assertRow(row);
    }

    @Test(groups = "short")
    public void should_handle_udts_with_custom_codecs() {
        CodecRegistry codecRegistry = new CodecRegistry();
        Cluster cluster = register(Cluster.builder()
                .addContactPoints(getContactPoints())
                .withPort(ccm().getBinaryPort())
                .withCodecRegistry(codecRegistry)
                .build());
        Session session = cluster.connect(keyspace);
        setUpUserTypes(cluster);
        TypeCodec<UDTValue> addressTypeCodec = TypeCodec.userType(addressType);
        TypeCodec<UDTValue> phoneTypeCodec = TypeCodec.userType(phoneType);
        codecRegistry
                .register(new AddressCodec(addressTypeCodec, Address.class))
                .register(new PhoneCodec(phoneTypeCodec, Phone.class))
        ;
        session.execute(insertQuery, uuid, "John Doe", address);
        ResultSet rows = session.execute(selectQuery, uuid);
        Row row = rows.one();
        assertThat(row.getUUID(0)).isEqualTo(uuid);
        assertThat(row.getObject(0)).isEqualTo(uuid);
        assertThat(row.get(0, UUID.class)).isEqualTo(uuid);
        assertThat(row.getString(1)).isEqualTo("John Doe");
        assertThat(row.getObject(1)).isEqualTo("John Doe");
        assertThat(row.get(1, String.class)).isEqualTo("John Doe");
        assertThat(row.getUDTValue(2)).isEqualTo(addressValue);
        // corner case: getObject should use default codecs;
        // but tuple and udt codecs are registered on the fly;
        // so if we have another manually-registered codec
        // that one will be picked up :(
        assertThat(row.getObject(2)).isEqualTo(address);
        assertThat(row.get(2, UDTValue.class)).isEqualTo(addressValue);
        assertThat(row.get(2, Address.class)).isEqualTo(address);
    }

    private void assertRow(Row row) {
        assertThat(row.getUUID(0)).isEqualTo(uuid);
        assertThat(row.getObject(0)).isEqualTo(uuid);
        assertThat(row.get(0, UUID.class)).isEqualTo(uuid);
        assertThat(row.getString(1)).isEqualTo("John Doe");
        assertThat(row.getObject(1)).isEqualTo("John Doe");
        assertThat(row.get(1, String.class)).isEqualTo("John Doe");
        assertThat(row.getUDTValue(2)).isEqualTo(addressValue);
        assertThat(row.getObject(2)).isEqualTo(addressValue);
        assertThat(row.get(2, UDTValue.class)).isEqualTo(addressValue);
    }

    private void setUpUserTypes(Cluster cluster) {
        addressType = cluster.getMetadata().getKeyspace(keyspace).getUserType("address");
        phoneType = cluster.getMetadata().getKeyspace(keyspace).getUserType("phone");
        UDTValue phone1Value = phoneType.newValue()
                .setString("number", phone1.number)
                .setSet("tags", phone1.tags);
        UDTValue phone2Value = phoneType.newValue()
                .setString("number", phone2.number)
                .setSet("tags", phone2.tags);
        addressValue = addressType.newValue()
                .setString("street", address.street)
                .setInt(1, address.zipcode)
                .setList("phones", Lists.newArrayList(phone1Value, phone2Value));
    }

    static class AddressCodec extends MappingCodec<Address, UDTValue> {

        private final UserType userType;

        public AddressCodec(TypeCodec<UDTValue> innerCodec, Class<Address> javaType) {
            super(innerCodec, javaType);
            userType = (UserType) innerCodec.getCqlType();
        }

        @Override
        protected Address deserialize(UDTValue value) {
            return value == null ? null : new Address(value.getString("street"), value.getInt("zipcode"), value.getList("phones", Phone.class));
        }

        @Override
        protected UDTValue serialize(Address value) {
            return value == null ? null : userType.newValue().setString("street", value.street).setInt("zipcode", value.zipcode).setList("phones", value.phones, Phone.class);
        }
    }

    static class PhoneCodec extends MappingCodec<Phone, UDTValue> {

        private final UserType userType;

        public PhoneCodec(TypeCodec<UDTValue> innerCodec, Class<Phone> javaType) {
            super(innerCodec, javaType);
            userType = (UserType) innerCodec.getCqlType();
        }

        @Override
        protected Phone deserialize(UDTValue value) {
            return value == null ? null : new Phone(value.getString("number"), value.getSet("tags", String.class));
        }

        @Override
        protected UDTValue serialize(Phone value) {
            return value == null ? null : userType.newValue().setString("number", value.number).setSet("tags", value.tags);
        }
    }

    static class Address {

        String street;

        int zipcode;

        List<Phone> phones;

        public Address(String street, int zipcode, List<Phone> phones) {
            this.street = street;
            this.zipcode = zipcode;
            this.phones = phones;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            Address address = (Address) o;
            return zipcode == address.zipcode && street.equals(address.street) && phones.equals(address.phones);
        }

        @Override
        public int hashCode() {
            int result = street.hashCode();
            result = 31 * result + zipcode;
            result = 31 * result + phones.hashCode();
            return result;
        }
    }

    static class Phone {

        String number;

        Set<String> tags;

        public Phone(String number, Set<String> tags) {
            this.number = number;
            this.tags = tags;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            Phone phone = (Phone) o;
            return number.equals(phone.number) && tags.equals(phone.tags);
        }

        @Override
        public int hashCode() {
            int result = number.hashCode();
            result = 31 * result + tags.hashCode();
            return result;
        }
    }
}
