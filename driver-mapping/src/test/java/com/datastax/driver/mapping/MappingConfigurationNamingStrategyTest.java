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
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.utils.CassandraVersion;
import com.datastax.driver.mapping.annotations.*;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@CassandraVersion(value = "2.1.0")
public class MappingConfigurationNamingStrategyTest extends CCMTestsSupport {

    private static AtomicInteger counter = new AtomicInteger(0);

    @Override
    public void onTestContextInitialized() {
        execute("CREATE TABLE lowerlisp (\"primary-key\" int primary key, \"my-value\" int)",
                "CREATE TABLE uppersnake (\"PRIMARY_KEY\" int primary key, \"MY_VALUE\" int)",
                "CREATE TYPE \"ADDRESS\" (\"ZIP_CODE\" int, \"CITY_AND_STATE\" text)",
                "CREATE TABLE user (\"NAME\" text primary key, \"ADDRESS\" frozen<\"ADDRESS\">)");
    }

    @Table(name = "lowerlisp")
    static class UpperSnake {

        @PartitionKey
        int PRIMARY_KEY;

        int MY_VALUE;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            UpperSnake that = (UpperSnake) o;

            return PRIMARY_KEY == that.PRIMARY_KEY && MY_VALUE == that.MY_VALUE;
        }

        @Override
        public int hashCode() {
            int result = PRIMARY_KEY;
            result = 31 * result + MY_VALUE;
            return result;
        }
    }

    @Test(groups = "short")
    public void should_use_naming_strategy_fields() {
        // given a configuration with a java naming convention of upper snake case (i.e. HELLO_WORLD) and a
        // cassandra naming convention of lower lisp case (i.e. hello-world) with and access strategy of fields
        MappingConfiguration conf = MappingConfiguration.builder()
                .withPropertyMapper(new DefaultPropertyMapper()
                        .setNamingStrategy(new DefaultNamingStrategy(
                                NamingConventions.UPPER_SNAKE_CASE,
                                NamingConventions.LOWER_LISP_CASE))
                        .setPropertyAccessStrategy(PropertyAccessStrategy.FIELDS))
                .build();

        MappingManager mappingManager = new MappingManager(session(), conf);
        // when creating a mapper
        // should succeed since fields match the upper snake case strategy and they are appropriately converted
        // to lower lisp format (PRIMARY_KEY -> primary-key, MY_VALUE -> my-value)
        Mapper<UpperSnake> mapper = mappingManager.mapper(UpperSnake.class);

        // should be able to insert and retrieve data
        UpperSnake in = new UpperSnake();
        in.PRIMARY_KEY = counter.incrementAndGet();
        in.MY_VALUE = counter.incrementAndGet();

        mapper.save(in);

        UpperSnake out = mapper.get(in.PRIMARY_KEY);
        assertThat(out).isEqualTo(in);
    }

    @Table(name = "lowerlisp")
    static class NamingStrategyOverrideField {

        // Override what would be mapped to key to primary-key
        @PartitionKey
        @Column(name = "primary-key")
        int key;

        int MY_VALUE;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            NamingStrategyOverrideField that = (NamingStrategyOverrideField) o;

            return key == that.key && MY_VALUE == that.MY_VALUE;
        }

        @Override
        public int hashCode() {
            int result = key;
            result = 31 * result + MY_VALUE;
            return result;
        }
    }

    @Test(groups = "short")
    public void should_override_naming_strategy_with_column_annotation_name_fields() {
        // given a configuration with a java naming convention of upper snake case (i.e. HELLO_WORLD) and a
        // cassandra naming convention of lower lisp case (i.e. hello-world) with an access strategy of fields
        MappingConfiguration conf = MappingConfiguration.builder()
                .withPropertyMapper(new DefaultPropertyMapper()
                        .setNamingStrategy(new DefaultNamingStrategy(
                                NamingConventions.UPPER_SNAKE_CASE,
                                NamingConventions.LOWER_LISP_CASE))
                        .setPropertyAccessStrategy(PropertyAccessStrategy.FIELDS))
                .build();

        // when creating a mapper
        // should succeed since fields match the upper snake case strategy and they are appropriately converted
        // to lower lisp format (MY_VALUE -> my-value) and since there is a @Column-annotated name override (key field
        // has override to 'primary-key'
        MappingManager mappingManager = new MappingManager(session(), conf);
        Mapper<NamingStrategyOverrideField> mapper = mappingManager.mapper(NamingStrategyOverrideField.class);

        // should be able to insert and retrieve data
        NamingStrategyOverrideField in = new NamingStrategyOverrideField();
        in.key = counter.incrementAndGet();
        in.MY_VALUE = counter.incrementAndGet();

        mapper.save(in);

        NamingStrategyOverrideField out = mapper.get(in.key);
        assertThat(out).isEqualTo(in);
    }

    @Table(name = "uppersnake")
    @SuppressWarnings({"unused", "WeakerAccess"})
    static class LowerSnake {

        private int _primaryKey;
        private int _myValue;

        @PartitionKey
        public int getprimary_key() {
            return _primaryKey;
        }

        public void setprimary_key(int k) {
            this._primaryKey = k;
        }

        public int getmy_value() {
            return _myValue;
        }

        public void setmy_value(int v) {
            this._myValue = v;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            LowerSnake that = (LowerSnake) o;

            return _primaryKey == that._primaryKey && _myValue == that._myValue;
        }

        @Override
        public int hashCode() {
            int result = _primaryKey;
            result = 31 * result + _myValue;
            return result;
        }
    }

    @Test(groups = "short")
    public void should_use_naming_strategy_getters_and_setters() {
        // given a configuration with a java naming convention of lower snake case (i.e. hello_world) and a
        // cassandra naming convention of upper snake case (i.e. HELLO_WORLD) with an access strategy of
        // getters and setters
        MappingConfiguration conf = MappingConfiguration.builder()
                .withPropertyMapper(new DefaultPropertyMapper()
                        .setNamingStrategy(new DefaultNamingStrategy(
                                NamingConventions.LOWER_SNAKE_CASE,
                                NamingConventions.UPPER_SNAKE_CASE))
                        .setPropertyAccessStrategy(PropertyAccessStrategy.GETTERS_AND_SETTERS))
                .build();

        // when creating a mapper
        // should succeed since getters match the lower snake case strategy and they are appropriately converted
        // to upper snake case format (getprimary_key -> PRIMARY_KEY, getmy_value -> MY_VALUE)
        MappingManager mappingManager = new MappingManager(session(), conf);
        Mapper<LowerSnake> mapper = mappingManager.mapper(LowerSnake.class);

        // should be able to insert and retrieve data
        LowerSnake in = new LowerSnake();
        in.setprimary_key(counter.incrementAndGet());
        in.setmy_value(counter.incrementAndGet());

        mapper.save(in);

        LowerSnake out = mapper.get(in.getprimary_key());
        assertThat(out).isEqualTo(in);
    }

    @Table(name = "uppersnake")
    @SuppressWarnings({"unused", "WeakerAccess"})
    static class NamingStrategyOverrideGetter {

        private int _primaryKey;
        private int _myValue;

        // Override column name from what would be mapped to KEY to PRIMARY_KEY
        @PartitionKey
        @Column(name = "PRIMARY_KEY", caseSensitive = true)
        public int getKey() {
            return _primaryKey;
        }

        public void setKey(int k) {
            this._primaryKey = k;
        }

        public int getmy_value() {
            return _myValue;
        }

        public void setmy_value(int v) {
            this._myValue = v;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            NamingStrategyOverrideGetter that = (NamingStrategyOverrideGetter) o;

            return _primaryKey == that._primaryKey && _myValue == that._myValue;
        }

        @Override
        public int hashCode() {
            int result = _primaryKey;
            result = 31 * result + _myValue;
            return result;
        }
    }

    @Test(groups = "short")
    public void should_override_naming_strategy_with_column_annotation_name_getters() {
        // given a configuration with a java naming convention of lower snake case (i.e. hello_world) and a
        // cassandra naming convention of upper snake case (i.e. HELLO_WORLD) with an access strategy for
        // getters and setters
        MappingConfiguration conf = MappingConfiguration.builder()
                .withPropertyMapper(new DefaultPropertyMapper()
                        .setNamingStrategy(new DefaultNamingStrategy(
                                NamingConventions.LOWER_SNAKE_CASE,
                                NamingConventions.UPPER_SNAKE_CASE))
                        .setPropertyAccessStrategy(PropertyAccessStrategy.GETTERS_AND_SETTERS))
                .build();

        // when creating a mapper
        // should succeed since getters match the lower snake case strategy and they are appropriately converted
        // to upper snake case format (getmy_value -> MY_VALUE) and @Column-annotated name override on getKey is
        // PRIMARY_KEY.
        MappingManager mappingManager = new MappingManager(session(), conf);
        Mapper<NamingStrategyOverrideGetter> mapper = mappingManager.mapper(NamingStrategyOverrideGetter.class);

        // should be able to insert and retrieve data
        NamingStrategyOverrideGetter in = new NamingStrategyOverrideGetter();
        in.setKey(counter.incrementAndGet());
        in.setmy_value(counter.incrementAndGet());

        mapper.save(in);

        NamingStrategyOverrideGetter out = mapper.get(in.getKey());
        assertThat(out).isEqualTo(in);
    }

    @Accessor
    interface NamingStrategyAccessor {
        @Query("SELECT * from uppersnake where \"PRIMARY_KEY\" = ?")
        Result<NamingStrategyOverrideGetter> getValue(int key);
    }

    @Test(groups = "short")
    public void should_apply_naming_strategy_when_mapping_from_accessor_result() {
        // given a configuration with a java naming convention of lower snake case (i.e. hello_world) and a
        // cassandra naming convention of upper snake case (i.e. HELLO_WORLD) with an access strategy for
        // getters and setters
        MappingConfiguration conf = MappingConfiguration.builder()
                .withPropertyMapper(new DefaultPropertyMapper()
                        .setNamingStrategy(new DefaultNamingStrategy(
                                NamingConventions.LOWER_SNAKE_CASE,
                                NamingConventions.UPPER_SNAKE_CASE))
                        .setPropertyAccessStrategy(PropertyAccessStrategy.GETTERS_AND_SETTERS))
                .build();

        MappingManager mappingManager = new MappingManager(session(), conf);
        Mapper<NamingStrategyOverrideGetter> mapper = mappingManager.mapper(NamingStrategyOverrideGetter.class);
        NamingStrategyAccessor acc = mappingManager.createAccessor(NamingStrategyAccessor.class);

        NamingStrategyOverrideGetter in = new NamingStrategyOverrideGetter();
        in.setKey(counter.incrementAndGet());
        in.setmy_value(counter.incrementAndGet());

        mapper.save(in);

        Result<NamingStrategyOverrideGetter> out = acc.getValue(in.getKey());

        assertThat(out.one()).isEqualTo(in);
    }

    @UDT(name = "ADDRESS", caseSensitiveType = true)
    static class Address {
        int zip_code;

        @Field(name = "CITY_AND_STATE", caseSensitive = true)
        String cityAndState;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Address address = (Address) o;

            return zip_code == address.zip_code && (cityAndState != null ? cityAndState.equals(address.cityAndState) : address.cityAndState == null);
        }

        @Override
        public int hashCode() {
            int result = zip_code;
            result = 31 * result + (cityAndState != null ? cityAndState.hashCode() : 0);
            return result;
        }
    }

    @Table(name = "user")
    static class User {
        @PartitionKey
        String name;

        Address address;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            User user = (User) o;

            return (name != null ? name.equals(user.name) : user.name == null) && (address != null ? address.equals(user.address) : user.address == null);
        }

        @Override
        public int hashCode() {
            int result = name != null ? name.hashCode() : 0;
            result = 31 * result + (address != null ? address.hashCode() : 0);
            return result;
        }
    }

    @Test(groups = "short")
    public void should_apply_naming_strategy_to_udts() {
        // given a configuration with a java naming convention of lower snake case (i.e. hello_world) and a
        // cassandra naming convention of upper snake case (i.e. HELLO_WORLD) with an access strategy for
        // fields
        MappingConfiguration conf = MappingConfiguration.builder()
                .withPropertyMapper(new DefaultPropertyMapper()
                        .setNamingStrategy(new DefaultNamingStrategy(
                                NamingConventions.LOWER_SNAKE_CASE,
                                NamingConventions.UPPER_SNAKE_CASE))
                        .setPropertyAccessStrategy(PropertyAccessStrategy.FIELDS))
                .build();

        MappingManager mappingManager = new MappingManager(session(), conf);

        // when creating a mapper
        // should succeed since fields match the lower snake case strategy and they are appropriately converted for
        // both the table (User) and the underlying UDT (Address)
        Mapper<User> mapper = mappingManager.mapper(User.class);

        // insert user w/ address
        Address address = new Address();
        address.zip_code = 90210;
        address.cityAndState = "Beverly Hills, CA";

        User user = new User();
        user.name = "John Doe";
        user.address = address;

        mapper.save(user);

        // retrieve user w/ address and ensure it matches the input.
        User out = mapper.get(user.name);
        assertThat(out).isEqualTo(user);

        // should be able to use udtCodec to get a codec back that is capable of converting a retrieved
        // udt column value into an Address object and it should match the input.
        TypeCodec<Address> addressCodec = mappingManager.udtCodec(Address.class);
        Row row = session().execute("select \"ADDRESS\" from user where \"NAME\" = 'John Doe'").one();
        Address outAddress = row.get(0, addressCodec);
        assertThat(outAddress).isEqualTo(address);
    }
}
