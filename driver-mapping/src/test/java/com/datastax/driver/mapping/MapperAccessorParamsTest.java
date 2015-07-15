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

import java.util.Collection;

import com.datastax.driver.core.ResultSet;
import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.CCMBridge;
import com.datastax.driver.mapping.annotations.*;

public class MapperAccessorParamsTest extends CCMBridge.PerClassSingleNodeCluster {
    @Override
    protected Collection<String> getTableDefinitions() {
        return Lists.newArrayList(
                "CREATE TABLE user ( key int primary key, gender int)",
                "CREATE INDEX on user(gender)",
                "CREATE TABLE user_str ( key int primary key, gender text)",
                "CREATE INDEX on user_str(gender)");
    }

    @Test(groups = "short")
    void should_include_enumtype_in_accessor_ordinal() {
        UserAccessor accessor = new MappingManager(session)
                .createAccessor(UserAccessor.class);

        accessor.addUser(0, Enum.FEMALE);
        accessor.addUser(1, Enum.MALE);

        assertThat(accessor.getUser(Enum.MALE).one().getGender()).isEqualTo(Enum.MALE);
        assertThat(accessor.getUser(Enum.MALE).one().getKey()).isEqualTo(1);

        assertThat(accessor.getUser(Enum.FEMALE).one().getGender()).isEqualTo(Enum.FEMALE);
        assertThat(accessor.getUser(Enum.FEMALE).one().getKey()).isEqualTo(0);
    }

    @Test(groups = "short")
    void should_include_enumtype_in_accessor_string() {
        UserAccessor accessor = new MappingManager(session)
                .createAccessor(UserAccessor.class);

        accessor.addUserStr(0, Enum.FEMALE);
        accessor.addUserStr(1, Enum.MALE);

        assertThat(accessor.getUserStr(Enum.MALE).one().getGender()).isEqualTo(Enum.MALE);
        assertThat(accessor.getUserStr(Enum.MALE).one().getKey()).isEqualTo(1);

        assertThat(accessor.getUserStr(Enum.FEMALE).one().getGender()).isEqualTo(Enum.FEMALE);
        assertThat(accessor.getUserStr(Enum.FEMALE).one().getKey()).isEqualTo(0);
    }

    enum Enum {
        MALE, FEMALE
    }

    @Accessor
    public interface UserAccessor {
        @Query("select * from user where gender=?")
        Result<User> getUser(@Enumerated(EnumType.ORDINAL) Enum value);

        @Query("select * from user_str where gender=?")
        Result<UserStr> getUserStr(@Enumerated(EnumType.STRING) Enum value);

        @Query("insert into user (key, gender) values (?,?)")
        ResultSet addUser(int key, @Enumerated(EnumType.ORDINAL) Enum value);

        @Query("insert into user_str (key, gender) values (?,?)")
        ResultSet addUserStr(int key, @Enumerated(EnumType.STRING) Enum value);
    }

    @Table(name = "user")
    public static class User {
        @PartitionKey
        private int key;

        @Enumerated(EnumType.ORDINAL)
        private Enum gender;

        public User() {
        }

        public User(int k, Enum val) {
            this.key = k;
            this.gender = val;
        }

        public int getKey() {
            return this.key;
        }

        public void setKey(int pk) {
            this.key = pk;
        }

        public Enum getGender() {
            return this.gender;
        }

        public void setGender(Enum val) {
            this.gender = val;
        }
    }

    @Table(name = "user")
    public static class UserStr {
        @PartitionKey
        private int key;

        private Enum gender;

        public UserStr() {
        }

        public UserStr(int k, Enum val) {
            this.key = k;
            this.gender = val;
        }

        public int getKey() {
            return this.key;
        }

        public void setKey(int pk) {
            this.key = pk;
        }

        public Enum getGender() {
            return this.gender;
        }

        public void setGender(Enum val) {
            this.gender = val;
        }
    }
}