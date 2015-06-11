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

import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.CCMBridge;
import com.datastax.driver.mapping.annotations.*;

public class MapperAccessorParamsTest extends CCMBridge.PerClassSingleNodeCluster {
    @Override
    protected Collection<String> getTableDefinitions() {
        return Lists.newArrayList("CREATE TABLE user ( key int primary key, gender int)",
            "CREATE INDEX on user(gender)");

    }

    @Test(groups = "short")
    void should_include_enumtype_in_accessor() {
        Mapper<User> userMapper = new MappingManager(session).mapper(User.class);
        userMapper.save(new User(1, 0));
        userMapper.save(new User(2, 1));

        UserAccessor accessor = new MappingManager(session)
            .createAccessor(UserAccessor.class);
        assertThat(accessor.getUser(Enum.MALE).one().getGender()).isEqualTo(0);
        assertThat(accessor.getUser(Enum.MALE).one().getKey()).isEqualTo(1);

    }

    enum Enum {
        MALE, FEMALE
    }

    @Accessor
    public interface UserAccessor {
        @Query("select * from user where gender=:val")
        Result<User> getUser(@Param("val") @Enumerated(EnumType.ORDINAL) Enum value);
    }

    @Table(name = "user")
    public static class User {
        @PartitionKey
        private int key;

        private int gender;

        public User() {
        }

        public User(int k, int val) {
            this.key = k;
            this.gender = val;
        }

        public int getKey() {
            return this.key;
        }

        public void setKey(int pk) {
            this.key = pk;
        }

        public int getGender() {
            return this.gender;
        }

        public void setGender(int val) {
            this.gender = val;
        }
    }
}