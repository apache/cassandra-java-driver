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
import com.datastax.driver.core.utils.CassandraVersion;
import com.datastax.driver.mapping.annotations.*;
import org.testng.SkipException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.ProtocolVersion.V1;

/**
 * Tests to ensure validity of {@link com.datastax.driver.mapping.annotations.Computed}
 * annotation to map computed fields.
 */
@SuppressWarnings("unused")
public class MapperComputedFieldsTest extends CCMTestsSupport {

    @Override
    public void onTestContextInitialized() {
        execute(
                "CREATE TABLE user (login text primary key, name text)",
                "INSERT INTO user (login, name) VALUES ('testlogin', 'test name')");
    }

    ProtocolVersion protocolVersion;
    MappingManager mappingManager;
    Mapper<User> userMapper;

    @BeforeMethod(groups = "short")
    void setup() {
        mappingManager = new MappingManager(session());
        protocolVersion = cluster().getConfiguration().getProtocolOptions().getProtocolVersion();
        if (protocolVersion.compareTo(V1) > 0)
            userMapper = mappingManager.mapper(User.class);
    }

    @Test(groups = "short", expectedExceptions = UnsupportedOperationException.class)
    void should_get_unsupported_operation_exception_on_v1() {
        if (protocolVersion.compareTo(V1) > 0)
            throw new SkipException("Skipped when protocol version > V1");

        userMapper = mappingManager.mapper(User.class);
    }

    @Test(groups = "short")
    @CassandraVersion("2.0.0")
    void should_save_and_get_entity_with_computed_fields() {
        long writeTime = System.currentTimeMillis() * 1000;
        User newUser = new User("testlogin2", "blah");
        newUser.setWriteTime(1); // will be ignored

        userMapper.save(newUser);

        User fetched = userMapper.get("testlogin2");
        assertThat(fetched.getLogin()).isEqualTo("testlogin2");
        assertThat(fetched.getName()).isEqualTo("blah");
        // write time should be within 30 seconds.
        assertThat(fetched.getWriteTime()).isGreaterThanOrEqualTo(writeTime).isLessThan(writeTime + 30000000L);
        assertThat(fetched.getTtl()).isNull(); // TTL should be null since it was not set.

        // Overwrite with TTL
        session().execute("insert into user (login, name) values ('testlogin2', 'blah') using TTL 600");
        fetched = userMapper.get("testlogin2");
        assertThat(fetched.getWriteTime()).isGreaterThanOrEqualTo(writeTime).isLessThan(writeTime + 30000000L);
        assertThat(fetched.getTtl()).isBetween(570, 600); // TTL should be within 30 secs.

        // cleanup
        userMapper.delete(newUser);
    }

    @Test(groups = "short")
    @CassandraVersion("2.0.0")
    void should_add_aliases_for_fields_in_select_queries() {
        BoundStatement bs = (BoundStatement) userMapper.getQuery("test");
        assertThat(bs.preparedStatement().getQueryString())
                .contains("SELECT", "login AS col", "name AS col", "writetime(\"name\") AS col");
    }

    @Test(groups = "short")
    @CassandraVersion("2.0.0")
    public void should_map_aliased_resultset_to_objects() {
        Statement getQuery = userMapper.getQuery("testlogin");
        getQuery.setConsistencyLevel(ConsistencyLevel.QUORUM);
        ResultSet rs = session().execute(getQuery);

        Result<User> result = userMapper.map(rs);
        User user = result.one();

        assertThat(user.getLogin()).isEqualTo("testlogin");
    }

    @Test(groups = "short")
    @CassandraVersion("2.0.0")
    void should_map_unaliased_resultset_to_objects() {
        UserAccessor userAccessor = mappingManager.createAccessor(UserAccessor.class);
        ResultSet rs = userAccessor.all();

        Result<User> result = userMapper.map(rs);
        User user = result.one();
        assertThat(user.getLogin()).isEqualTo("testlogin");
        assertThat(user.getWriteTime()).isEqualTo(0);
    }

    @Test(groups = "short", expectedExceptions = CodecNotFoundException.class)
    @CassandraVersion("2.0.0")
    void should_fail_if_computed_field_is_not_right_type() {
        Mapper<User_WrongComputedType> mapper = mappingManager.mapper(User_WrongComputedType.class);

        User_WrongComputedType user = mapper.get("testlogin");
    }

    @Test(groups = "short", expectedExceptions = IllegalArgumentException.class)
    @CassandraVersion("2.0.0")
    void should_fail_if_computed_field_marked_with_column_annotation() {
        mappingManager.mapper(User_WrongAnnotationForComputed.class);
    }

    @Table(name = "user")
    public static class User {
        @PartitionKey
        private String login;
        private String name;

        public User() {
        }

        public User(String login, String name) {
            this.login = login;
            this.name = name;
        }

        // quotes in the column name inserted on purpose
        // to test the alias generation mechanism
        @Computed(value = "writetime(\"name\")")
        long writeTime;

        @Computed("ttl(name)")
        Integer ttl;

        public String getLogin() {
            return login;
        }

        public void setLogin(String login) {
            this.login = login;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public long getWriteTime() {
            return writeTime;
        }

        public void setWriteTime(long writeTime) {
            this.writeTime = writeTime;
        }

        public Integer getTtl() {
            return ttl;
        }

        public void setTtl(Integer ttl) {
            this.ttl = ttl;
        }
    }

    @Accessor
    interface UserAccessor {
        @Query("select * from user")
        ResultSet all();
    }

    @Table(name = "user")
    public static class User_WrongComputedType {
        @PartitionKey
        private String login;
        private String name;

        @Computed(value = "writetime(name)")
        String writeTime;

        public String getLogin() {
            return login;
        }

        public void setLogin(String login) {
            this.login = login;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getWriteTime() {
            return writeTime;
        }

        public void setWriteTime(String writeTime) {
            this.writeTime = writeTime;
        }
    }

    @Table(name = "user")
    public static class User_WrongAnnotationForComputed {
        @PartitionKey
        private String login;
        private String name;

        @Column(name = "writetime(v)")
        long writeTime;

        public User_WrongAnnotationForComputed() {
        }

        public User_WrongAnnotationForComputed(String login, String name) {
            this.login = login;
            this.name = name;
        }

        public String getLogin() {
            return login;
        }

        public void setLogin(String login) {
            this.login = login;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public long getWriteTime() {
            return writeTime;
        }

        public void setWriteTime(long writeTime) {
            this.writeTime = writeTime;
        }
    }
}
