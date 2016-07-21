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

import com.datastax.driver.core.CCMTestsSupport;
import com.datastax.driver.core.Statement;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("unused")
public class MapperIfNotExistsTest extends CCMTestsSupport {

    Mapper<User> mapper;

    @Override
    public void onTestContextInitialized() {
        execute("CREATE TABLE user (login text primary key, v int)");
    }

    @BeforeMethod(groups = "short")
    public void setup() {
        mapper = new MappingManager(session()).mapper(User.class);
    }

    @Test(groups = "short")
    void should_not_insert_if_not_exists_specified() {
        insertAndVerifyApplied("user1", 1, true, true);
        insertAndVerifyApplied("user1", 2, true, false);
        assertThat(mapper.get("user1").v).isEqualTo(1);
    }

    @Test(groups = "short")
    void should_insert_if_not_exist_not_specified() {
        insertAndVerifyApplied("user1", 1, false, true);
        insertAndVerifyApplied("user1", 2, false, true);
        assertThat(mapper.get("user1").v).isEqualTo(2);
    }

    private void insertAndVerifyApplied(String username, int v, boolean ifNotExists, boolean expectedWasApplied) {
        User user = new User(username, v);
        List<Mapper.Option> options = new ArrayList<Mapper.Option>();
        Statement statement = mapper.saveQuery(user, Mapper.Option.ifNotExists(ifNotExists));
        assertThat(session().execute(statement).wasApplied()).isEqualTo(expectedWasApplied);
    }


    @Table(name = "user")
    public static class User {
        @PartitionKey
        private String login;
        private int v;

        public User() {
        }

        public User(String login, int v) {
            this.login = login;
            this.v = v;
        }

        public String getLogin() {
            return login;
        }

        public void setLogin(String login) {
            this.login = login;
        }

        public int getV() {
            return v;
        }

        public void setV(int v) {
            this.v = v;
        }
    }
}
