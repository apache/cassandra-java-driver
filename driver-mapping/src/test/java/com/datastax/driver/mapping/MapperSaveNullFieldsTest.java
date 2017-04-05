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

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.CCMTestsSupport;
import com.datastax.driver.core.utils.CassandraVersion;
import com.datastax.driver.mapping.Mapper.Option;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("unused")
@CassandraVersion("2.1.0")
public class MapperSaveNullFieldsTest extends CCMTestsSupport {

    Mapper<User> mapper;

    @Override
    public void onTestContextInitialized() {
        execute("CREATE TABLE user (login text primary key, name text, phone text)");
    }

    @BeforeMethod(groups = "short")
    public void setup() {
        mapper = new MappingManager(session()).mapper(User.class);
    }

    @Test(groups = "short")
    void should_save_null_fields_if_requested() {
        should_save_null_fields(true, Option.saveNullFields(true));

        mapper.setDefaultSaveOptions(Option.saveNullFields(true));
        should_save_null_fields(true);
    }

    @Test(groups = "short")
    void should_save_null_fields_by_default() {
        should_save_null_fields(true);
    }

    @Test(groups = "short")
    void should_ignore_null_fields_if_requested() {
        should_save_null_fields(false, Option.saveNullFields(false));

        mapper.setDefaultSaveOptions(Option.saveNullFields(false));
        should_save_null_fields(false);
    }

    private void should_save_null_fields(boolean saveExpected, Option... options) {
        // Try different combinations of null fields
        should_save_null_fields(true, true, saveExpected, options);
        should_save_null_fields(true, false, saveExpected, options);
        should_save_null_fields(false, true, saveExpected, options);
        should_save_null_fields(false, false, saveExpected, options);
    }

    private void should_save_null_fields(boolean nullName, boolean nullPhone, boolean saveExpected, Option... options) {
        // Start with clean data
        session().execute("insert into user(login, name, phone) "
                + "values ('test_login', 'previous_name', 'previous_phone')");

        String newName = nullName ? null : "new_name";
        String newPhone = nullPhone ? null : "new_phone";
        String description = String.format("update with name=%s, phone = %s", newName, newPhone);
        User newUser = new User("test_login", newName, newPhone);

        // Check if null fields are included in generated statement:
        BoundStatement bs = (BoundStatement) mapper.saveQuery(newUser, options);
        String queryString = bs.preparedStatement().getQueryString();
        if (nullName && !saveExpected)
            assertThat(queryString).as(description).doesNotContain("\"name\"");
        else
            assertThat(queryString).as(description).contains("\"name\"");

        if (nullPhone && !saveExpected)
            assertThat(queryString).as(description).doesNotContain("\"phone\"");
        else
            assertThat(queryString).as(description).contains("\"phone\"");

        // Save entity and check the data
        mapper.save(newUser, options);
        User savedUser = mapper.get("test_login");
        String expectedName = nullName
                ? (saveExpected ? null : "previous_name")
                : "new_name";
        String expectedPhone = nullPhone
                ? (saveExpected ? null : "previous_phone")
                : "new_phone";
        assertThat(savedUser.getName()).as(description).isEqualTo(expectedName);
        assertThat(savedUser.getPhone()).as(description).isEqualTo(expectedPhone);
    }

    @Table(name = "user")
    public static class User {
        @PartitionKey
        private String login;
        private String name;
        private String phone;

        public User() {
        }

        public User(String login, String name, String phone) {
            this.login = login;
            this.name = name;
            this.phone = phone;
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

        public String getPhone() {
            return phone;
        }

        public void setPhone(String phone) {
            this.phone = phone;
        }
    }
}
