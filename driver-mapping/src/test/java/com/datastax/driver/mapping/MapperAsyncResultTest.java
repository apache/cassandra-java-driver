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
import com.datastax.driver.core.GuavaCompatibility;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.utils.CassandraVersion;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that a Result can be paged asynchronously.
 *
 * @jira_ticket JAVA-1157
 */
@SuppressWarnings("unused")
@CassandraVersion(value = "2.0", description = "uses paging")
public class MapperAsyncResultTest extends CCMTestsSupport {

    @Override
    public void onTestContextInitialized() {
        execute("CREATE TABLE users (id int PRIMARY KEY, name text)");
    }

    @Table(name = "users")
    static class User {

        @PartitionKey
        private int id;

        private String name;

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

    }

    @BeforeMethod(groups = "short")
    public void cleanup() {
        session().execute("TRUNCATE users");
    }

    @Test(groups = "short")
    public void should_iterate_single_page_result_set_asynchronously() {
        should_iterate_result_set_asynchronously(100, 500);
    }

    @Test(groups = "short")
    public void should_iterate_multi_page_result_set_asynchronously() {
        should_iterate_result_set_asynchronously(1000, 20);
    }

    private void should_iterate_result_set_asynchronously(int totalCount, int fetchSize) {
        for (int i = 0; i < totalCount; i++)
            session().execute(String.format("INSERT INTO users (id, name) values (%d, '%s')", i, "user" + i));
        Statement statement = new SimpleStatement("SELECT * FROM users").setFetchSize(fetchSize);
        Mapper<User> mapper = new MappingManager(session()).mapper(User.class);
        ResultsAccumulator accumulator = new ResultsAccumulator();
        ListenableFuture<Result<User>> results = mapper.mapAsync(session().executeAsync(statement));
        ListenableFuture<Result<User>> future = GuavaCompatibility.INSTANCE.transformAsync(
                results,
                accumulator);
        Futures.getUnchecked(future);
        assertThat(accumulator.all.size()).isEqualTo(totalCount);
    }

    /**
     * Dummy transformation that accumulates all traversed results
     */
    static class ResultsAccumulator implements AsyncFunction<Result<User>, Result<User>> {

        final Set<Integer> all = new ConcurrentSkipListSet<Integer>();

        @Override
        public ListenableFuture<Result<User>> apply(Result<User> users) throws Exception {
            int remainingInPage = users.getAvailableWithoutFetching();
            for (User user : users) {
                all.add(user.getId());
                if (--remainingInPage == 0)
                    break;
            }
            boolean wasLastPage = users.getExecutionInfo().getPagingState() == null;
            if (wasLastPage)
                return Futures.immediateFuture(users);
            else
                return GuavaCompatibility.INSTANCE.transformAsync(users.fetchMoreResults(), this);
        }
    }
}
