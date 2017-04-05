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
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.MoreFutures.SuccessCallback;
import com.datastax.driver.core.utils.MoreObjects;
import com.datastax.driver.core.utils.UUIDs;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import org.testng.annotations.Test;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import static com.datastax.driver.core.TestUtils.CREATE_KEYSPACE_SIMPLE_FORMAT;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.testng.Assert.fail;

/**
 * Basic tests for the mapping module.
 */
@SuppressWarnings("unused")
public class MapperAsyncTest extends CCMTestsSupport {

    private static final String KEYSPACE = "mapper_async_test_ks";

    private final User paul = new User("Paul", "paul@yahoo.com");

    @Override
    public void onTestContextInitialized() {
        execute(
                String.format(CREATE_KEYSPACE_SIMPLE_FORMAT, KEYSPACE, 1),
                String.format("CREATE TABLE %s.users (user_id uuid PRIMARY KEY, name text, email text)", KEYSPACE)
        );
    }

    /**
     * Validates that when using save, get and deleteAsync using the same executor as the caller that the driver
     * does not block indefinitely in a netty worker thread.
     *
     * @jira_ticket JAVA-1070
     * @test_category queries:async
     * @test_category object_mapper
     */
    @Test(groups = "short")
    public void should_get_query_async_without_blocking() {

        final CountDownLatch latch = new CountDownLatch(1);

        // create a second cluster to perform everything asynchronously,
        // including session initialization
        Cluster cluster2 = register(
                createClusterBuilder()
                        .addContactPoints(getContactPoints())
                        .withPort(ccm().getBinaryPort())
                        .build());

        ListenableFuture<MappingManager> mappingManagerFuture = Futures.transform(
                cluster2.connectAsync(),
                new Function<Session, MappingManager>() {
                    @Override
                    public MappingManager apply(Session session) {
                        return new MappingManager(session);
                    }
                });

        Futures.addCallback(mappingManagerFuture, new SuccessCallback<MappingManager>() {

            @Override
            public void onSuccess(MappingManager manager) {

                final Mapper<User> mapper = manager.mapper(User.class);
                ListenableFuture<Void> saveFuture = mapper.saveAsync(paul);
                Futures.addCallback(saveFuture, new SuccessCallback<Void>() {

                    @Override
                    public void onSuccess(Void result) {
                        ListenableFuture<User> getFuture = mapper.getAsync(paul.getUserId());
                        Futures.addCallback(getFuture, new SuccessCallback<User>() {

                            @Override
                            public void onSuccess(User paul) {

                                Futures.addCallback(mapper.deleteAsync(paul), new SuccessCallback<Void>() {
                                    @Override
                                    public void onSuccess(Void result) {
                                        latch.countDown();
                                    }
                                });
                            }
                        });
                    }
                });
            }
        });

        try {
            Uninterruptibles.awaitUninterruptibly(latch, 5, MINUTES);
        } catch (Exception e) {
            fail("Operation did not complete normally within 5 minutes");
        }
    }

    @Table(name = "users", keyspace = KEYSPACE)
    public static class User {

        @PartitionKey
        @Column(name = "user_id")
        private UUID userId;

        private String name;

        private String email;

        public User() {
        }

        public User(String name, String email) {
            this.userId = UUIDs.random();
            this.name = name;
            this.email = email;
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

        public String getEmail() {
            return email;
        }

        public void setEmail(String email) {
            this.email = email;
        }

        @Override
        public boolean equals(Object other) {
            if (other == null || other.getClass() != this.getClass())
                return false;

            User that = (User) other;
            return MoreObjects.equal(userId, that.userId)
                    && MoreObjects.equal(name, that.name)
                    && MoreObjects.equal(email, that.email);
        }

        @Override
        public int hashCode() {
            return MoreObjects.hashCode(userId, name, email);
        }
    }

}
