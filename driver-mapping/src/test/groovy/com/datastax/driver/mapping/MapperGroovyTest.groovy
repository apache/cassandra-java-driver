/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datastax.driver.mapping

import com.datastax.driver.core.CCMTestsSupport
import com.datastax.driver.mapping.annotations.Column
import com.datastax.driver.mapping.annotations.PartitionKey
import com.datastax.driver.mapping.annotations.Table
import org.testng.annotations.Test

import static org.assertj.core.api.Assertions.assertThat

/**
 * A simple test to check that mapping a Groovy class works as expected.
 *
 * @jira_ticket JAVA-1279
 */
public class MapperGroovyTest extends CCMTestsSupport {

    @Table(name = "users")
    static class User {

        @PartitionKey
        @Column(name = "user_id")
        def UUID userId

        def String name

    }

    @Override
    public void onTestContextInitialized() {
        execute("CREATE TABLE users (user_id uuid PRIMARY KEY, name text)")
    }

    @Test(groups = "short")
    public void should_map_groovy_class() {
        def mapper = new MappingManager(session()).mapper(User)
        def user1 = new User()
        user1.userId = UUID.randomUUID()
        user1.name = "John Doe"
        mapper.save user1
        def user2 = mapper.get user1.userId
        assertThat(user2.userId).isEqualTo(user1.userId)
        assertThat(user2.name).isEqualTo(user1.name)
    }

}

