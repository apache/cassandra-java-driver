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

import com.datastax.driver.core.CCMConfig;
import com.datastax.driver.core.CCMTestsSupport;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CreateCCM;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import org.testng.annotations.Test;

import static com.datastax.driver.core.CreateCCM.TestMode.PER_METHOD;
import static com.datastax.driver.core.TestUtils.*;
import static com.datastax.driver.mapping.MapperTest.User;
import static org.testng.Assert.fail;

@CreateCCM(PER_METHOD)
@CCMConfig(dirtiesContext = true)
public class MapperReconnectionTest extends CCMTestsSupport {

    @Override
    public Cluster.Builder createClusterBuilder() {
        return super.createClusterBuilder().withReconnectionPolicy(new ConstantReconnectionPolicy(1000));
    }

    @Override
    public void onTestContextInitialized() {
        execute("CREATE TABLE users (user_id uuid PRIMARY KEY, name text, email text, year int, gender text)");
    }

    /**
     * Ensures that when the driver looses connectivity,
     * if a mapper query preparation is attempted in the meanwhile,
     * the failed future will not be kept in cache, so that when
     * connectivity comes back again,
     * the same query preparation can be reattempted.
     *
     * @jira_ticket JAVA-1283
     * @test_category object_mapper
     */
    @Test(groups = "long")
    public void should_not_keep_failed_future_in_query_cache() throws Exception {
        MappingManager manager = new MappingManager(session());
        Mapper<User> m = manager.mapper(User.class);
        User u1 = new User("Paul", "paul@gmail.com");
        m.save(u1);
        ccm().stop(1);
        ccm().waitForDown(1);
        waitForDown(ipOfNode(1), cluster());
        try {
            m.get(u1.getUserId());
            fail("Should have thrown NoHostAvailableException");
        } catch (NoHostAvailableException e) {
            // ok
        }
        ccm().start(1);
        ccm().waitForUp(1);
        waitForUp(ipOfNode(1), cluster());
        try {
            m.get(u1.getUserId());
        } catch (NoHostAvailableException e) {
            fail("Should not have thrown NoHostAvailableException");
        }
    }

}
