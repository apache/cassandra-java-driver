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
package com.datastax.driver.core;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class ReplicationStrategyTest {

    @Test(groups = "unit")
    public void createSimpleReplicationStrategyTest() throws Exception {
        ReplicationStrategy strategy = ReplicationStrategy.create(
                ImmutableMap.<String, String>builder()
                        .put("class", "SimpleStrategy")
                        .put("replication_factor", "3")
                        .build());

        assertNotNull(strategy);
        assertTrue(strategy instanceof ReplicationStrategy.SimpleStrategy);
    }

    @Test(groups = "unit")
    public void createNetworkTopologyStrategyTest() throws Exception {
        ReplicationStrategy strategy = ReplicationStrategy.create(
                ImmutableMap.<String, String>builder()
                        .put("class", "NetworkTopologyStrategy")
                        .put("dc1", "2")
                        .put("dc2", "2")
                        .build());

        assertNotNull(strategy);
        assertTrue(strategy instanceof ReplicationStrategy.NetworkTopologyStrategy);
    }

    @Test(groups = "unit")
    public void createSimpleReplicationStrategyWithoutFactorTest() throws Exception {
        ReplicationStrategy strategy = ReplicationStrategy.create(
                ImmutableMap.<String, String>builder()
                        .put("class", "SimpleStrategy")
                                //no replication_factor
                        .build());

        assertNull(strategy);
    }

    @Test(groups = "unit")
    public void createUnknownStrategyTest() throws Exception {
        ReplicationStrategy strategy = ReplicationStrategy.create(
                ImmutableMap.<String, String>builder()
                        //no such strategy
                        .put("class", "FooStrategy")
                        .put("foo_factor", "3")
                        .build());

        assertNull(strategy);
    }

    @Test(groups = "unit")
    public void createUnspecifiedStrategyTest() throws Exception {
        ReplicationStrategy strategy = ReplicationStrategy.create(
                ImmutableMap.<String, String>builder()
                        //nothing useful is set
                        .put("foo", "bar")
                        .build());

        assertNull(strategy);
    }
}
