/*
 *      Copyright (C) 2012 DataStax Inc.
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
package com.datastax.driver.core;

import java.util.*;

import org.testng.annotations.Test;

import static org.testng.Assert.*;

import com.datastax.driver.core.exceptions.*;
import com.datastax.driver.core.policies.*;
import static com.datastax.driver.core.TestUtils.*;

public class ConsistencyTest extends AbstractPoliciesTest {

    @Test(groups = "long")
    public void testRFOneTokenAware() throws Throwable {
        Cluster.Builder builder = Cluster.builder().withLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
        CCMBridge.CCMCluster c = CCMBridge.buildCluster(3, builder);
        try {

            createSchema(c.session, 1);
            init(c, 12, ConsistencyLevel.ONE);
            query(c, 12, ConsistencyLevel.ONE);

            assertQueried(CCMBridge.IP_PREFIX + "1", 0);
            assertQueried(CCMBridge.IP_PREFIX + "2", 12);
            assertQueried(CCMBridge.IP_PREFIX + "3", 0);

            resetCoordinators();
            c.cassandraCluster.forceStop(2);
            waitForDownWithWait(CCMBridge.IP_PREFIX + "2", c.cluster, 5);

            List<ConsistencyLevel> acceptedList = Arrays.asList(
                                                    ConsistencyLevel.ANY
                                                  );

            List<ConsistencyLevel> failList = Arrays.asList(
                                                    ConsistencyLevel.ONE,
                                                    ConsistencyLevel.TWO,
                                                    ConsistencyLevel.THREE,
                                                    ConsistencyLevel.QUORUM,
                                                    ConsistencyLevel.ALL,
                                                    ConsistencyLevel.LOCAL_QUORUM,
                                                    ConsistencyLevel.EACH_QUORUM);

            // Test successful writes
            for (ConsistencyLevel cl : acceptedList) {
                try {
                    init(c, 12, cl);
                } catch (Exception e) {
                    fail(String.format("Test failed at CL.%s with message: %s", cl, e.getMessage()));
                }
            }

            // Test successful reads
            for (ConsistencyLevel cl : acceptedList) {
                try {
                    query(c, 12, cl);
                } catch (InvalidQueryException e) {
                    List<String> acceptableErrorMessages = Arrays.asList(
                        "ANY ConsistencyLevel is only supported for writes");
                    assertTrue(acceptableErrorMessages.contains(e.getMessage()));
                }
            }

            // Test writes which should fail
            for (ConsistencyLevel cl : failList) {
                try {
                    init(c, 12, cl);
                    fail(String.format("Test passed at CL.%s.", cl));
                } catch (InvalidQueryException e) {
                    List<String> acceptableErrorMessages = Arrays.asList(
                        "consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)",
                        "consistency level EACH_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)");
                    assertTrue(acceptableErrorMessages.contains(e.getMessage()), String.format("Received: %s", e.getMessage()));
                } catch (UnavailableException e) {
                    // expected to fail when the client has already marked the
                    // node as DOWN
                } catch (WriteTimeoutException e) {
                    // expected to fail when the client hasn't marked the
                    // node as DOWN yet
                }
            }

            // Test reads which should fail
            for (ConsistencyLevel cl : failList) {
                try {
                    query(c, 12, cl);
                    fail(String.format("Test passed at CL.%s.", cl));
                } catch (InvalidQueryException e) {
                    List<String> acceptableErrorMessages = Arrays.asList(
                        "consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)",
                        "EACH_QUORUM ConsistencyLevel is only supported for writes");
                    assertTrue(acceptableErrorMessages.contains(e.getMessage()), String.format("Received: %s", e.getMessage()));
                } catch (ReadTimeoutException e) {
                    // expected to fail when the client hasn't marked the
                    // node as DOWN yet
                } catch (UnavailableException e) {
                    // expected to fail when the client has already marked the
                    // node as DOWN
                }
            }

        } catch (Throwable e) {
            c.errorOut();
            throw e;
        } finally {
            resetCoordinators();
            c.discard();
        }
    }

    @Test(groups = "long")
    public void testRFTwoTokenAware() throws Throwable {
        Cluster.Builder builder = Cluster.builder().withLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
        CCMBridge.CCMCluster c = CCMBridge.buildCluster(3, builder);
        try {

            createSchema(c.session, 2);
            init(c, 12, ConsistencyLevel.TWO);
            query(c, 12, ConsistencyLevel.TWO);

            assertQueried(CCMBridge.IP_PREFIX + "1", 0);
            assertQueried(CCMBridge.IP_PREFIX + "2", 12);
            assertQueried(CCMBridge.IP_PREFIX + "3", 0);

            resetCoordinators();
            c.cassandraCluster.forceStop(2);
            waitForDownWithWait(CCMBridge.IP_PREFIX + "2", c.cluster, 5);

            List<ConsistencyLevel> acceptedList = Arrays.asList(
                                                    ConsistencyLevel.ANY,
                                                    ConsistencyLevel.ONE
                                                  );

            List<ConsistencyLevel> failList = Arrays.asList(
                                                    ConsistencyLevel.TWO,
                                                    ConsistencyLevel.QUORUM,
                                                    ConsistencyLevel.THREE,
                                                    ConsistencyLevel.ALL,
                                                    ConsistencyLevel.LOCAL_QUORUM,
                                                    ConsistencyLevel.EACH_QUORUM);

            // Test successful writes
            for (ConsistencyLevel cl : acceptedList) {
                try {
                    init(c, 12, cl);
                } catch (Exception e) {
                    fail(String.format("Test failed at CL.%s with message: %s", cl, e.getMessage()));
                }
            }

            // Test successful reads
            for (ConsistencyLevel cl : acceptedList) {
                try {
                    query(c, 12, cl);
                } catch (InvalidQueryException e) {
                    List<String> acceptableErrorMessages = Arrays.asList(
                        "ANY ConsistencyLevel is only supported for writes");
                    assertTrue(acceptableErrorMessages.contains(e.getMessage()));
                }
            }

            // Test writes which should fail
            for (ConsistencyLevel cl : failList) {
                try {
                    init(c, 12, cl);
                    fail(String.format("Test passed at CL.%s.", cl));
                } catch (InvalidQueryException e) {
                    List<String> acceptableErrorMessages = Arrays.asList(
                        "consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)",
                        "consistency level EACH_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)");
                    assertTrue(acceptableErrorMessages.contains(e.getMessage()), String.format("Received: %s", e.getMessage()));
                } catch (UnavailableException e) {
                    // expected to fail when the client has already marked the
                    // node as DOWN
                } catch (WriteTimeoutException e) {
                    // expected to fail when the client hasn't marked the
                    // node as DOWN yet
                }
            }

            // Test reads which should fail
            for (ConsistencyLevel cl : failList) {
                try {
                    query(c, 12, cl);
                    fail(String.format("Test passed at CL.%s.", cl));
                } catch (InvalidQueryException e) {
                    List<String> acceptableErrorMessages = Arrays.asList(
                        "consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)",
                        "EACH_QUORUM ConsistencyLevel is only supported for writes");
                    assertTrue(acceptableErrorMessages.contains(e.getMessage()), String.format("Received: %s", e.getMessage()));
                } catch (ReadTimeoutException e) {
                    // expected to fail when the client hasn't marked the
                    // node as DOWN yet
                } catch (UnavailableException e) {
                    // expected to fail when the client has already marked the
                    // node as DOWN
                }
            }

        } catch (Throwable e) {
            c.errorOut();
            throw e;
        } finally {
            resetCoordinators();
            c.discard();
        }
    }

    @Test(groups = "long")
    public void testRFThreeTokenAware() throws Throwable {
        Cluster.Builder builder = Cluster.builder().withLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
        CCMBridge.CCMCluster c = CCMBridge.buildCluster(3, builder);
        try {

            createSchema(c.session, 3);
            init(c, 12, ConsistencyLevel.TWO);
            query(c, 12, ConsistencyLevel.TWO);

            assertQueried(CCMBridge.IP_PREFIX + "1", 0);
            assertQueried(CCMBridge.IP_PREFIX + "2", 12);
            assertQueried(CCMBridge.IP_PREFIX + "3", 0);

            resetCoordinators();
            c.cassandraCluster.forceStop(2);
            waitForDownWithWait(CCMBridge.IP_PREFIX + "2", c.cluster, 5);

            List<ConsistencyLevel> acceptedList = Arrays.asList(
                                                    ConsistencyLevel.ANY,
                                                    ConsistencyLevel.ONE,
                                                    ConsistencyLevel.TWO,
                                                    ConsistencyLevel.QUORUM
                                                  );

            List<ConsistencyLevel> failList = Arrays.asList(
                                                    ConsistencyLevel.THREE,
                                                    ConsistencyLevel.ALL,
                                                    ConsistencyLevel.LOCAL_QUORUM,
                                                    ConsistencyLevel.EACH_QUORUM);

            // Test successful writes
            for (ConsistencyLevel cl : acceptedList) {
                try {
                    init(c, 12, cl);
                } catch (Exception e) {
                    fail(String.format("Test failed at CL.%s with message: %s", cl, e.getMessage()));
                }
            }

            // Test successful reads
            for (ConsistencyLevel cl : acceptedList) {
                try {
                    query(c, 12, cl);
                } catch (InvalidQueryException e) {
                    List<String> acceptableErrorMessages = Arrays.asList(
                        "ANY ConsistencyLevel is only supported for writes");
                    assertTrue(acceptableErrorMessages.contains(e.getMessage()));
                }
            }

            // Test writes which should fail
            for (ConsistencyLevel cl : failList) {
                try {
                    init(c, 12, cl);
                    fail(String.format("Test passed at CL.%s.", cl));
                } catch (InvalidQueryException e) {
                    List<String> acceptableErrorMessages = Arrays.asList(
                        "consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)",
                        "consistency level EACH_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)");
                    assertTrue(acceptableErrorMessages.contains(e.getMessage()), String.format("Received: %s", e.getMessage()));
                } catch (UnavailableException e) {
                    // expected to fail when the client has already marked the
                    // node as DOWN
                } catch (WriteTimeoutException e) {
                    // expected to fail when the client hasn't marked the
                    // node as DOWN yet
                }
            }

            // Test reads which should fail
            for (ConsistencyLevel cl : failList) {
                try {
                    query(c, 12, cl);
                    fail(String.format("Test passed at CL.%s.", cl));
                } catch (InvalidQueryException e) {
                    List<String> acceptableErrorMessages = Arrays.asList(
                        "consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)",
                        "EACH_QUORUM ConsistencyLevel is only supported for writes");
                    assertTrue(acceptableErrorMessages.contains(e.getMessage()), String.format("Received: %s", e.getMessage()));
                } catch (ReadTimeoutException e) {
                    // expected to fail when the client hasn't marked the
                    // node as DOWN yet
                } catch (UnavailableException e) {
                    // expected to fail when the client has already marked the
                    // node as DOWN
                }
            }

        } catch (Throwable e) {
            c.errorOut();
            throw e;
        } finally {
            resetCoordinators();
            c.discard();
        }
    }

    @Test(groups = "long")
    public void testRFOneDowngradingCL() throws Throwable {
        Cluster.Builder builder = Cluster.builder().withLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy())).withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE);
        CCMBridge.CCMCluster c = CCMBridge.buildCluster(3, builder);
        try {

            createSchema(c.session, 1);
            init(c, 12, ConsistencyLevel.ONE);
            query(c, 12, ConsistencyLevel.ONE);

            assertQueried(CCMBridge.IP_PREFIX + "1", 0);
            assertQueried(CCMBridge.IP_PREFIX + "2", 12);
            assertQueried(CCMBridge.IP_PREFIX + "3", 0);

            resetCoordinators();
            c.cassandraCluster.forceStop(2);
            waitForDownWithWait(CCMBridge.IP_PREFIX + "2", c.cluster, 5);

            List<ConsistencyLevel> acceptedList = Arrays.asList(
                                                    ConsistencyLevel.ANY
                                                  );

            List<ConsistencyLevel> failList = Arrays.asList(
                                                    ConsistencyLevel.ONE,
                                                    ConsistencyLevel.TWO,
                                                    ConsistencyLevel.THREE,
                                                    ConsistencyLevel.QUORUM,
                                                    ConsistencyLevel.ALL,
                                                    ConsistencyLevel.LOCAL_QUORUM,
                                                    ConsistencyLevel.EACH_QUORUM);

            // Test successful writes
            for (ConsistencyLevel cl : acceptedList) {
                try {
                    init(c, 12, cl);
                } catch (Exception e) {
                    fail(String.format("Test failed at CL.%s with message: %s", cl, e.getMessage()));
                }
            }

            // Test successful reads
            for (ConsistencyLevel cl : acceptedList) {
                try {
                    query(c, 12, cl);
                } catch (InvalidQueryException e) {
                    List<String> acceptableErrorMessages = Arrays.asList(
                        "ANY ConsistencyLevel is only supported for writes");
                    assertTrue(acceptableErrorMessages.contains(e.getMessage()));
                }
            }

            // Test writes which should fail
            for (ConsistencyLevel cl : failList) {
                try {
                    init(c, 12, cl);
                    fail(String.format("Test passed at CL.%s.", cl));
                } catch (InvalidQueryException e) {
                    List<String> acceptableErrorMessages = Arrays.asList(
                        "consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)",
                        "consistency level EACH_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)");
                    assertTrue(acceptableErrorMessages.contains(e.getMessage()), String.format("Received: %s", e.getMessage()));
                } catch (UnavailableException e) {
                    // expected to fail when the client has already marked the
                    // node as DOWN
                } catch (WriteTimeoutException e) {
                    // expected to fail when the client hasn't marked the
                    // node as DOWN yet
                }
            }

            // Test reads which should fail
            for (ConsistencyLevel cl : failList) {
                try {
                    query(c, 12, cl);
                    fail(String.format("Test passed at CL.%s.", cl));
                } catch (InvalidQueryException e) {
                    List<String> acceptableErrorMessages = Arrays.asList(
                        "consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)",
                        "EACH_QUORUM ConsistencyLevel is only supported for writes");
                    assertTrue(acceptableErrorMessages.contains(e.getMessage()), String.format("Received: %s", e.getMessage()));
                } catch (ReadTimeoutException e) {
                    // expected to fail when the client hasn't marked the
                    // node as DOWN yet
                } catch (UnavailableException e) {
                    // expected to fail when the client has already marked the
                    // node as DOWN
                }
            }

        } catch (Throwable e) {
            c.errorOut();
            throw e;
        } finally {
            resetCoordinators();
            c.discard();
        }
    }

    @Test(groups = "long")
    public void testRFTwoDowngradingCL() throws Throwable {
        Cluster.Builder builder = Cluster.builder().withLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy())).withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE);
        CCMBridge.CCMCluster c = CCMBridge.buildCluster(3, builder);
        try {

            createSchema(c.session, 2);
            init(c, 12, ConsistencyLevel.TWO);
            query(c, 12, ConsistencyLevel.TWO);

            assertQueried(CCMBridge.IP_PREFIX + "1", 0);
            assertQueried(CCMBridge.IP_PREFIX + "2", 12);
            assertQueried(CCMBridge.IP_PREFIX + "3", 0);

            resetCoordinators();
            c.cassandraCluster.forceStop(2);
            // FIXME: This sleep is needed to allow the waitFor() to work
            Thread.sleep(20000);
            waitForDownWithWait(CCMBridge.IP_PREFIX + "2", c.cluster, 5);

            List<ConsistencyLevel> acceptedList = Arrays.asList(
                                                    ConsistencyLevel.ANY,
                                                    ConsistencyLevel.ONE,
                                                    ConsistencyLevel.TWO,
                                                    ConsistencyLevel.QUORUM,
                                                    ConsistencyLevel.THREE,
                                                    ConsistencyLevel.ALL
                                                  );

            List<ConsistencyLevel> failList = Arrays.asList(
                                                    ConsistencyLevel.LOCAL_QUORUM,
                                                    ConsistencyLevel.EACH_QUORUM);

            // Test successful writes
            for (ConsistencyLevel cl : acceptedList) {
                try {
                    init(c, 12, cl);
                } catch (Exception e) {
                    fail(String.format("Test failed at CL.%s with message: %s", cl, e.getMessage()));
                }
            }

            // Test successful reads
            for (ConsistencyLevel cl : acceptedList) {
                try {
                    query(c, 12, cl);
                } catch (InvalidQueryException e) {
                    List<String> acceptableErrorMessages = Arrays.asList(
                        "ANY ConsistencyLevel is only supported for writes");
                    assertTrue(acceptableErrorMessages.contains(e.getMessage()));
                }
            }

            // Test writes which should fail
            for (ConsistencyLevel cl : failList) {
                try {
                    init(c, 12, cl);
                    fail(String.format("Test passed at CL.%s.", cl));
                } catch (InvalidQueryException e) {
                    List<String> acceptableErrorMessages = Arrays.asList(
                        "consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)",
                        "consistency level EACH_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)");
                    assertTrue(acceptableErrorMessages.contains(e.getMessage()), String.format("Received: %s", e.getMessage()));
                } catch (UnavailableException e) {
                    // expected to fail when the client has already marked the
                    // node as DOWN
                } catch (WriteTimeoutException e) {
                    // expected to fail when the client hasn't marked the
                    // node as DOWN yet
                }
            }

            // Test reads which should fail
            for (ConsistencyLevel cl : failList) {
                try {
                    query(c, 12, cl);
                    fail(String.format("Test passed at CL.%s.", cl));
                } catch (InvalidQueryException e) {
                    List<String> acceptableErrorMessages = Arrays.asList(
                        "consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)",
                        "EACH_QUORUM ConsistencyLevel is only supported for writes");
                    assertTrue(acceptableErrorMessages.contains(e.getMessage()), String.format("Received: %s", e.getMessage()));
                } catch (ReadTimeoutException e) {
                    // expected to fail when the client hasn't marked the
                    // node as DOWN yet
                } catch (UnavailableException e) {
                    // expected to fail when the client has already marked the
                    // node as DOWN
                }
            }

        } catch (Throwable e) {
            c.errorOut();
            throw e;
        } finally {
            resetCoordinators();
            c.discard();
        }
    }

    @Test(groups = "long")
    public void testRFThreeRoundRobinDowngradingCL() throws Throwable {
        Cluster.Builder builder = Cluster.builder().withLoadBalancingPolicy(new RoundRobinPolicy()).withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE);
        testRFThreeDowngradingCL(builder);
    }

    @Test(groups = "long")
    public void testRFThreeTokenAwareDowngradingCL() throws Throwable {
        Cluster.Builder builder = Cluster.builder().withLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy())).withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE);
        testRFThreeDowngradingCL(builder);
    }

    public void testRFThreeDowngradingCL(Cluster.Builder builder) throws Throwable {
        CCMBridge.CCMCluster c = CCMBridge.buildCluster(3, builder);
        try {

            createSchema(c.session, 3);
            init(c, 12, ConsistencyLevel.ALL);
            query(c, 12, ConsistencyLevel.ALL);

            try {
                // This test catches TokenAwarePolicy
                // However, full tests in LoadBalancingPolicyTest.java
                assertQueried(CCMBridge.IP_PREFIX + "1", 0);
                assertQueried(CCMBridge.IP_PREFIX + "2", 12);
                assertQueried(CCMBridge.IP_PREFIX + "3", 0);
            } catch (AssertionError e) {
                // This test catches RoundRobinPolicy
                assertQueried(CCMBridge.IP_PREFIX + "1", 4);
                assertQueried(CCMBridge.IP_PREFIX + "2", 4);
                assertQueried(CCMBridge.IP_PREFIX + "3", 4);
            }

            resetCoordinators();
            c.cassandraCluster.forceStop(2);
            waitForDownWithWait(CCMBridge.IP_PREFIX + "2", c.cluster, 5);

            List<ConsistencyLevel> acceptedList = Arrays.asList(
                                                    ConsistencyLevel.ANY,
                                                    ConsistencyLevel.ONE,
                                                    ConsistencyLevel.TWO,
                                                    ConsistencyLevel.QUORUM,
                                                    ConsistencyLevel.THREE,
                                                    ConsistencyLevel.ALL
                                                  );

            List<ConsistencyLevel> failList = Arrays.asList(
                                                    ConsistencyLevel.LOCAL_QUORUM,
                                                    ConsistencyLevel.EACH_QUORUM);

            // Test successful writes
            for (ConsistencyLevel cl : acceptedList) {
                try {
                    init(c, 12, cl);
                } catch (Exception e) {
                    fail(String.format("Test failed at CL.%s with message: %s", cl, e.getMessage()));
                }
            }

            // Test successful reads
            for (ConsistencyLevel cl : acceptedList) {
                try {
                    query(c, 12, cl);
                } catch (InvalidQueryException e) {
                    List<String> acceptableErrorMessages = Arrays.asList(
                        "ANY ConsistencyLevel is only supported for writes");
                    assertTrue(acceptableErrorMessages.contains(e.getMessage()));
                }
            }

            // Test writes which should fail
            for (ConsistencyLevel cl : failList) {
                try {
                    init(c, 12, cl);
                    fail(String.format("Test passed at CL.%s.", cl));
                } catch (InvalidQueryException e) {
                    List<String> acceptableErrorMessages = Arrays.asList(
                        "consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)",
                        "consistency level EACH_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)");
                    assertTrue(acceptableErrorMessages.contains(e.getMessage()), String.format("Received: %s", e.getMessage()));
                } catch (UnavailableException e) {
                    // expected to fail when the client has already marked the
                    // node as DOWN
                } catch (WriteTimeoutException e) {
                    // expected to fail when the client hasn't marked the
                    // node as DOWN yet
                }
            }

            // Test reads which should fail
            for (ConsistencyLevel cl : failList) {
                try {
                    query(c, 12, cl);
                    fail(String.format("Test passed at CL.%s.", cl));
                } catch (InvalidQueryException e) {
                    List<String> acceptableErrorMessages = Arrays.asList(
                        "consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)",
                        "EACH_QUORUM ConsistencyLevel is only supported for writes");
                    assertTrue(acceptableErrorMessages.contains(e.getMessage()), String.format("Received: %s", e.getMessage()));
                } catch (ReadTimeoutException e) {
                    // expected to fail when the client hasn't marked the
                    // node as DOWN yet
                } catch (UnavailableException e) {
                    // expected to fail when the client has already marked the
                    // node as DOWN
                }
            }

        } catch (Throwable e) {
            c.errorOut();
            throw e;
        } finally {
            resetCoordinators();
            c.discard();
        }
    }

    @Test(groups = "long")
    public void testRFThreeDowngradingCLTwoDCs() throws Throwable {
        Cluster.Builder builder = Cluster.builder().withLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy())).withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE);
        CCMBridge.CCMCluster c = CCMBridge.buildCluster(3, 3, builder);
        try {

            createMultiDCSchema(c.session, 3, 3);
            init(c, 12, ConsistencyLevel.TWO);
            query(c, 12, ConsistencyLevel.TWO);

            assertQueried(CCMBridge.IP_PREFIX + "1", 0);
            assertQueried(CCMBridge.IP_PREFIX + "2", 0);
            assertQueried(CCMBridge.IP_PREFIX + "3", 12);
            assertQueried(CCMBridge.IP_PREFIX + "4", 0);
            assertQueried(CCMBridge.IP_PREFIX + "5", 0);
            assertQueried(CCMBridge.IP_PREFIX + "6", 0);

            resetCoordinators();
            c.cassandraCluster.forceStop(2);
            // FIXME: This sleep is needed to allow the waitFor() to work
            Thread.sleep(20000);
            waitForDownWithWait(CCMBridge.IP_PREFIX + "2", c.cluster, 5);

            List<ConsistencyLevel> acceptedList = Arrays.asList(
                                                    ConsistencyLevel.ANY,
                                                    ConsistencyLevel.ONE,
                                                    ConsistencyLevel.TWO,
                                                    ConsistencyLevel.QUORUM,
                                                    ConsistencyLevel.THREE,
                                                    ConsistencyLevel.ALL,
                                                    ConsistencyLevel.LOCAL_QUORUM,
                                                    ConsistencyLevel.EACH_QUORUM
                                                  );

            List<ConsistencyLevel> failList = Arrays.asList();

            // Test successful writes
            for (ConsistencyLevel cl : acceptedList) {
                try {
                    init(c, 12, cl);
                } catch (Exception e) {
                    fail(String.format("Test failed at CL.%s with message: %s", cl, e.getMessage()));
                }
            }

            // Test successful reads
            for (ConsistencyLevel cl : acceptedList) {
                try {
                    query(c, 12, cl);
                } catch (InvalidQueryException e) {
                    List<String> acceptableErrorMessages = Arrays.asList(
                        "EACH_QUORUM ConsistencyLevel is only supported for writes",
                        "ANY ConsistencyLevel is only supported for writes");
                    assertTrue(acceptableErrorMessages.contains(e.getMessage()), String.format("Received: %s", e.getMessage()));
                }
            }

            // Test writes which should fail
            for (ConsistencyLevel cl : failList) {
                try {
                    init(c, 12, cl);
                    fail(String.format("Test passed at CL.%s.", cl));
                } catch (UnavailableException e) {
                    // expected to fail when the client has already marked the
                    // node as DOWN
                } catch (WriteTimeoutException e) {
                    // expected to fail when the client hasn't marked the
                    // node as DOWN yet
                }
            }

            // Test reads which should fail
            for (ConsistencyLevel cl : failList) {
                try {
                    query(c, 12, cl);
                    fail(String.format("Test passed at CL.%s.", cl));
                } catch (ReadTimeoutException e) {
                    // expected to fail when the client hasn't marked the
                    // node as DOWN yet
                } catch (UnavailableException e) {
                    // expected to fail when the client has already marked the
                    // node as DOWN
                }
            }

        } catch (Throwable e) {
            c.errorOut();
            throw e;
        } finally {
            resetCoordinators();
            c.discard();
        }
    }

    @Test(groups = "long")
    public void testRFThreeDowngradingCLTwoDCsDCAware() throws Throwable {
        Cluster.Builder builder = Cluster.builder().withLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy("dc2"))).withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE);
        CCMBridge.CCMCluster c = CCMBridge.buildCluster(3, 3, builder);
        try {

            createMultiDCSchema(c.session, 3, 3);
            init(c, 12, ConsistencyLevel.TWO);
            query(c, 12, ConsistencyLevel.TWO);

            assertQueried(CCMBridge.IP_PREFIX + "1", 0);
            assertQueried(CCMBridge.IP_PREFIX + "2", 0);
            assertQueried(CCMBridge.IP_PREFIX + "3", 0);
            assertQueried(CCMBridge.IP_PREFIX + "4", 4);
            assertQueried(CCMBridge.IP_PREFIX + "5", 4);
            assertQueried(CCMBridge.IP_PREFIX + "6", 4);

            resetCoordinators();
            c.cassandraCluster.forceStop(2);
            // FIXME: This sleep is needed to allow the waitFor() to work
            //Thread.sleep(20000);
            waitForDownWithWait(CCMBridge.IP_PREFIX + "2", c.cluster, 5);

            List<ConsistencyLevel> acceptedList = Arrays.asList(
                                                    ConsistencyLevel.ANY,
                                                    ConsistencyLevel.ONE,
                                                    ConsistencyLevel.TWO,
                                                    ConsistencyLevel.QUORUM,
                                                    ConsistencyLevel.THREE,
                                                    ConsistencyLevel.ALL,
                                                    ConsistencyLevel.LOCAL_QUORUM,
                                                    ConsistencyLevel.EACH_QUORUM
                                                  );

            List<ConsistencyLevel> failList = Arrays.asList();

            // Test successful writes
            for (ConsistencyLevel cl : acceptedList) {
                try {
                    init(c, 12, cl);
                } catch (Exception e) {
                    fail(String.format("Test failed at CL.%s with message: %s", cl, e.getMessage()));
                }
            }

            // Test successful reads
            for (ConsistencyLevel cl : acceptedList) {
                try {
                    query(c, 12, cl);
                } catch (InvalidQueryException e) {
                    List<String> acceptableErrorMessages = Arrays.asList(
                        "EACH_QUORUM ConsistencyLevel is only supported for writes",
                        "ANY ConsistencyLevel is only supported for writes");
                    assertTrue(acceptableErrorMessages.contains(e.getMessage()), String.format("Received: %s", e.getMessage()));
                }
            }

            // Test writes which should fail
            for (ConsistencyLevel cl : failList) {
                try {
                    init(c, 12, cl);
                    fail(String.format("Test passed at CL.%s.", cl));
                } catch (UnavailableException e) {
                    // expected to fail when the client has already marked the
                    // node as DOWN
                } catch (WriteTimeoutException e) {
                    // expected to fail when the client hasn't marked the
                    // node as DOWN yet
                }
            }

            // Test reads which should fail
            for (ConsistencyLevel cl : failList) {
                try {
                    query(c, 12, cl);
                    fail(String.format("Test passed at CL.%s.", cl));
                } catch (ReadTimeoutException e) {
                    // expected to fail when the client hasn't marked the
                    // node as DOWN yet
                } catch (UnavailableException e) {
                    // expected to fail when the client has already marked the
                    // node as DOWN
                }
            }
        } catch (Throwable e) {
            c.errorOut();
            throw e;
        } finally {
            resetCoordinators();
            c.discard();
        }
    }
}
