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
package com.datastax.driver.core;

import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.ReadTimeoutException;
import com.datastax.driver.core.exceptions.UnavailableException;
import com.datastax.driver.core.exceptions.WriteTimeoutException;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.datastax.driver.core.CreateCCM.TestMode.PER_METHOD;
import static com.datastax.driver.core.TestUtils.IP_PREFIX;
import static com.datastax.driver.core.TestUtils.ipOfNode;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@SuppressWarnings("unused")
@CreateCCM(PER_METHOD)
@CCMConfig(
        dirtiesContext = true,
        createKeyspace = false,
        config = {
                // tests fail often with write or read timeouts
                "phi_convict_threshold:5",
                "read_request_timeout_in_ms:200000",
                "write_request_timeout_in_ms:200000"
        }
)
public class ConsistencyTest extends AbstractPoliciesTest {

    private static final Logger logger = LoggerFactory.getLogger(ConsistencyTest.class);

    private Cluster.Builder tokenAwareRoundRobin() {
        return Cluster.builder()
                .withSocketOptions(new SocketOptions().setReadTimeoutMillis(300000))
                .withLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
    }

    private Cluster.Builder tokenAwareRoundRobinNoShuffle() {
        return Cluster.builder()
                .withSocketOptions(new SocketOptions().setReadTimeoutMillis(300000))
                .withLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy(), false));
    }

    private Cluster.Builder tokenAwareRoundRobinDowngrading() {
        return Cluster.builder()
                .withSocketOptions(new SocketOptions().setReadTimeoutMillis(300000))
                .withLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()))
                .withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE);
    }

    private Cluster.Builder tokenAwareRoundRobinNoShuffleDowngrading() {
        return Cluster.builder()
                .withSocketOptions(new SocketOptions().setReadTimeoutMillis(300000))
                .withLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy(), false))
                .withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE);
    }

    private Cluster.Builder roundRobinDowngrading() {
        return Cluster.builder()
                .withSocketOptions(new SocketOptions().setReadTimeoutMillis(300000))
                .withLoadBalancingPolicy(new RoundRobinPolicy())
                .withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE);
    }

    private Cluster.Builder tokenAwareDCAwareRoundRobinNoShuffleDowngrading() {
        return Cluster.builder()
                .withSocketOptions(new SocketOptions().setReadTimeoutMillis(300000))
                .withLoadBalancingPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().withLocalDc("dc2").build(), false))
                .withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE);
    }

    @Test(groups = "long")
    @CCMConfig(
            numberOfNodes = 3,
            clusterProvider = "tokenAwareRoundRobin")
    public void testRFOneTokenAware() throws Throwable {

        createSchema(1);
        init(12, ConsistencyLevel.ONE);
        query(12, ConsistencyLevel.ONE);

        assertQueried(IP_PREFIX + '1', 0);
        assertQueried(IP_PREFIX + '2', 12);
        assertQueried(IP_PREFIX + '3', 0);

        resetCoordinators();
        stopAndWait(2);

        List<ConsistencyLevel> acceptedList = singletonList(ConsistencyLevel.ANY);

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
                init(12, cl);
            } catch (Exception e) {
                fail(String.format("Test failed at CL.%s with message: %s", cl, e.getMessage()));
            }
        }

        // Test successful reads
        for (ConsistencyLevel cl : acceptedList) {
            try {
                query(12, cl);
            } catch (InvalidQueryException e) {
                List<String> acceptableErrorMessages = singletonList(
                        "ANY ConsistencyLevel is only supported for writes");
                assertTrue(acceptableErrorMessages.contains(e.getMessage()));
            }
        }

        // Test writes which should fail
        for (ConsistencyLevel cl : failList) {
            try {
                init(12, cl);
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
                query(12, cl);
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
    }

    @Test(groups = "long")
    @CCMConfig(numberOfNodes = 3,
            clusterProvider = "tokenAwareRoundRobinNoShuffle")
    public void testRFTwoTokenAware() throws Throwable {
        createSchema(2);
        init(12, ConsistencyLevel.TWO);
        query(12, ConsistencyLevel.TWO);

        assertQueried(IP_PREFIX + '1', 0);
        assertQueried(IP_PREFIX + '2', 12);
        assertQueried(IP_PREFIX + '3', 0);

        resetCoordinators();
        stopAndWait(2);

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
                init(12, cl);
            } catch (Exception e) {
                fail(String.format("Test failed at CL.%s with message: %s", cl, e.getMessage()));
            }
        }

        // Test successful reads
        for (ConsistencyLevel cl : acceptedList) {
            try {
                query(12, cl);
            } catch (InvalidQueryException e) {
                List<String> acceptableErrorMessages = singletonList(
                        "ANY ConsistencyLevel is only supported for writes");
                assertTrue(acceptableErrorMessages.contains(e.getMessage()));
            }
        }

        // Test writes which should fail
        for (ConsistencyLevel cl : failList) {
            try {
                init(12, cl);
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
                query(12, cl);
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
    }

    @Test(groups = "long")
    @CCMConfig(numberOfNodes = 3,
            clusterProvider = "tokenAwareRoundRobinNoShuffle")
    public void testRFThreeTokenAware() throws Throwable {
        createSchema(3);
        init(12, ConsistencyLevel.TWO);
        query(12, ConsistencyLevel.TWO);

        assertQueried(IP_PREFIX + '1', 0);
        assertQueried(IP_PREFIX + '2', 12);
        assertQueried(IP_PREFIX + '3', 0);

        resetCoordinators();
        stopAndWait(2);

        Set<ConsistencyLevel> cls = EnumSet.allOf(ConsistencyLevel.class);
        // Remove serial consistencies as they require conditional read/writes
        cls.remove(ConsistencyLevel.SERIAL);
        cls.remove(ConsistencyLevel.LOCAL_SERIAL);

        List<ConsistencyLevel> acceptedList = Arrays.asList(
                ConsistencyLevel.ANY,
                ConsistencyLevel.ONE,
                ConsistencyLevel.TWO,
                ConsistencyLevel.QUORUM,
                ConsistencyLevel.LOCAL_QUORUM,
                ConsistencyLevel.EACH_QUORUM
        );

        List<ConsistencyLevel> failList = Arrays.asList(
                ConsistencyLevel.THREE,
                ConsistencyLevel.ALL
        );


        // Test successful writes
        for (ConsistencyLevel cl : acceptedList) {
            try {
                init(12, cl);
            } catch (Exception e) {
                fail(String.format("Test failed at CL.%s with message: %s", cl, e.getMessage()));
            }
        }

        // Test successful reads
        for (ConsistencyLevel cl : acceptedList) {
            try {
                query(12, cl);
            } catch (InvalidQueryException e) {
                List<String> acceptableErrorMessages = Arrays.asList(
                        "ANY ConsistencyLevel is only supported for writes",
                        "EACH_QUORUM ConsistencyLevel is only supported for writes"
                );
                assertTrue(acceptableErrorMessages.contains(e.getMessage()), "Got unexpected message " + e.getMessage());
            }
        }

        // Test writes which should fail
        for (ConsistencyLevel cl : failList) {
            try {
                init(12, cl);
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
                query(12, cl);
                fail(String.format("Test passed at CL.%s.", cl));
            } catch (ReadTimeoutException e) {
                // expected to fail when the client hasn't marked the
                // node as DOWN yet
            } catch (UnavailableException e) {
                // expected to fail when the client has already marked the
                // node as DOWN
            }
        }
    }


    @Test(groups = "long")
    @CCMConfig(numberOfNodes = 3,
            clusterProvider = "tokenAwareRoundRobinDowngrading")
    public void testRFOneDowngradingCL() throws Throwable {
        createSchema(1);
        init(12, ConsistencyLevel.ONE);
        query(12, ConsistencyLevel.ONE);

        assertQueried(IP_PREFIX + '1', 0);
        assertQueried(IP_PREFIX + '2', 12);
        assertQueried(IP_PREFIX + '3', 0);

        resetCoordinators();
        stopAndWait(2);

        List<ConsistencyLevel> acceptedList = singletonList(
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
                init(12, cl);
            } catch (Exception e) {
                fail(String.format("Test failed at CL.%s with message: %s", cl, e.getMessage()));
            }
        }

        // Test successful reads
        for (ConsistencyLevel cl : acceptedList) {
            try {
                query(12, cl);
            } catch (InvalidQueryException e) {
                List<String> acceptableErrorMessages = singletonList(
                        "ANY ConsistencyLevel is only supported for writes");
                assertTrue(acceptableErrorMessages.contains(e.getMessage()));
            }
        }

        // Test writes which should fail
        for (ConsistencyLevel cl : failList) {
            try {
                init(12, cl);
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
                query(12, cl);
                fail(String.format("Test passed at CL.%s.", cl));
            } catch (InvalidQueryException e) {
                List<String> acceptableErrorMessages = singletonList(
                        "EACH_QUORUM ConsistencyLevel is only supported for writes"
                );
                assertTrue(acceptableErrorMessages.contains(e.getMessage()), "Got unexpected message " + e.getMessage());
            } catch (ReadTimeoutException e) {
                // expected to fail when the client hasn't marked the
                // node as DOWN yet
            } catch (UnavailableException e) {
                // expected to fail when the client has already marked the
                // node as DOWN
            }
        }
    }

    @Test(groups = "long")
    @CCMConfig(numberOfNodes = 3,
            clusterProvider = "tokenAwareRoundRobinNoShuffleDowngrading")
    public void testRFTwoDowngradingCL() throws Throwable {
        createSchema(2);
        init(12, ConsistencyLevel.TWO);
        query(12, ConsistencyLevel.TWO);

        assertQueried(IP_PREFIX + '1', 0);
        assertQueried(IP_PREFIX + '2', 12);
        assertQueried(IP_PREFIX + '3', 0);

        resetCoordinators();
        stopAndWait(2);

        Set<ConsistencyLevel> acceptedList = EnumSet.allOf(ConsistencyLevel.class);
        // Remove serial consistencies as they require conditional read/writes
        acceptedList.remove(ConsistencyLevel.SERIAL);
        acceptedList.remove(ConsistencyLevel.LOCAL_SERIAL);

        // Test successful writes
        for (ConsistencyLevel cl : acceptedList) {
            try {
                init(12, cl);
            } catch (Exception e) {
                fail(String.format("Test failed at CL.%s with message: %s", cl, e.getMessage()));
            }
        }

        // Test successful reads
        for (ConsistencyLevel cl : acceptedList) {
            try {
                query(12, cl);
            } catch (InvalidQueryException e) {
                List<String> acceptableErrorMessages = Arrays.asList(
                        "ANY ConsistencyLevel is only supported for writes",
                        "EACH_QUORUM ConsistencyLevel is only supported for writes"
                );
                assertTrue(acceptableErrorMessages.contains(e.getMessage()), "Got unexpected message " + e.getMessage());
            }
        }
    }

    @Test(groups = "long")
    @CCMConfig(numberOfNodes = 3,
            clusterProvider = "roundRobinDowngrading")
    public void testRFThreeRoundRobinDowngradingCL() throws Throwable {
        testRFThreeDowngradingCL();
    }

    @Test(groups = "long")
    @CCMConfig(numberOfNodes = 3,
            clusterProvider = "tokenAwareRoundRobinNoShuffleDowngrading")
    public void testRFThreeTokenAwareDowngradingCL() throws Throwable {
        testRFThreeDowngradingCL();
    }

    private void testRFThreeDowngradingCL() throws Throwable {

        createSchema(3);
        init(12, ConsistencyLevel.ALL);
        query(12, ConsistencyLevel.ALL);

        try {
            // This test catches TokenAwarePolicy
            // However, full tests in LoadBalancingPolicyTest.java
            assertQueried(IP_PREFIX + '1', 0);
            assertQueried(IP_PREFIX + '2', 12);
            assertQueried(IP_PREFIX + '3', 0);
        } catch (AssertionError e) {
            // This test catches RoundRobinPolicy
            assertQueried(IP_PREFIX + '1', 4);
            assertQueried(IP_PREFIX + '2', 4);
            assertQueried(IP_PREFIX + '3', 4);
        }

        resetCoordinators();
        stopAndWait(2);

        Set<ConsistencyLevel> acceptedList = EnumSet.allOf(ConsistencyLevel.class);
        // Remove serial consistencies as they require conditional read/writes
        acceptedList.remove(ConsistencyLevel.SERIAL);
        acceptedList.remove(ConsistencyLevel.LOCAL_SERIAL);

        // Test successful writes
        for (ConsistencyLevel cl : acceptedList) {
            try {
                init(12, cl);
            } catch (Exception e) {
                fail(String.format("Test failed at CL.%s with message: %s", cl, e.getMessage()));
            }
        }

        // Test successful reads
        for (ConsistencyLevel cl : acceptedList) {
            try {
                query(12, cl);
            } catch (InvalidQueryException e) {
                List<String> acceptableErrorMessages = Arrays.asList(
                        "ANY ConsistencyLevel is only supported for writes",
                        "EACH_QUORUM ConsistencyLevel is only supported for writes"
                );
                assertTrue(acceptableErrorMessages.contains(e.getMessage()), "Got unexpected message " + e.getMessage());
            }
        }
    }

    @Test(groups = "long")
    @CCMConfig(
            numberOfNodes = {3, 3},
            clusterProvider = "tokenAwareRoundRobinNoShuffleDowngrading"
    )
    public void testRFThreeDowngradingCLTwoDCs() throws Throwable {
        createMultiDCSchema(3, 3);
        init(12, ConsistencyLevel.TWO);
        query(12, ConsistencyLevel.TWO);

        assertQueried(IP_PREFIX + '1', 0);
        assertQueried(IP_PREFIX + '2', 12);
        assertQueried(IP_PREFIX + '3', 0);
        assertQueried(IP_PREFIX + '4', 0);
        assertQueried(IP_PREFIX + '5', 0);
        assertQueried(IP_PREFIX + '6', 0);

        resetCoordinators();
        stopAndWait(3);

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

        List<ConsistencyLevel> failList = emptyList();

        // Test successful writes
        for (ConsistencyLevel cl : acceptedList) {
            logger.debug("Test successful init(): " + cl);
            try {
                init(12, cl);
            } catch (Exception e) {
                fail(String.format("Test failed at CL.%s with message: %s", cl, e.getMessage()));
            }
        }

        // Test successful reads
        for (ConsistencyLevel cl : acceptedList) {
            logger.debug("Test successful query(): " + cl);
            try {
                query(12, cl);
            } catch (InvalidQueryException e) {
                List<String> acceptableErrorMessages = Arrays.asList(
                        "EACH_QUORUM ConsistencyLevel is only supported for writes",
                        "ANY ConsistencyLevel is only supported for writes");
                assertTrue(acceptableErrorMessages.contains(e.getMessage()), String.format("Received: %s", e.getMessage()));
            }
        }

        // Test writes which should fail
        for (ConsistencyLevel cl : failList) {
            logger.debug("Test failure init(): " + cl);
            try {
                init(12, cl);
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
            logger.debug("Test failure query(): " + cl);
            try {
                query(12, cl);
                fail(String.format("Test passed at CL.%s.", cl));
            } catch (ReadTimeoutException e) {
                // expected to fail when the client hasn't marked the
                // node as DOWN yet
            } catch (UnavailableException e) {
                // expected to fail when the client has already marked the
                // node as DOWN
            }
        }
    }

    @Test(groups = "long")
    @CCMConfig(
            numberOfNodes = {3, 3},
            clusterProvider = "tokenAwareDCAwareRoundRobinNoShuffleDowngrading"
    )
    public void testRFThreeDowngradingCLTwoDCsDCAware() throws Throwable {
        createMultiDCSchema(3, 3);
        init(12, ConsistencyLevel.TWO);
        query(12, ConsistencyLevel.TWO);

        assertQueried(IP_PREFIX + '1', 0);
        assertQueried(IP_PREFIX + '2', 0);
        assertQueried(IP_PREFIX + '3', 0);
        assertQueried(IP_PREFIX + '4', 0);
        assertQueried(IP_PREFIX + '5', 12);
        assertQueried(IP_PREFIX + '6', 0);

        resetCoordinators();
        stopAndWait(2);

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

        List<ConsistencyLevel> failList = emptyList();

        // Test successful writes
        for (ConsistencyLevel cl : acceptedList) {
            try {
                init(12, cl);
            } catch (Exception e) {
                fail(String.format("Test failed at CL.%s with message: %s", cl, e.getMessage()));
            }
        }

        // Test successful reads
        for (ConsistencyLevel cl : acceptedList) {
            try {
                query(12, cl);
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
                init(12, cl);
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
                query(12, cl);
                fail(String.format("Test passed at CL.%s.", cl));
            } catch (ReadTimeoutException e) {
                // expected to fail when the client hasn't marked the
                // node as DOWN yet
            } catch (UnavailableException e) {
                // expected to fail when the client has already marked the
                // node as DOWN
            }
        }
    }

    private void stopAndWait(int node) {
        logger.debug("Stopping node " + node);
        ccm().stop(node);
        ccm().waitForDown(node);// this uses port ping
        TestUtils.waitForDown(ipOfNode(node), cluster()); // this uses UP/DOWN events
        logger.debug("Node " + node + " stopped, sleeping one extra minute to allow nodes to gossip");
        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.MINUTES);
    }

}
