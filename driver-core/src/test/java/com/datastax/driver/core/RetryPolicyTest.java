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

import java.net.InetAddress;
import java.util.*;

import org.testng.annotations.Test;
import static org.testng.Assert.*;

import com.datastax.driver.core.exceptions.*;
import com.datastax.driver.core.policies.*;
import static com.datastax.driver.core.TestUtils.*;

public class RetryPolicyTest extends AbstractPoliciesTest {

    /**
     * Test RetryPolicy to ensure the basic unit get tests for the RetryDecisions.
     */
    public static class TestRetryPolicy implements RetryPolicy {
        public RetryDecision onReadTimeout(Query query, ConsistencyLevel cl, int requiredResponses, int receivedResponses, boolean dataRetrieved, int nbRetry) {
            return RetryDecision.rethrow();
        }
        public RetryDecision onWriteTimeout(Query query, ConsistencyLevel cl, WriteType writeType, int requiredAcks, int receivedAcks, int nbRetry) {
            return RetryDecision.rethrow();
        }
        public RetryDecision onUnavailable(Query query, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry) {
            return RetryDecision.rethrow();
        }
        public static void testRetryDecision() {
            assertEquals(RetryDecision.retry(ConsistencyLevel.ONE).getType(), RetryDecision.Type.RETRY);
            assertEquals(RetryDecision.retry(ConsistencyLevel.ONE).getRetryConsistencyLevel(), ConsistencyLevel.ONE);
            assertEquals(RetryDecision.rethrow().getType(), RetryDecision.Type.RETHROW);
            assertEquals(RetryDecision.ignore().getType(), RetryDecision.Type.IGNORE);

            assertEquals(RetryDecision.retry(ConsistencyLevel.ONE).toString(), "Retry at " + ConsistencyLevel.ONE);
            assertEquals(RetryDecision.rethrow().toString(), "Rethrow");
            assertEquals(RetryDecision.ignore().toString(), "Ignore");
        }
    }

    /**
     * Test RetryDecision get variables and defaults are correct.
     */
    @Test(groups = "unit")
    public void RetryDecisionTest() throws Throwable {
        TestRetryPolicy.testRetryDecision();
    }

    /*
     * Test the DefaultRetryPolicy.
     */
    @Test(groups = "long")
    public void defaultRetryPolicy() throws Throwable {
        Cluster.Builder builder = Cluster.builder();
        defaultPolicyTest(builder);
    }

    /*
     * Test the DefaultRetryPolicy with Logging enabled.
     */
    @Test(groups = "long")
    public void defaultLoggingPolicy() throws Throwable {
        Cluster.Builder builder = Cluster.builder().withRetryPolicy(new LoggingRetryPolicy(DefaultRetryPolicy.INSTANCE));
        defaultPolicyTest(builder);
    }

    /*
     * Test the FallthroughRetryPolicy.
     * Uses the same code that DefaultRetryPolicy uses.
     */
    @Test(groups = "long")
    public void fallthroughRetryPolicy() throws Throwable {
        Cluster.Builder builder = Cluster.builder().withRetryPolicy(FallthroughRetryPolicy.INSTANCE);
        defaultPolicyTest(builder);
    }

    /*
     * Test the FallthroughRetryPolicy with Logging enabled.
     * Uses the same code that DefaultRetryPolicy uses.
     */
    @Test(groups = "long")
    public void fallthroughLoggingPolicy() throws Throwable {
        Cluster.Builder builder = Cluster.builder().withRetryPolicy(new LoggingRetryPolicy(FallthroughRetryPolicy.INSTANCE));
        defaultPolicyTest(builder);
    }

    public void defaultPolicyTest(Cluster.Builder builder) throws Throwable {
        CCMBridge.CCMCluster c = CCMBridge.buildCluster(2, builder);
        try {
            createSchema(c.session);
            // FIXME: Race condition where the nodes are not fully up yet and assertQueried reports slightly different numbers with fallthrough*Policy
            Thread.sleep(5000);

            init(c, 12);
            query(c, 12);

            assertQueried(CCMBridge.IP_PREFIX + "1", 6);
            assertQueried(CCMBridge.IP_PREFIX + "2", 6);

            resetCoordinators();

            // Test reads
            boolean successfulQuery = false;
            boolean readTimeoutOnce = false;
            boolean unavailableOnce = false;
            boolean restartOnce = false;
            for (int i = 0; i < 100; ++i) {
                try {
                    // Force a ReadTimeoutException to be performed once
                    if (!readTimeoutOnce) {
                        c.cassandraCluster.forceStop(2);
                    }

                    // Force an UnavailableException to be performed once
                    if (readTimeoutOnce && !unavailableOnce) {
                        waitForDownWithWait(CCMBridge.IP_PREFIX + "2", c.cluster, 5);
                    }

                    // Bring back node to ensure other errors are not thrown on restart
                    if (unavailableOnce && !restartOnce) {
                        c.cassandraCluster.start(2);
                        restartOnce = true;
                    }

                    query(c, 12);

                    if (restartOnce)
                        successfulQuery = true;
                } catch (UnavailableException e) {
                    assertEquals("Not enough replica available for query at consistency ONE (1 required but only 0 alive)", e.getMessage());
                    unavailableOnce = true;
                } catch (ReadTimeoutException e) {
                    assertEquals("Cassandra timeout during read query at consistency ONE (1 responses were required but only 0 replica responded)", e.getMessage());
                    readTimeoutOnce = true;
                }
            }

            // Ensure the full cycle was completed
            assertTrue(successfulQuery, "Hit testing race condition. [Never completed successfully.] (Shouldn't be an issue.):\n");
            assertTrue(readTimeoutOnce, "Hit testing race condition. [Never encountered a ReadTimeoutException.] (Shouldn't be an issue.):\n");
            assertTrue(unavailableOnce, "Hit testing race condition. [Never encountered an UnavailableException.] (Shouldn't be an issue.):\n");

            // A weak test to ensure that the nodes were contacted
            assertQueriedAtLeast(CCMBridge.IP_PREFIX + "1", 1);
            assertQueriedAtLeast(CCMBridge.IP_PREFIX + "2", 1);

            resetCoordinators();


            // Test writes
            successfulQuery = false;
            boolean writeTimeoutOnce = false;
            unavailableOnce = false;
            restartOnce = false;
            for (int i = 0; i < 100; ++i) {
                try {
                    // Force a WriteTimeoutException to be performed once
                    if (!writeTimeoutOnce) {
                        c.cassandraCluster.forceStop(2);
                    }

                    // Force an UnavailableException to be performed once
                    if (writeTimeoutOnce && !unavailableOnce) {
                        waitForDownWithWait(CCMBridge.IP_PREFIX + "2", c.cluster, 5);
                    }

                    // Bring back node to ensure other errors are not thrown on restart
                    if (unavailableOnce && !restartOnce) {
                        c.cassandraCluster.start(2);
                        restartOnce = true;
                    }

                    init(c, 12);

                    if (restartOnce)
                        successfulQuery = true;
                } catch (UnavailableException e) {
                    assertEquals("Not enough replica available for query at consistency ONE (1 required but only 0 alive)", e.getMessage());
                    unavailableOnce = true;
                } catch (WriteTimeoutException e) {
                    assertEquals("Cassandra timeout during write query at consistency ONE (1 replica were required but only 0 acknowledged the write)", e.getMessage());
                    writeTimeoutOnce = true;
                }
            }
            // Ensure the full cycle was completed
            assertTrue(successfulQuery, "Hit testing race condition. [Never completed successfully.] (Shouldn't be an issue.):\n");
            assertTrue(writeTimeoutOnce, "Hit testing race condition. [Never encountered a ReadTimeoutException.] (Shouldn't be an issue.):\n");
            assertTrue(unavailableOnce, "Hit testing race condition. [Never encountered an UnavailableException.] (Shouldn't be an issue.):\n");

            // TODO: Missing test to see if nodes were written to


            // Test batch writes
            successfulQuery = false;
            writeTimeoutOnce = false;
            unavailableOnce = false;
            restartOnce = false;
            for (int i = 0; i < 100; ++i) {
                try {
                    // Force a WriteTimeoutException to be performed once
                    if (!writeTimeoutOnce) {
                        c.cassandraCluster.forceStop(2);
                    }

                    // Force an UnavailableException to be performed once
                    if (writeTimeoutOnce && !unavailableOnce) {
                        waitForDownWithWait(CCMBridge.IP_PREFIX + "2", c.cluster, 5);
                    }

                    // Bring back node to ensure other errors are not thrown on restart
                    if (unavailableOnce && !restartOnce) {
                        c.cassandraCluster.start(2);
                        restartOnce = true;
                    }

                    init(c, 12, true);

                    if (restartOnce)
                        successfulQuery = true;
                } catch (UnavailableException e) {
                    assertEquals("Not enough replica available for query at consistency ONE (1 required but only 0 alive)", e.getMessage());
                    unavailableOnce = true;
                } catch (WriteTimeoutException e) {
                    assertEquals("Cassandra timeout during write query at consistency ONE (1 replica were required but only 0 acknowledged the write)", e.getMessage());
                    writeTimeoutOnce = true;
                }
            }
            // Ensure the full cycle was completed
            assertTrue(successfulQuery, "Hit testing race condition. [Never completed successfully.] (Shouldn't be an issue.):\n");
            assertTrue(writeTimeoutOnce, "Hit testing race condition. [Never encountered a ReadTimeoutException.] (Shouldn't be an issue.):\n");
            assertTrue(unavailableOnce, "Hit testing race condition. [Never encountered an UnavailableException.] (Shouldn't be an issue.):\n");

            // TODO: Missing test to see if nodes were written to

        } catch (Throwable e) {
            c.errorOut();
            throw e;
        } finally {
            resetCoordinators();
            c.discard();
        }
    }

    /**
     * Tests DowngradingConsistencyRetryPolicy
     */
    @Test(groups = "long")
    public void downgradingConsistencyRetryPolicy() throws Throwable {
        Cluster.Builder builder = Cluster.builder().withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE);
        downgradingConsistencyRetryPolicy(builder);
    }

    /**
     * Tests DowngradingConsistencyRetryPolicy with LoggingRetryPolicy
     */
    @Test(groups = "long")
    public void downgradingConsistencyLoggingPolicy() throws Throwable {
        Cluster.Builder builder = Cluster.builder().withRetryPolicy(new LoggingRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE));
        downgradingConsistencyRetryPolicy(builder);
    }

    /**
     * Tests DowngradingConsistencyRetryPolicy
     */
    public void downgradingConsistencyRetryPolicy(Cluster.Builder builder) throws Throwable {
        CCMBridge.CCMCluster c = CCMBridge.buildCluster(3, builder);
        try {
            createSchema(c.session, 3);
            // FIXME: Race condition where the nodes are not fully up yet and assertQueried reports slightly different numbers
            Thread.sleep(5000);

            init(c, 12, ConsistencyLevel.ALL);
            query(c, 12, ConsistencyLevel.ALL);

            assertQueried(CCMBridge.IP_PREFIX + "1", 4);
            assertQueried(CCMBridge.IP_PREFIX + "2", 4);
            assertQueried(CCMBridge.IP_PREFIX + "3", 4);

            resetCoordinators();
            c.cassandraCluster.forceStop(2);
            waitForDownWithWait(CCMBridge.IP_PREFIX + "2", c.cluster, 10);

            query(c, 12, ConsistencyLevel.ALL);

            assertQueried(CCMBridge.IP_PREFIX + "1", 6);
            assertQueried(CCMBridge.IP_PREFIX + "2", 0);
            assertQueried(CCMBridge.IP_PREFIX + "3", 6);

            resetCoordinators();
            c.cassandraCluster.forceStop(1);
            waitForDownWithWait(CCMBridge.IP_PREFIX + "1", c.cluster, 5);
            Thread.sleep(5000);

            try {
                query(c, 12, ConsistencyLevel.ALL);
            } catch (ReadTimeoutException e) {
                assertEquals("Cassandra timeout during read query at consistency TWO (2 responses were required but only 1 replica responded)", e.getMessage());
            }

            query(c, 12, ConsistencyLevel.QUORUM);

            assertQueried(CCMBridge.IP_PREFIX + "1", 0);
            assertQueried(CCMBridge.IP_PREFIX + "2", 0);
            assertQueried(CCMBridge.IP_PREFIX + "3", 12);

            resetCoordinators();

            query(c, 12, ConsistencyLevel.TWO);

            assertQueried(CCMBridge.IP_PREFIX + "1", 0);
            assertQueried(CCMBridge.IP_PREFIX + "2", 0);
            assertQueried(CCMBridge.IP_PREFIX + "3", 12);

            resetCoordinators();

            query(c, 12, ConsistencyLevel.ONE);

            assertQueried(CCMBridge.IP_PREFIX + "1", 0);
            assertQueried(CCMBridge.IP_PREFIX + "2", 0);
            assertQueried(CCMBridge.IP_PREFIX + "3", 12);

        } catch (Throwable e) {
            c.errorOut();
            throw e;
        } finally {
            resetCoordinators();
            c.discard();
        }
    }

    /*
     * Test the AlwaysIgnoreRetryPolicy with Logging enabled.
     */
    @Test(groups = "long")
    public void alwaysIgnoreRetryPolicyTest() throws Throwable {
        Cluster.Builder builder = Cluster.builder().withRetryPolicy(new LoggingRetryPolicy(AlwaysIgnoreRetryPolicy.INSTANCE));
        CCMBridge.CCMCluster c = CCMBridge.buildCluster(2, builder);

        try {
            createSchema(c.session);
            init(c, 12);
            query(c, 12);

            assertQueried(CCMBridge.IP_PREFIX + "1", 6);
            assertQueried(CCMBridge.IP_PREFIX + "2", 6);

            resetCoordinators();

            // Test failed reads
            c.cassandraCluster.forceStop(2);
            for (int i = 0; i < 10; ++i) {
                query(c, 12);
            }

            // A weak test to ensure that the nodes were contacted
            assertQueried(CCMBridge.IP_PREFIX + "1", 120);
            assertQueried(CCMBridge.IP_PREFIX + "2", 0);
            resetCoordinators();


            c.cassandraCluster.start(2);
            waitFor(CCMBridge.IP_PREFIX + "2", c.cluster);

            // Test successful reads
            for (int i = 0; i < 10; ++i) {
                query(c, 12);
            }

            // A weak test to ensure that the nodes were contacted
            assertQueriedAtLeast(CCMBridge.IP_PREFIX + "1", 1);
            assertQueriedAtLeast(CCMBridge.IP_PREFIX + "2", 1);
            resetCoordinators();


            // Test writes
            for (int i = 0; i < 100; ++i) {
                init(c, 12);
            }

            // TODO: Missing test to see if nodes were written to


            // Test failed writes
            c.cassandraCluster.forceStop(2);
            for (int i = 0; i < 100; ++i) {
                init(c, 12);
            }

            // TODO: Missing test to see if nodes were written to

        } catch (Throwable e) {
            c.errorOut();
            throw e;
        } finally {
            resetCoordinators();
            c.discard();
        }
    }

    private class QueryRunnable implements Runnable {
        private CCMBridge.CCMCluster c;
        private int i;

        public QueryRunnable(CCMBridge.CCMCluster c, int i) {
            this.c = c;
            this.i = i;
        }

        public void run(){
            query(c, i);
        }
    }

    private class InitRunnable implements Runnable {
        private CCMBridge.CCMCluster c;
        private int i;

        public InitRunnable(CCMBridge.CCMCluster c, int i) {
            this.c = c;
            this.i = i;
        }

        public void run(){
            try {
                init(c, i);
                fail();
            } catch (DriverInternalError e) {
            } catch (NoHostAvailableException e) { }
        }
    }

    /*
     * Test the AlwaysIgnoreRetryPolicy with Logging enabled.
     */
    @Test(groups = "long")
    public void alwaysRetryRetryPolicyTest() throws Throwable {
        Cluster.Builder builder = Cluster.builder().withRetryPolicy(new LoggingRetryPolicy(AlwaysRetryRetryPolicy.INSTANCE));
        CCMBridge.CCMCluster c = CCMBridge.buildCluster(2, builder);

        try {
            createSchema(c.session);
            init(c, 12);
            query(c, 12);

            assertQueried(CCMBridge.IP_PREFIX + "1", 6);
            assertQueried(CCMBridge.IP_PREFIX + "2", 6);

            resetCoordinators();

            // Test failed reads
            c.cassandraCluster.forceStop(2);

            Thread t1 = new Thread(new QueryRunnable(c, 12));
            t1.start();
            t1.join(10000);
            if (t1.isAlive())
                t1.interrupt();

            // A weak test to ensure that the nodes were contacted
            assertQueried(CCMBridge.IP_PREFIX + "1", 0);
            assertQueried(CCMBridge.IP_PREFIX + "2", 0);
            resetCoordinators();


            c.cassandraCluster.start(2);
            waitFor(CCMBridge.IP_PREFIX + "2", c.cluster);

            // Test successful reads
            for (int i = 0; i < 10; ++i) {
                query(c, 12);
            }

            // A weak test to ensure that the nodes were contacted
            assertQueriedAtLeast(CCMBridge.IP_PREFIX + "1", 1);
            assertQueriedAtLeast(CCMBridge.IP_PREFIX + "2", 1);
            resetCoordinators();


            // Test writes
            for (int i = 0; i < 100; ++i) {
                init(c, 12);
            }

            // TODO: Missing test to see if nodes were written to


            // Test failed writes
            c.cassandraCluster.forceStop(2);
            Thread t2 = new Thread(new InitRunnable(c, 12));
            t2.start();
            t2.join(10000);
            if (t2.isAlive())
                t2.interrupt();

            // TODO: Missing test to see if nodes were written to

        } catch (Throwable e) {
            c.errorOut();
            throw e;
        } finally {
            resetCoordinators();
            c.discard();
        }
    }
}
