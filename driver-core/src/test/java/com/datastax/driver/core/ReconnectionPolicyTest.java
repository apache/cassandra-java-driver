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

import com.datastax.driver.core.exceptions.NoHostAvailableException;

import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;

import com.datastax.driver.core.policies.ReconnectionPolicy;
import org.testng.annotations.Test;
import static org.testng.Assert.*;

public class ReconnectionPolicyTest extends AbstractPoliciesTest {

    /*
     * Test the ExponentialReconnectionPolicy.
     */
    @Test(groups = "integration")
    public void exponentialReconnectionPolicyTest() throws Throwable {
        Cluster.Builder builder = Cluster.builder().withReconnectionPolicy(new ExponentialReconnectionPolicy(2 * 1000, 5 * 60 * 1000));

        // Ensure that ExponentialReconnectionPolicy is what we should be testing
        if (!(builder.getConfiguration().getPolicies().getReconnectionPolicy() instanceof ExponentialReconnectionPolicy)) {
            fail("Set policy does not match retrieved policy.");
        }

        // Test basic getters
        ExponentialReconnectionPolicy reconnectionPolicy = (ExponentialReconnectionPolicy) builder.getConfiguration().getPolicies().getReconnectionPolicy();
        assertTrue(reconnectionPolicy.getBaseDelayMs() == 2 * 1000);
        assertTrue(reconnectionPolicy.getMaxDelayMs() == 5 * 60 * 1000);

        // Test erroneous instantiations
        try {
            new ExponentialReconnectionPolicy(-1, 1);
            fail();
        } catch (IllegalArgumentException e) {}

        try {
            new ExponentialReconnectionPolicy(1, -1);
            fail();
        } catch (IllegalArgumentException e) {}

        try {
            new ExponentialReconnectionPolicy(-1, -1);
            fail();
        } catch (IllegalArgumentException e) {}

        try {
            new ExponentialReconnectionPolicy(2, 1);
            fail();
        } catch (IllegalArgumentException e) {}

        // Test nextDelays()
        ReconnectionPolicy.ReconnectionSchedule schedule = new ExponentialReconnectionPolicy(2 * 1000, 5 * 60 * 1000).newSchedule();
        assertTrue(schedule.nextDelayMs() == 2000);
        assertTrue(schedule.nextDelayMs() == 4000);
        assertTrue(schedule.nextDelayMs() == 8000);
        assertTrue(schedule.nextDelayMs() == 16000);
        assertTrue(schedule.nextDelayMs() == 32000);
        for (int i = 0; i < 64; ++i)
            schedule.nextDelayMs();
        assertTrue(schedule.nextDelayMs() == reconnectionPolicy.getMaxDelayMs());

        // Run integration test
        long restartTime = 2 + 4 + 8 + 2;   // 16: 3 full cycles + 2 seconds
        long retryTime = 30;                // 4th cycle start time
        reconnectionPolicyTest(builder, restartTime, retryTime);
    }

    /*
     * Test the ConstantReconnectionPolicy.
     */
    @Test(groups = "integration")
    public void constantReconnectionPolicyTest() throws Throwable {
        Cluster.Builder builder = Cluster.builder().withReconnectionPolicy(new ConstantReconnectionPolicy(10 * 1000));

        // Ensure that ConstantReconnectionPolicy is what we should be testing
        if (!(builder.getConfiguration().getPolicies().getReconnectionPolicy() instanceof ConstantReconnectionPolicy)) {
            fail("Set policy does not match retrieved policy.");
        }

        // Test basic getters
        ConstantReconnectionPolicy reconnectionPolicy = (ConstantReconnectionPolicy) builder.getConfiguration().getPolicies().getReconnectionPolicy();
        assertTrue(reconnectionPolicy.getConstantDelayMs() == 10 * 1000);

        // Test erroneous instantiations
        try {
            new ConstantReconnectionPolicy(-1);
            fail();
        } catch (IllegalArgumentException e) {}

        // Test nextDelays()
        ReconnectionPolicy.ReconnectionSchedule schedule = new ConstantReconnectionPolicy(10 * 1000).newSchedule();
        assertTrue(schedule.nextDelayMs() == 10000);
        assertTrue(schedule.nextDelayMs() == 10000);
        assertTrue(schedule.nextDelayMs() == 10000);
        assertTrue(schedule.nextDelayMs() == 10000);
        assertTrue(schedule.nextDelayMs() == 10000);

        // Run integration test
        long restartTime = 16;      // matches the above test
        long retryTime = 20;        // 2nd cycle start time
        reconnectionPolicyTest(builder, restartTime, retryTime);
    }

    public void reconnectionPolicyTest(Cluster.Builder builder, long restartTime, long retryTime) throws Throwable {
        CCMBridge.CCMCluster c = CCMBridge.buildCluster(1, builder);
        createSchema(c.session, 1);

        try {
            init(c, 12);
            query(c, 12);

            // Ensure a basic test works
            assertQueried(CCMBridge.IP_PREFIX + "1", 12);
            resetCoordinators();
            c.cassandraCluster.forceStop(1);

            // Start timing and ensure that the node is down
            long startTime = 0;
            try {
                startTime = System.nanoTime() / 1000000000;
                query(c, 12);
                fail("Test race condition where node has not shut off quickly enough.");
            } catch (NoHostAvailableException e) {}

            long thisTime;
            boolean restarted = false;
            while (true) {
                thisTime = System.nanoTime() / 1000000000;

                // Restart node at restartTime
                if (!restarted && thisTime - startTime > restartTime) {
                    c.cassandraCluster.start(1);
                    restarted = true;
                }

                // Continue testing queries each second
                try {
                    query(c, 12);
                    assertQueried(CCMBridge.IP_PREFIX + "1", 12);
                    resetCoordinators();

                    // Ensure the time when the query completes successfully is what was expected
                    assertTrue(retryTime - 2 < thisTime - startTime && thisTime - startTime < retryTime + 2);
                } catch (NoHostAvailableException e) {
                    Thread.sleep(1000);
                    continue;
                }

                // The the same query a few more times, just to be sure
                query(c, 12);
                assertQueried(CCMBridge.IP_PREFIX + "1", 12);
                resetCoordinators();

                query(c, 12);
                assertQueried(CCMBridge.IP_PREFIX + "1", 12);
                resetCoordinators();

                query(c, 12);
                assertQueried(CCMBridge.IP_PREFIX + "1", 12);

                // BUG: Errors appear underneath this when c.discard is called (exponentialReconnectionPolicyTest)
                // [Reconnection-1] ERROR com.datastax.driver.core.ControlConnection  - [Control connection] Cannot connect to any host, scheduling retry in 32000 milliseconds
                // Print statement is useful for seeing the error in the console
                System.out.println("Should not error out underneath this line, especially not with another 32 second wait");
                break;
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
