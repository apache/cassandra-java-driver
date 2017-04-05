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

import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import org.testng.annotations.Test;

import static com.datastax.driver.core.CreateCCM.TestMode.PER_METHOD;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@CreateCCM(PER_METHOD)
@CCMConfig(dirtiesContext = true, createKeyspace = false)
@SuppressWarnings("unused")
public class ReconnectionPolicyTest extends AbstractPoliciesTest {

    private Cluster.Builder exponential() {
        return Cluster.builder().withReconnectionPolicy(new ExponentialReconnectionPolicy(2 * 1000, 5 * 60 * 1000));
    }

    private Cluster.Builder constant() {
        return Cluster.builder().withReconnectionPolicy(new ConstantReconnectionPolicy(10 * 1000));
    }

    /*
     * Test the ExponentialReconnectionPolicy.
     */
    @Test(groups = "long")
    @CCMConfig(clusterProvider = "exponential")
    public void exponentialReconnectionPolicyTest() throws Throwable {

        // Ensure that ExponentialReconnectionPolicy is what we should be testing
        if (!(cluster().getConfiguration().getPolicies().getReconnectionPolicy() instanceof ExponentialReconnectionPolicy)) {
            fail("Set policy does not match retrieved policy.");
        }

        // Test basic getters
        ExponentialReconnectionPolicy reconnectionPolicy = (ExponentialReconnectionPolicy) cluster().getConfiguration().getPolicies().getReconnectionPolicy();
        assertTrue(reconnectionPolicy.getBaseDelayMs() == 2 * 1000);
        assertTrue(reconnectionPolicy.getMaxDelayMs() == 5 * 60 * 1000);

        // Test erroneous instantiations
        try {
            new ExponentialReconnectionPolicy(-1, 1);
            fail();
        } catch (IllegalArgumentException e) {
            //ok
        }

        try {
            new ExponentialReconnectionPolicy(1, -1);
            fail();
        } catch (IllegalArgumentException e) {
            //ok
        }

        try {
            new ExponentialReconnectionPolicy(-1, -1);
            fail();
        } catch (IllegalArgumentException e) {
            //ok
        }

        try {
            new ExponentialReconnectionPolicy(2, 1);
            fail();
        } catch (IllegalArgumentException e) {
            //ok
        }

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
        long breakTime = 62;                // time until next reconnection attempt

        // TODO: Try to sort out variance
        //reconnectionPolicyTest(restartTime, retryTime, breakTime);
    }

    /*
     * Test the ConstantReconnectionPolicy.
     */
    @Test(groups = "long")
    @CCMConfig(clusterProvider = "constant")
    public void constantReconnectionPolicyTest() throws Throwable {

        // Ensure that ConstantReconnectionPolicy is what we should be testing
        if (!(cluster().getConfiguration().getPolicies().getReconnectionPolicy() instanceof ConstantReconnectionPolicy)) {
            fail("Set policy does not match retrieved policy.");
        }

        // Test basic getters
        ConstantReconnectionPolicy reconnectionPolicy = (ConstantReconnectionPolicy) cluster().getConfiguration().getPolicies().getReconnectionPolicy();
        assertTrue(reconnectionPolicy.getConstantDelayMs() == 10 * 1000);

        // Test erroneous instantiations
        try {
            new ConstantReconnectionPolicy(-1);
            fail();
        } catch (IllegalArgumentException e) {
            //ok
        }

        // Test nextDelays()
        ReconnectionPolicy.ReconnectionSchedule schedule = new ConstantReconnectionPolicy(10 * 1000).newSchedule();
        assertTrue(schedule.nextDelayMs() == 10000);
        assertTrue(schedule.nextDelayMs() == 10000);
        assertTrue(schedule.nextDelayMs() == 10000);
        assertTrue(schedule.nextDelayMs() == 10000);
        assertTrue(schedule.nextDelayMs() == 10000);

        // Run integration test
        long restartTime = 32;      // matches the above test
        long retryTime = 40;        // 2nd cycle start time
        long breakTime = 10;        // time until next reconnection attempt

        // TODO: Try to sort out variance
        //reconnectionPolicyTest(restartTime, retryTime, breakTime);
    }

    public void reconnectionPolicyTest(long restartTime, long retryTime, long breakTime) throws Throwable {
        createSchema(1);
        init(12);
        query(12);

        // Ensure a basic test works
        assertQueried(TestUtils.IP_PREFIX + '1', 12);
        resetCoordinators();
        ccm().forceStop(1);

        // Start timing and ensure that the node is down
        long startTime = 0;
        try {
            startTime = System.nanoTime() / 1000000000;
            query(12);
            fail("Test race condition where node has not shut off quickly enough.");
        } catch (NoHostAvailableException e) {
            //ok
        }

        long thisTime;
        boolean restarted = false;
        while (true) {
            thisTime = System.nanoTime() / 1000000000;

            // Restart node at restartTime
            if (!restarted && thisTime - startTime > restartTime) {
                ccm().start(1);
                restarted = true;
            }

            // Continue testing queries each second
            try {
                query(12);
                assertQueried(TestUtils.IP_PREFIX + '1', 12);
                resetCoordinators();

                // Ensure the time when the query completes successfully is what was expected
                assertTrue(retryTime - 2 < thisTime - startTime && thisTime - startTime < retryTime + 2, String.format("Waited %s seconds instead an expected %s seconds wait", thisTime - startTime, retryTime));
            } catch (NoHostAvailableException e) {
                Thread.sleep(1000);
                continue;
            }

            Thread.sleep(breakTime);

            // The the same query once more, just to be sure
            query(12);
            assertQueried(TestUtils.IP_PREFIX + '1', 12);
            resetCoordinators();

            // Ensure the reconnection times reset
            ccm().forceStop(1);

            // Start timing and ensure that the node is down
            startTime = 0;
            try {
                startTime = System.nanoTime() / 1000000000;
                query(12);
                fail("Test race condition where node has not shut off quickly enough.");
            } catch (NoHostAvailableException e) {
                //ok
            }

            restarted = false;
            while (true) {
                thisTime = System.nanoTime() / 1000000000;

                // Restart node at restartTime
                if (!restarted && thisTime - startTime > restartTime) {
                    ccm().start(1);
                    restarted = true;
                }

                // Continue testing queries each second
                try {
                    query(12);
                    assertQueried(TestUtils.IP_PREFIX + '1', 12);
                    resetCoordinators();

                    // Ensure the time when the query completes successfully is what was expected
                    assertTrue(retryTime - 2 < thisTime - startTime && thisTime - startTime < retryTime + 2, String.format("Waited %s seconds instead an expected %s seconds wait", thisTime - startTime, retryTime));
                } catch (NoHostAvailableException e) {
                    Thread.sleep(1000);
                    continue;
                }
                break;
            }
            break;
        }
    }
}
