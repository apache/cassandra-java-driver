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

import org.testng.annotations.Test;
import static org.testng.Assert.assertEquals;

/**
 * Test the drivers threads for isDaemon
 */
public class DaemonModeTest {

    /**
     * Test the number of not daemon threads does not change
     * @throws Throwable
     */
    @Test(groups = "short")
    public void onlyDaemonThreads() throws Throwable
    {
        int runningThreads = getNumberOfNoneDaemonThreads();
        
        Cluster.Builder builder = Cluster.builder();
        CCMBridge.CCMCluster c = CCMBridge.buildCluster(1, builder);        
        assertEquals(getNumberOfNoneDaemonThreads(), runningThreads);

        try {
            c.session.execute("SELECT COUNT(*) FROM system.hints LIMIT 1").one().getLong(0);
            assertEquals(getNumberOfNoneDaemonThreads(), runningThreads);
        } catch (Throwable e) {
            c.errorOut();
            throw e;
        } finally {
            c.discard();
            assertEquals(getNumberOfNoneDaemonThreads(), runningThreads);
        }
    }
    
    private int getNumberOfNoneDaemonThreads()
    {
        int count = 0;
        for (Thread t : Thread.getAllStackTraces().keySet()) {
            if (!t.isDaemon()) {
                count++;
            }
        }
        return count;
    }
}
