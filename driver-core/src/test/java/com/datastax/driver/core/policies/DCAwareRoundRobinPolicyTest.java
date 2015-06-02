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
package com.datastax.driver.core.policies;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.testng.annotations.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

import com.datastax.driver.core.*;

public class DCAwareRoundRobinPolicyTest {
    Logger policyLogger = Logger.getLogger(DCAwareRoundRobinPolicy.class);
    MemoryAppender logs;
    CCMBridge ccm;

    @BeforeClass(groups = "short")
    public void createCcm() {
        // Two-DC cluster with one host in each DC
        ccm = CCMBridge.create("test", 1, 1);
    }

    @AfterClass(groups = "short")
    public void deleteCcm() {
        if (ccm != null)
            ccm.remove();
    }

    @BeforeMethod(groups = "short")
    public void startRecordingLogs() {
        policyLogger.setLevel(Level.WARN);
        logs = new MemoryAppender();
        policyLogger.addAppender(logs);
    }

    @AfterMethod(groups = "short")
    public void stopRecordingLogs() {
        policyLogger.setLevel(null);
        policyLogger.removeAppender(logs);
    }

    @Test(groups = "short")
    public void should_use_local_dc_from_contact_points_when_not_explicitly_specified() {
        Cluster cluster = null;
        CountingDCAwarePolicy policy = new CountingDCAwarePolicy();

        try {
            // Pass host1 as contact point
            cluster = Cluster.builder()
                .addContactPoint(CCMBridge.ipOfNode(1))
                .withLoadBalancingPolicy(policy)
                .build();
            cluster.init();

            Host host1 = TestUtils.findHost(cluster, 1);

            // Policy's localDC should be the one from the contact point
            assertThat(policy.getLocalDc()).isEqualTo(host1.getDatacenter());

            assertThat(policy.initHosts).containsExactly(host1);

            assertThat(logs.get())
                .doesNotContain("Some contact points don't match local data center");

        } finally {
            if (cluster != null)
                cluster.close();
        }
    }

    @Test(groups = "short")
    public void should_warn_if_contact_points_have_different_dcs_when_not_explicitly_specified() {
        Cluster cluster = null;
        CountingDCAwarePolicy policy = new CountingDCAwarePolicy();

        try {
            // Pass both hosts as contact points, they have different DCs
            cluster = Cluster.builder()
                .addContactPoints(CCMBridge.ipOfNode(1), CCMBridge.ipOfNode(2))
                .withLoadBalancingPolicy(policy)
                .build();
            cluster.init();

            Host host1 = TestUtils.findHost(cluster, 1);
            Host host2 = TestUtils.findHost(cluster, 2);

            assertThat(policy.initHosts).containsOnly(host1, host2);

            assertThat(logs.get())
                .contains("Some contact points don't match local data center");

        } finally {
            if (cluster != null)
                cluster.close();
        }
    }

    @Test(groups = "short")
    public void should_use_provided_local_dc_and_not_warn_if_contact_points_match() {
        Cluster cluster = null;
        String providedLocalDc = "dc1";
        CountingDCAwarePolicy policy = new CountingDCAwarePolicy(providedLocalDc);

        try {
            cluster = Cluster.builder()
                .addContactPoints(CCMBridge.ipOfNode(1))
                .withLoadBalancingPolicy(policy)
                .build();
            cluster.init();

            Host host1 = TestUtils.findHost(cluster, 1);

            assertEquals(policy.getLocalDc(), providedLocalDc);

            assertThat(policy.initHosts).containsExactly(host1);

            assertThat(logs.get())
                .doesNotContain("Some contact points don't match local data center");

        } finally {
            if (cluster != null)
                cluster.close();
        }
    }

    @Test(groups = "short")
    public void should_use_provided_local_dc_and_warn_if_contact_points_dont_match() {
        Cluster cluster = null;
        // Provide non-existent DC to make sure none of the contact points matches
        String providedLocalDc = "dc3";
        CountingDCAwarePolicy policy = new CountingDCAwarePolicy(providedLocalDc);

        try {
            cluster = Cluster.builder()
                .addContactPoints(CCMBridge.ipOfNode(1), CCMBridge.ipOfNode(2))
                .withLoadBalancingPolicy(policy)
                .build();
            cluster.init();

            Host host1 = TestUtils.findHost(cluster, 1);
            Host host2 = TestUtils.findHost(cluster, 2);

            assertEquals(policy.getLocalDc(), providedLocalDc);

            assertThat(policy.initHosts).containsOnly(host1, host2);

            assertThat(logs.get())
                .contains("Some contact points don't match local data center");

        } finally {
            if (cluster != null)
                cluster.close();
        }
    }

    /**
     * Wraps the policy under test to spy the calls to init.
     */
    static class CountingDCAwarePolicy extends DelegatingLoadBalancingPolicy {
        Set<Host> initHosts = new CopyOnWriteArraySet<Host>();

        CountingDCAwarePolicy() {
            super(new DCAwareRoundRobinPolicy());
        }

        CountingDCAwarePolicy(String localDc) {
            super(new DCAwareRoundRobinPolicy(localDc));
        }

        String getLocalDc() {
            return ((DCAwareRoundRobinPolicy) delegate).localDc;
        }

        @Override public void init(Cluster cluster, Collection<Host> hosts) {
            System.out.println("init " + hosts);
            initHosts.addAll(hosts);
            super.init(cluster, hosts);
        }
    }
}
