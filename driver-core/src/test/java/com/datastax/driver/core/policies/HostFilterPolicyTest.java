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

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;

import org.mockito.Matchers;
import com.google.common.base.Predicates;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import org.mockito.Mockito;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import static com.datastax.driver.core.TestUtils.*;

import com.datastax.driver.core.AbstractPoliciesTest;
import com.datastax.driver.core.CCMBridge;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

import org.testng.annotations.Test;

public class HostFilterPolicyTest extends AbstractPoliciesTest {

    private final InetSocketAddress host0 = InetSocketAddress.createUnresolved("192.168.1.1", 2345);
    private final InetSocketAddress host1 = InetSocketAddress.createUnresolved("192.168.1.2", 9876);
    private final InetSocketAddress host2 = InetSocketAddress.createUnresolved("192.168.1.3", 6666);
    
    private final List<InetSocketAddress> hosts = ImmutableList.of(host0, host1);

    private final List<String> dcs = ImmutableList.of("dc0", "dc1");

    @SuppressWarnings("deprecation")
    @Test(groups = "unit")
    public void testHostFilterPolicy() {
        
        Cluster cluster = Mockito.mock(Cluster.class);
        List<Host> hosts = ImmutableList.of(Mockito.mock(Host.class), Mockito.mock(Host.class));

        // Check that when the predicate returns true for a given host the policy acts as a pass through
        {
            Predicate<Host> predicate = Predicates.<Host>alwaysTrue();
            
            LoadBalancingPolicy ipolicy = Mockito.mock(LoadBalancingPolicy.class);
            when(ipolicy.distance(any(Host.class))).thenReturn(HostDistance.LOCAL);
            
            HostFilterPolicy policy = new HostFilterPolicy(ipolicy, predicate);
            
            Host host = Mockito.mock(Host.class);

            policy.onAdd(host);
            Mockito.verify(ipolicy, times(1)).onAdd(host);
            
            policy.onDown(host);
            Mockito.verify(ipolicy, times(1)).onDown(host);

            policy.onUp(host);
            Mockito.verify(ipolicy, times(1)).onUp(host);

            policy.onRemove(host);
            Mockito.verify(ipolicy, times(1)).onRemove(host);

            policy.onSuspected(host);
            Mockito.verify(ipolicy, times(1)).onSuspected(host);

            assertThat(policy.distance(host)).isSameAs(HostDistance.LOCAL);
            
            policy.init(cluster, hosts);
            Mockito.verify(ipolicy, times(1)).init(eq(cluster), eq(hosts));
        }
        
        // Check that when the predicate returns false for a given host the policy filters out every action
        {
            Predicate<Host> predicate = Predicates.<Host>alwaysFalse();
            
            LoadBalancingPolicy ipolicy = Mockito.mock(LoadBalancingPolicy.class);
            when(ipolicy.distance(any(Host.class))).thenReturn(HostDistance.LOCAL);

            HostFilterPolicy policy = new HostFilterPolicy(ipolicy, predicate);
            
            Host host = Mockito.mock(Host.class);
            
            policy.onAdd(host);
            Mockito.verify(ipolicy, never()).onAdd(host);
            
            policy.onDown(host);
            Mockito.verify(ipolicy, never()).onDown(host);

            policy.onUp(host);
            Mockito.verify(ipolicy, never()).onUp(host);

            policy.onRemove(host);
            Mockito.verify(ipolicy, never()).onRemove(host);

            policy.onSuspected(host);
            Mockito.verify(ipolicy, never()).onSuspected(host);

            assertThat(policy.distance(host)).isSameAs(HostDistance.IGNORED);
            
            try {
                policy.init(cluster, hosts);
                fail("should throw since no host can be passed to child policy");
            }
            catch( IllegalArgumentException e){
                Mockito.verify(ipolicy, never()).init(any(Cluster.class), Matchers.anyCollectionOf(Host.class));
            }
        }
    }
    
    @Test(groups = "unit")
    public void testNewQueryPlan() {
        
        List<Host> hosts = ImmutableList.of(Mockito.mock(Host.class), Mockito.mock(Host.class));
        
        LoadBalancingPolicy ipolicy = Mockito.mock(LoadBalancingPolicy.class);
        when(ipolicy.newQueryPlan(any(String.class), any(Statement.class))).thenReturn(hosts.iterator());
        
        HostFilterPolicy policy = new HostFilterPolicy(ipolicy, null);

        assertThat(policy.newQueryPlan("keyspace", Mockito.mock(Statement.class)).next()).isSameAs(hosts.get(0));
    }
    
    @Test(groups = "unit")
    public void testClose() {
        
        new HostFilterPolicy(null, null).close();

        CloseableLoadBalancingPolicy ipolicy = Mockito.mock(CloseableLoadBalancingPolicy.class);
        new HostFilterPolicy(ipolicy, null).close();
        Mockito.verify(ipolicy, times(1)).close();
    }
    
    @Test(groups = "unit")
    public void testMkHostAddressListPolicy() {
        
        LoadBalancingPolicy ipolicy = Mockito.mock(LoadBalancingPolicy.class);
        when(ipolicy.distance(any(Host.class))).thenReturn(HostDistance.LOCAL);

        Host hostInList = Mockito.mock(Host.class);
        when(hostInList.getSocketAddress()).thenReturn(host0);
        
        Host hostOutList = Mockito.mock(Host.class);
        when(hostOutList.getSocketAddress()).thenReturn(host2);
        
        // black list policy
        {
            HostFilterPolicy policy = HostFilterPolicy.mkHostAddressListPolicy(ipolicy, hosts, false);
            
            assertThat(policy.distance(hostInList)).isSameAs(HostDistance.IGNORED);
            assertThat(policy.distance(hostOutList)).isSameAs(HostDistance.LOCAL);
        }

        // white list policy
        {
            HostFilterPolicy policy = HostFilterPolicy.mkHostAddressListPolicy(ipolicy, hosts, true);
            
            assertThat(policy.distance(hostInList)).isSameAs(HostDistance.LOCAL);
            assertThat(policy.distance(hostOutList)).isSameAs(HostDistance.IGNORED);
        }

    }
    
    @Test(groups = "unit")
    public void testMkDCListPolicy() {
                
        LoadBalancingPolicy ipolicy = Mockito.mock(LoadBalancingPolicy.class);
        when(ipolicy.distance(any(Host.class))).thenReturn(HostDistance.LOCAL);

        Host hostInList = Mockito.mock(Host.class);
        when(hostInList.getDatacenter()).thenReturn("dc0");
        
        Host hostOutList = Mockito.mock(Host.class);
        when(hostOutList.getDatacenter()).thenReturn("dc2");
        
        Host hostNoDC = Mockito.mock(Host.class);
        when(hostNoDC.getDatacenter()).thenReturn(null);
        
        // black list policy
        {
            HostFilterPolicy policy = HostFilterPolicy.mkDCListPolicy(ipolicy, dcs, false);
            
            assertThat(policy.distance(hostInList)).isSameAs(HostDistance.IGNORED);
            assertThat(policy.distance(hostOutList)).isSameAs(HostDistance.LOCAL);
            assertThat(policy.distance(hostNoDC)).isSameAs(HostDistance.LOCAL);
        }

        // white list policy
        {
            HostFilterPolicy policy = HostFilterPolicy.mkDCListPolicy(ipolicy, dcs, true);
            
            assertThat(policy.distance(hostInList)).isSameAs(HostDistance.LOCAL);
            assertThat(policy.distance(hostOutList)).isSameAs(HostDistance.IGNORED);
            assertThat(policy.distance(hostNoDC)).isSameAs(HostDistance.LOCAL);
        }

    }
    
    @Test(groups = "unit")
    public void testMkHostAddressPredicate() {
        
        Predicate<Host> predicate = HostFilterPolicy.mkHostAddressPredicate(ImmutableList.of(host0, host1));

        {
            Host host = Mockito.mock(Host.class);
            Mockito.when( host.getSocketAddress()).thenReturn(host0);
            
            assertThat(predicate.apply(host)).isTrue();
        }

        {
            Host host = Mockito.mock(Host.class);
            Mockito.when( host.getSocketAddress()).thenReturn(host2);
            
            assertThat(predicate.apply(host)).isFalse();
        }
    }

    @Test(groups = "unit")
    public void testMkHostDCPredicate() {
        
        Predicate<Host> predicate = HostFilterPolicy.mkHostDCPredicate(dcs, true);

        {
            Host host = Mockito.mock(Host.class);
            Mockito.when( host.getDatacenter()).thenReturn("dc0");
            
            assertThat(predicate.apply(host)).isTrue();
        }

        {
            Host host = Mockito.mock(Host.class);
            Mockito.when( host.getDatacenter()).thenReturn("dc2");
            
            assertThat(predicate.apply(host)).isFalse();
        }

        {
            Host host = Mockito.mock(Host.class);
            Mockito.when( host.getDatacenter()).thenReturn(null);
            
            assertThat(predicate.apply(host)).isTrue();
            
            Predicate<Host> predicateFalse = HostFilterPolicy.mkHostDCPredicate(dcs, false);
            assertThat(predicateFalse.apply(host)).isFalse();
        }
    }

    @Test(groups = "long")
    public void DCAwareRoundRobinWithOneRemoteHostInForbiddenDCTest() throws Throwable {

        HostFilterPolicy policy = HostFilterPolicy.mkDCListPolicy(new DCAwareRoundRobinPolicy("dc2", 1, false), Arrays.asList("dc1"), false);

        Cluster.Builder builder = Cluster.builder().withLoadBalancingPolicy(policy);
        
        CCMBridge.CCMCluster c = CCMBridge.buildCluster(1, 1, builder);
        try {

            createMultiDCSchema(c.session);
            init(c, 12);
            query(c, 12);

            assertQueried(CCMBridge.IP_PREFIX + '1', 0);
            assertQueried(CCMBridge.IP_PREFIX + '2', 12);

            resetCoordinators();
            c.cassandraCluster.bootstrapNode(3, "dc3");
            waitFor(CCMBridge.IP_PREFIX + '3', c.cluster);

            query(c, 12);

            assertQueried(CCMBridge.IP_PREFIX + '1', 0);
            assertQueried(CCMBridge.IP_PREFIX + '2', 12);
            assertQueried(CCMBridge.IP_PREFIX + '3', 0);

            resetCoordinators();
            c.cassandraCluster.decommissionNode(2);
            waitForDecommission(CCMBridge.IP_PREFIX + '2', c.cluster);

            query(c, 12);

            assertQueried(CCMBridge.IP_PREFIX + '1', 0);
            assertQueried(CCMBridge.IP_PREFIX + '2', 0);
            assertQueried(CCMBridge.IP_PREFIX + '3', 12);

            resetCoordinators();
            c.cassandraCluster.forceStop(3);
            waitForDown(CCMBridge.IP_PREFIX + '3', c.cluster);

            try {
                query(c, 12);
                fail("");
            } catch (NoHostAvailableException e) {
                // No more nodes so ...
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
