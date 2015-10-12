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
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

import org.mockito.Mockito;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.policies.Policies.Builder;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

public class PoliciesTest {

    @Test(groups = "unit")
    public void testBuildLoadBalancingWithFilters() {
        
        LoadBalancingPolicy ipolicy = Mockito.mock(LoadBalancingPolicy.class);
        when(ipolicy.distance(any(Host.class))).thenReturn(HostDistance.LOCAL);

        InetSocketAddress addr0 = InetSocketAddress.createUnresolved("192.168.1.1", 2345);
        InetSocketAddress addr1 = InetSocketAddress.createUnresolved("192.168.1.2", 9876);

        List<InetSocketAddress> okAddresses = ImmutableList.of(addr0);
        List<InetSocketAddress> koAddresses = ImmutableList.of(addr1);

        List<String> okDCs = ImmutableList.of("dc0");
        List<String> koDCs = ImmutableList.of("dc1");

        Builder builder = Policies.builder()
                        .withLoadBalancingPolicy(ipolicy)
                        .withBlackListedDCs(koDCs)
                        .withWhiteListedDCs(okDCs)
                        .withBlackListedHosts(koAddresses)
                        .withWhiteListedHosts(okAddresses);
        
        Policies policies = builder.build();

        LoadBalancingPolicy policy = policies.getLoadBalancingPolicy();
        
        // OK: white listed address and data center
        assertThat(policy.distance(mkHost(addr0, "dc0"))).isSameAs(HostDistance.LOCAL);
        
        // KO: black listed host and white listed data center
        assertThat(policy.distance(mkHost(addr1, "dc0"))).isSameAs(HostDistance.IGNORED);

        // KO: white listed host and black listed data center
        assertThat(policy.distance(mkHost(addr0, "dc1"))).isSameAs(HostDistance.IGNORED);

        // KO: black listed host and black listed data center
        assertThat(policy.distance(mkHost(addr1, "dc1"))).isSameAs(HostDistance.IGNORED);

        // OK: white listed host and undefined data center
        assertThat(policy.distance(mkHost(addr0, null))).isSameAs(HostDistance.LOCAL);
    }
    
    private static Host mkHost( InetSocketAddress addr, String dc ){
        Host host = Mockito.mock(Host.class);
        when(host.getSocketAddress()).thenReturn(addr);
        when(host.getDatacenter()).thenReturn(dc);

        return host;
    }
}
