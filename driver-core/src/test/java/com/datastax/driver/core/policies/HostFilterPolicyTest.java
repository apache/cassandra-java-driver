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
package com.datastax.driver.core.policies;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Statement;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;
import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

public class HostFilterPolicyTest {
    @Mock
    Cluster cluster;

    @Mock
    Host host1, host2, host3;

    InetSocketAddress address1 = InetSocketAddress.createUnresolved("192.168.1.1", 2345);
    InetSocketAddress address2 = InetSocketAddress.createUnresolved("192.168.1.2", 9876);
    InetSocketAddress address3 = InetSocketAddress.createUnresolved("192.168.1.3", 6666);

    @Mock
    LoadBalancingPolicy wrappedPolicy;

    @Captor
    ArgumentCaptor<Collection<Host>> hostsCaptor;

    @BeforeMethod(groups = "unit")
    public void setup() {
        MockitoAnnotations.initMocks(this);

        when(host1.getSocketAddress()).thenReturn(address1);
        when(host2.getSocketAddress()).thenReturn(address2);
        when(host3.getSocketAddress()).thenReturn(address3);

        when(wrappedPolicy.distance(any(Host.class))).thenReturn(HostDistance.LOCAL);
    }

    @Test(groups = "unit")
    public void should_delegate_to_wrapped_policy_when_predicate_is_true() {
        Predicate<Host> predicate = Predicates.alwaysTrue();
        HostFilterPolicy policy = new HostFilterPolicy(wrappedPolicy, predicate);

        policy.onAdd(host1);
        verify(wrappedPolicy).onAdd(host1);

        policy.onDown(host1);
        verify(wrappedPolicy).onDown(host1);

        policy.onUp(host1);
        verify(wrappedPolicy).onUp(host1);

        policy.onRemove(host1);
        verify(wrappedPolicy).onRemove(host1);

        assertThat(policy.distance(host1)).isSameAs(HostDistance.LOCAL);

        policy.close();
        verify(wrappedPolicy).close();
    }

    @Test(groups = "unit")
    public void should_not_delegate_to_wrapped_policy_when_predicate_is_false() {
        Predicate<Host> predicate = Predicates.alwaysFalse();
        HostFilterPolicy policy = new HostFilterPolicy(wrappedPolicy, predicate);

        policy.onAdd(host1);
        verify(wrappedPolicy, never()).onAdd(host1);

        policy.onDown(host1);
        verify(wrappedPolicy, never()).onDown(host1);

        policy.onUp(host1);
        verify(wrappedPolicy, never()).onUp(host1);

        policy.onRemove(host1);
        verify(wrappedPolicy, never()).onRemove(host1);

        assertThat(policy.distance(host1)).isSameAs(HostDistance.IGNORED);
    }

    @Test(groups = "unit")
    public void should_filter_init_hosts_with_predicate() {
        Predicate<Host> predicate = Predicates.in(Lists.newArrayList(host1, host2));
        HostFilterPolicy policy = new HostFilterPolicy(wrappedPolicy, predicate);

        policy.init(cluster, Lists.newArrayList(host1, host2, host3));

        verify(wrappedPolicy).init(eq(cluster), hostsCaptor.capture());
        assertThat(hostsCaptor.getValue()).containsOnly(host1, host2);
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
    public void should_throw_if_predicate_filters_out_all_init_hosts() {
        Predicate<Host> predicate = Predicates.alwaysFalse();
        HostFilterPolicy policy = new HostFilterPolicy(wrappedPolicy, predicate);

        policy.init(cluster, Lists.newArrayList(host1, host2, host3));
    }

    @Test(groups = "unit")
    public void should_return_query_plan_of_wrapped_policy() {
        when(wrappedPolicy.newQueryPlan(any(String.class), any(Statement.class)))
                .thenReturn(Iterators.forArray(host1, host2, host3));

        HostFilterPolicy policy = new HostFilterPolicy(wrappedPolicy, null);

        assertThat(policy.newQueryPlan("keyspace", mock(Statement.class)))
                .containsExactly(host1, host2, host3);
    }

    @Test(groups = "unit")
    public void should_ignore_DCs_in_black_list() {
        when(host1.getDatacenter()).thenReturn("dc1");
        when(host2.getDatacenter()).thenReturn("dc2");
        when(host3.getDatacenter()).thenReturn(null);

        HostFilterPolicy policy = HostFilterPolicy.fromDCBlackList(wrappedPolicy,
                Lists.newArrayList("dc2"));

        assertThat(policy.distance(host1)).isSameAs(HostDistance.LOCAL);
        assertThat(policy.distance(host2)).isSameAs(HostDistance.IGNORED);
        assertThat(policy.distance(host3)).isSameAs(HostDistance.LOCAL);
    }

    @Test(groups = "unit")
    public void should_ignore_DCs_not_in_white_list_and_not_null() {
        when(host1.getDatacenter()).thenReturn("dc1");
        when(host2.getDatacenter()).thenReturn("dc2");
        when(host3.getDatacenter()).thenReturn(null);

        HostFilterPolicy policy = HostFilterPolicy.fromDCWhiteList(wrappedPolicy,
                Lists.newArrayList("dc1"));

        assertThat(policy.distance(host1)).isSameAs(HostDistance.LOCAL);
        assertThat(policy.distance(host2)).isSameAs(HostDistance.IGNORED);
        assertThat(policy.distance(host3)).isSameAs(HostDistance.LOCAL);
    }
}
