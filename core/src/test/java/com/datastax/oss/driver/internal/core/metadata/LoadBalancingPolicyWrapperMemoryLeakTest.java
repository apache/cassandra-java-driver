/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.internal.core.metadata;

import static com.datastax.oss.driver.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.*;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistanceEvaluator;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.DefaultConsistencyLevelRegistry;
import com.datastax.oss.driver.internal.core.context.EventBus;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.loadbalancing.BasicLoadBalancingPolicy;
import com.datastax.oss.driver.internal.core.metrics.MetricsFactory;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;

import java.lang.ref.WeakReference;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class LoadBalancingPolicyWrapperMemoryLeakTest {

    private DefaultNode weakNode1;

    private DefaultNode weakNode2;

    @Mock
    private LoadBalancingPolicy.DistanceReporter distanceReporter;

    @Mock
    private InternalDriverContext context;

    @Mock
    private DriverConfig config;

    @Mock
    protected DriverExecutionProfile defaultProfile;

    @Mock
    private NodeDistanceEvaluator nodeDistanceEvaluator;
    @Mock
    private MetadataManager metadataManager;
    @Mock
    private Metadata metadata;

    @Mock
    protected MetricsFactory metricsFactory;

    private LoadBalancingPolicyWrapper wrapper;

    @Before
    public void setup() {

        when(context.getMetricsFactory()).thenReturn(metricsFactory);
        when(context.getSessionName()).thenReturn("test");
        when(context.getConfig()).thenReturn(config);
        when(context.getConsistencyLevelRegistry()).thenReturn(new DefaultConsistencyLevelRegistry());
        when(context.getNodeDistanceEvaluator(DriverExecutionProfile.DEFAULT_NAME))
                .thenReturn(nodeDistanceEvaluator);
        when(config.getProfile(DriverExecutionProfile.DEFAULT_NAME)).thenReturn(defaultProfile);
        when(defaultProfile.getString(DefaultDriverOption.REQUEST_CONSISTENCY)).thenReturn("ONE");
        when(defaultProfile.getName()).thenReturn(DriverExecutionProfile.DEFAULT_NAME);

        weakNode1 = TestNodeFactory.newNode(11, context);
        weakNode2 = TestNodeFactory.newNode(12, context);

        Map<UUID, Node> allNodes = new HashMap<>();
        allNodes.put(UUID.randomUUID(), weakNode1);
        allNodes.put(UUID.randomUUID(), weakNode2);

        when(metadataManager.getMetadata()).thenReturn(metadata);
        when(metadata.getNodes()).thenReturn(allNodes);

        when(context.getMetadataManager()).thenReturn(metadataManager);
        EventBus eventBus = new EventBus("test");
        when(context.getEventBus()).thenReturn(eventBus);
        BasicLoadBalancingPolicy policy = new BasicLoadBalancingPolicy(context, DriverExecutionProfile.DEFAULT_NAME);
        wrapper =
                new LoadBalancingPolicyWrapper(
                        context,
                        ImmutableMap.of(
                                DriverExecutionProfile.DEFAULT_NAME,
                                policy));

    }

    @Test
    public void should_garbage_collect_without_strong_references() {

        // given that
        given(nodeDistanceEvaluator.evaluateDistance(weakNode1, null)).willReturn(NodeDistance.IGNORED);
        given(nodeDistanceEvaluator.evaluateDistance(weakNode2, null)).willReturn(NodeDistance.IGNORED);

        // weak references to poke the private WeakHashMap in LoadBalancingPolicyWrapper.distances
        WeakReference<DefaultNode> weakReference1 = new WeakReference<>(weakNode1);
        WeakReference<DefaultNode> weakReference2 = new WeakReference<>(weakNode2);

        wrapper.init();

        // remove all the strong references, including the ones held by Mockito
        weakNode2 = null;
        reset(metricsFactory);
        reset(distanceReporter);
        reset(nodeDistanceEvaluator);
        reset(metadata);

        // verify
        System.gc();
        assertThat(weakReference1.get()).isNotNull();
        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> weakReference2.get() == null);
    }
}
