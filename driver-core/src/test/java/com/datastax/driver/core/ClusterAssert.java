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
package com.datastax.driver.core;

import static org.assertj.core.api.Assertions.assertThat;
import org.assertj.core.api.AbstractAssert;

public class ClusterAssert extends AbstractAssert<ClusterAssert, Cluster> {
    protected ClusterAssert(Cluster actual) {
        super(actual, ClusterAssert.class);
    }

    public ClusterAssert usesControlHost(int node) {
        String expectedAddress = CCMBridge.ipOfNode(node);
        Host controlHost = actual.manager.controlConnection.connectedHost();
        assertThat(controlHost.getAddress().getHostAddress()).isEqualTo(expectedAddress);
        return this;
    }

    public HostAssert host(int hostNumber) {
        // TODO at some point this won't work anymore if we have assertions that wait for a node to
        // join the cluster, e.g. assertThat(cluster).node(3).comesUp().
        Host host = TestUtils.findHost(actual, hostNumber);

        return new HostAssert(host, actual);
    }
}
