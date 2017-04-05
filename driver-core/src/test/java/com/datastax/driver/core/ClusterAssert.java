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

import com.google.common.collect.Iterators;
import org.assertj.core.api.AbstractAssert;

import java.net.InetAddress;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class ClusterAssert extends AbstractAssert<ClusterAssert, Cluster> {
    protected ClusterAssert(Cluster actual) {
        super(actual, ClusterAssert.class);
    }

    public ClusterAssert usesControlHost(int node) {
        String expectedAddress = TestUtils.ipOfNode(node);
        Host controlHost = actual.manager.controlConnection.connectedHost();
        assertThat(controlHost.getAddress().getHostAddress()).isEqualTo(expectedAddress);
        return this;
    }

    public ClusterAssert hasClosedControlConnection() {
        assertThat(actual.manager.controlConnection.isOpen()).isFalse();
        return this;
    }

    public ClusterAssert hasOpenControlConnection() {
        assertThat(actual.manager.controlConnection.isOpen()).isTrue();
        return this;
    }

    public HostAssert controlHost() {
        Host host = TestUtils.findOrWaitForControlConnection(actual, 10, TimeUnit.SECONDS);
        return new HostAssert(host, actual);
    }

    public HostAssert host(int hostNumber) {
        // Wait for the node to be added if it's not already known.
        // In 2.2+ C* does not send an added event until the node is ready so we wait a long time.
        Host host = TestUtils.findOrWaitForHost(actual, hostNumber,
                60 + Cluster.NEW_NODE_DELAY_SECONDS, TimeUnit.SECONDS);
        return new HostAssert(host, actual);
    }

    public HostAssert host(String hostAddress) {
        Host host = TestUtils.findOrWaitForHost(actual, hostAddress,
                60 + Cluster.NEW_NODE_DELAY_SECONDS, TimeUnit.SECONDS);
        return new HostAssert(host, actual);
    }

    public HostAssert host(InetAddress hostAddress) {
        return host(hostAddress.getHostAddress());
    }

    /**
     * Asserts that {@link Cluster}'s {@link Host}s have valid {@link TokenRange}s with the given keyspace.
     * <p/>
     * Ensures that no ranges intersect and that they cover the entire ring.
     *
     * @param keyspace Keyspace to grab {@link TokenRange}s from.
     */
    public ClusterAssert hasValidTokenRanges(String keyspace) {
        // Sort the token ranges so they are in order (needed for vnodes).
        Set<TokenRange> ranges = new TreeSet<TokenRange>();
        for (Host host : actual.getMetadata().getAllHosts()) {
            ranges.addAll(actual.getMetadata().getTokenRanges(keyspace, host));
        }
        return hasValidTokenRanges(ranges);
    }

    /**
     * Asserts that {@link Cluster}'s {@link Host}s have valid {@link TokenRange}s.
     * <p/>
     * Ensures that no ranges intersect and that they cover the entire ring.
     */
    public ClusterAssert hasValidTokenRanges() {
        // Sort the token ranges so they are in order (needed for vnodes).
        Set<TokenRange> ranges = new TreeSet<TokenRange>(actual.getMetadata().getTokenRanges());
        return hasValidTokenRanges(ranges);
    }

    /**
     * Asserts that given Set of {@link TokenRange}s are valid.
     * <p/>
     * Ensures that no ranges intersect and that they cover the entire ring.
     */
    private ClusterAssert hasValidTokenRanges(Set<TokenRange> ranges) {
        // Ensure no ranges intersect.
        Iterator<TokenRange> it = ranges.iterator();
        while (it.hasNext()) {
            TokenRange range = it.next();
            Assertions.assertThat(range).doesNotIntersect(Iterators.toArray(it, TokenRange.class));
        }

        // Ensure the defined ranges cover the entire ring.
        it = ranges.iterator();
        TokenRange mergedRange = it.next();
        while (it.hasNext()) {
            TokenRange next = it.next();
            mergedRange = mergedRange.mergeWith(next);
        }
        boolean isFullRing = mergedRange.getStart().equals(mergedRange.getEnd())
                && !mergedRange.isEmpty();
        assertThat(isFullRing)
                .as("Ring is not fully defined for Cluster.")
                .isTrue();

        return this;
    }
}
