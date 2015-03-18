package com.datastax.driver.core;

import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.Iterators;
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

    public ClusterAssert hasClosedControlConnection() {
        assertThat(actual.manager.controlConnection.isOpen()).isFalse();
        return this;
    }

    public HostAssert host(int hostNumber) {
        // TODO at some point this won't work anymore if we have assertions that wait for a node to
        // join the cluster, e.g. assertThat(cluster).node(3).comesUp().
        Host host = TestUtils.findHost(actual, hostNumber);

        return new HostAssert(host, actual);
    }

    /**
     * Asserts that {@link Cluster}'s {@link Host}s have valid {@link TokenRange}s with the given keyspace.
     *
     * Ensures that no ranges intersect and that they cover the entire ring.
     * @param keyspace Keyspace to grab {@link TokenRange}s from.
     */
    public ClusterAssert hasValidTokenRanges(String keyspace) {
        // Sort the token ranges so they are in order (needed for vnodes).
        Set<TokenRange> ranges = new TreeSet<TokenRange>();
        for(Host host : actual.getMetadata().getAllHosts()) {
            ranges.addAll(actual.getMetadata().getTokenRanges(keyspace, host));
        }
        return hasValidTokenRanges(ranges);
    }

    /**
     * Asserts that {@link Cluster}'s {@link Host}s have valid {@link TokenRange}s.
     *
     * Ensures that no ranges intersect and that they cover the entire ring.
     */
    public ClusterAssert hasValidTokenRanges() {
        // Sort the token ranges so they are in order (needed for vnodes).
        Set<TokenRange> ranges = new TreeSet<TokenRange>(actual.getMetadata().getTokenRanges());
        return hasValidTokenRanges(ranges);
    }

    /**
     * Asserts that given Set of {@link TokenRange}s are valid.
     *
     * Ensures that no ranges intersect and that they cover the entire ring.
     */
    private ClusterAssert hasValidTokenRanges(Set<TokenRange> ranges) {
        // Ensure no ranges intersect.
        Iterator<TokenRange> it = ranges.iterator();
        while(it.hasNext()) {
            TokenRange range = it.next();
            Assertions.assertThat(range).doesNotIntersect(Iterators.toArray(it, TokenRange.class));
        }

        // Ensure the defined ranges cover the entire ring.
        it = ranges.iterator();
        TokenRange mergedRange = it.next();
        while(it.hasNext()) {
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
