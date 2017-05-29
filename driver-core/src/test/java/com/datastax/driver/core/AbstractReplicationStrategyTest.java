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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.*;

/**
 * Base class for replication strategy tests. Currently only supports testing
 * using the default Murmur3Partitioner.
 */
public class AbstractReplicationStrategyTest {

    private static final Token.Factory partitioner = Token.getFactory("Murmur3Partitioner");

    private static final Cluster.Manager mockManager = mock(Cluster.Manager.class);

    protected static class HostMock extends Host {
        private final InetSocketAddress address;

        private HostMock(InetSocketAddress address, Cluster.Manager manager) {
            super(address, new ConvictionPolicy.DefaultConvictionPolicy.Factory(), manager);
            this.address = address;
        }

        private HostMock(InetSocketAddress address, String dc, String rack, Cluster.Manager manager) {
            this(address, manager);
            this.setLocationInfo(dc, rack);
        }

        @Override
        public String toString() {
            return address.toString();
        }

        public InetSocketAddress getMockAddress() {
            return address;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof HostMock))
                return false;

            return address.equals(((HostMock) o).address);
        }

        @Override
        public int hashCode() {
            return address.hashCode();
        }
    }

    protected static Token.Factory partitioner() {
        return partitioner;
    }

    /**
     * Convenience method to quickly create a mock host by a given address.
     * Specified address must be accessible, otherwise a RuntimeException is thrown
     */
    protected static HostMock host(InetSocketAddress address) {
        return new HostMock(address, mockManager);
    }

    /**
     * Convenience method to quickly create a mock host by the given address
     * located in the given datacenter/rack
     */
    protected static HostMock host(InetSocketAddress address, String dc, String rack) {
        return new HostMock(address, dc, rack, mockManager);
    }

    /**
     * Convenience method to cast a Host object into a MockHost.
     * Returns null if parameter host is not a mock
     */
    protected static HostMock asMock(Host host) {
        return (host instanceof HostMock ? (HostMock) host : null);
    }

    /**
     * Convenience method to quickly retrieve a mock host's address as specified
     * if created by the <code>host(...)</code> methods. Returns null if
     * given host is not a mock.
     */
    protected static InetSocketAddress mockAddress(Host host) {
        HostMock mock = asMock(host);
        return mock == null ? null : mock.getMockAddress();
    }

    protected static Token token(String value) {
        return partitioner.fromString(value);
    }

    protected static List<Token> tokens(String... values) {
        Builder<Token> builder = ImmutableList.<Token>builder();
        for (String value : values) {
            builder.add(token(value));
        }
        return builder.build();
    }

    /**
     * Asserts that the replica map for a given token contains the expected list of replica hosts.
     * Hosts are checked in order, replica placement should be an ordered set
     */
    protected static void assertReplicaPlacement(Map<Token, Set<Host>> replicaMap, Token token, InetSocketAddress... expected) {
        Set<Host> replicaSet = replicaMap.get(token);
        assertNotNull(replicaSet);
        assertReplicasForToken(replicaSet, expected);
    }

    /**
     * Checks if a given ordered set of replicas matches the expected list of replica hosts
     */
    protected static void assertReplicasForToken(Set<Host> replicaSet, InetSocketAddress... expected) {
        final String message = "Contents of replica set: " + replicaSet + " do not match expected hosts: " + Arrays.toString(expected);
        assertEquals(replicaSet.size(), expected.length, message);

        int i = 0;
        for (Host hostReturned : replicaSet) {
            boolean match = true;

            if (!expected[i++].equals(mockAddress(hostReturned))) {
                match = false;
            }
            assertTrue(match, message);
        }
    }

    protected static InetSocketAddress socketAddress(String address) {
        try {
            return new InetSocketAddress(InetAddress.getByName(address), 9042);
        } catch (UnknownHostException ex) {
            throw new RuntimeException(ex);
        }
    }
}
