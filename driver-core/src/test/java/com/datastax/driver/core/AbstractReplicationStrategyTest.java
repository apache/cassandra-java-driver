package com.datastax.driver.core;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

public class AbstractReplicationStrategyTest {
	
	private static final Token.Factory partitioner = Token.getFactory("Murmur3Partitioner");
	
	protected static class HostMock extends Host {
		private String address;
		
		private HostMock(String address) throws UnknownHostException {
			super(InetAddress.getByName(address), new ConvictionPolicy.Simple.Factory());
			this.address = address;
		}
		
		private HostMock(String address, String dc, String rack) throws UnknownHostException {
			this(address);
			this.setLocationInfo(dc, rack);
		}
		
		@Override
		public String toString() {
			return address;
		}
		
		public String getMockAddress() {
			return address;
		}
	}
	
	protected static Token.Factory partitioner() {
		return partitioner;
	}
	
	protected static HostMock host(String address) {
		try {
			return new HostMock(address);
		} catch (UnknownHostException ex) {
			throw new RuntimeException(ex); //wrap to avoid declarations
		}
	}
	
	protected static HostMock host(String address, String dc, String rack) {
		try {
			return new HostMock(address, dc, rack);
		} catch (UnknownHostException ex) {
			throw new RuntimeException(ex); //wrap to avoid declarations
		}
	}
	
	protected static HostMock asMock(Host host) { 
		return (host instanceof HostMock ? (HostMock)host : null);
	}
	
	protected static String mockAddress(Host host) {
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
	
	protected static void assertReplicaPlacement(Map<Token, Set<Host>> replicaMap, Token token, String... expected) {
		Set<Host> replicaSet = replicaMap.get(token);
		assertNotNull(replicaSet);
		assertReplicasForToken(replicaSet, expected);
	}
	
	protected static void assertReplicasForToken(Set<Host> replicaSet, String... expected) {
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
}
