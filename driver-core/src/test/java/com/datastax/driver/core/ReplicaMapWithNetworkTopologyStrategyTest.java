package com.datastax.driver.core;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

public class ReplicaMapWithNetworkTopologyStrategyTest extends AbstractReplicationStrategyTest {
	
	private static class ReplicationFactorDefinition {
		public final String dc;
		public final int replicationFactor;

		public ReplicationFactorDefinition(String dc, int replicationFactor) {
			this.dc = dc;
			this.replicationFactor = replicationFactor;
		}
	}
	
	private static ReplicationFactorDefinition rf(String dc, int replicationFactor) {
		return new ReplicationFactorDefinition(dc, replicationFactor);
	}
	
	private static ReplicationStrategy networkTopologyStrategy(ReplicationFactorDefinition... rfs) {
		Builder<String, String> builder = ImmutableMap.<String, String>builder()//
				.put("class", "NetworkTopologyStrategy");
		
		for (ReplicationFactorDefinition rf : rfs) {
			builder.put(rf.dc, String.valueOf(rf.replicationFactor));
		}
		
		return ReplicationStrategy.create(builder.build());
	}
	
	/*
	 * ---------------------------------------------------------------------------
	 * Ring, replication, etc... setup. These are reusable for the tests
	 * This data is based on a real ring topology. Most tests are using
	 * smaller and more specific topologies instead.
	 * ---------------------------------------------------------------------------
	 */
	
	private static final String DC1 = "DC1";
	private static final String DC2 = "DC2";
	private static final String DC3 = "DC3";
	private static final String RACK11 = "RACK11";
	private static final String RACK12 = "RACK12";
	private static final String RACK21 = "RACK21";
	private static final String RACK22 = "RACK22";
	private static final String RACK31 = "RACK31";
	
	private static final ReplicationStrategy exampleStrategy = networkTopologyStrategy(rf(DC1, 2), rf(DC2, 2));
	
	private static final ReplicationStrategy exampleStrategyTooManyReplicas = networkTopologyStrategy(rf(DC1, 4), rf(DC2, 4));

	private static final List<Token> exampleRing = ImmutableList.<Token>builder()
			.add(token("-9000000000000000000"))//
			.add(token("-8000000000000000000"))// 
			.add(token("-7000000000000000000"))//
			.add(token("-6000000000000000000"))//
			.add(token("-5000000000000000000"))//
			.add(token("-4000000000000000000"))//
			.add(token("-3000000000000000000"))//
			.add(token("-2000000000000000000"))//
			.add(token("-1000000000000000000"))//
			.add(token("0"))//
			.add(token("1000000000000000000"))//
			.add(token("2000000000000000000"))//
			.add(token("3000000000000000000"))//
			.add(token("4000000000000000000"))//
			.add(token("5000000000000000000"))//
			.add(token("6000000000000000000"))//
			.add(token("7000000000000000000"))//
			.add(token("8000000000000000000"))//
			.build();
	
	private static final Map<Token, Host> exampleTokenToPrimary = ImmutableMap.<Token, Host>builder()
			.put(token("-9000000000000000000"), host("127.0.0.101", DC1, RACK11))//
			.put(token("-8000000000000000000"), host("127.0.0.101", DC1, RACK11))//
			.put(token("-7000000000000000000"), host("127.0.0.105", DC1, RACK12))//
			.put(token("-6000000000000000000"), host("127.0.0.103", DC1, RACK11))//
			.put(token("-5000000000000000000"), host("127.0.0.101", DC1, RACK11))//
			.put(token("-4000000000000000000"), host("127.0.0.105", DC1, RACK12))//
			.put(token("-3000000000000000000"), host("127.0.0.102", DC2, RACK21))//
			.put(token("-2000000000000000000"), host("127.0.0.106", DC2, RACK22))//
			.put(token("-1000000000000000000"), host("127.0.0.103", DC1, RACK11))//
			.put(token("0"),                    host("127.0.0.104", DC2, RACK21))//
			.put(token("1000000000000000000"), host("127.0.0.105", DC1, RACK12))//
			.put(token("2000000000000000000"), host("127.0.0.104", DC2, RACK21))//
			.put(token("3000000000000000000"), host("127.0.0.104", DC2, RACK21))//
			.put(token("4000000000000000000"), host("127.0.0.102", DC2, RACK21))//
			.put(token("5000000000000000000"), host("127.0.0.106", DC2, RACK22))//
			.put(token("6000000000000000000"), host("127.0.0.103", DC1, RACK11))//
			.put(token("7000000000000000000"), host("127.0.0.102", DC2, RACK21))//
			.put(token("8000000000000000000"), host("127.0.0.106", DC2, RACK22))//
			.build();
	
	/*
	 * --------------
	 *     Tests
	 * --------------
	 */
	
	@Test(groups = "unit")
	public void networkTopologyWithSimpleDCLayoutTest1() {
		List<Token> ring = ImmutableList.<Token>builder()
			.add(token("-9000000000000000000"))
			.add(token("-4000000000000000000"))
			.add(token("4000000000000000000"))
			.add(token("9000000000000000000"))
			.build();
		
		Map<Token, Host> tokenToPrimary = ImmutableMap.<Token, Host>builder()
			.put(token("-9000000000000000000"), host("127.0.0.101", DC1, RACK11))
			.put(token("-4000000000000000000"), host("127.0.0.102", DC2, RACK21))
			.put(token("4000000000000000000"), host("127.0.0.101", DC1, RACK11))
			.put(token("9000000000000000000"), host("127.0.0.102", DC2, RACK21))
			.build();
		
		ReplicationStrategy strategy = networkTopologyStrategy(rf(DC1, 1), rf(DC2, 1));
		
		Map<Token, Set<Host>> replicaMap = strategy.computeTokenToReplicaMap(tokenToPrimary, ring);
		
		assertReplicaPlacement(replicaMap, token("-9000000000000000000"), "127.0.0.101", "127.0.0.102");
		assertReplicaPlacement(replicaMap, token("-4000000000000000000"), "127.0.0.102", "127.0.0.101");
		assertReplicaPlacement(replicaMap, token("4000000000000000000"), "127.0.0.101", "127.0.0.102");
		assertReplicaPlacement(replicaMap, token("9000000000000000000"), "127.0.0.102", "127.0.0.101");
	}
	
	@Test(groups = "unit")
	public void networkTopologyWithSimpleDCLayoutTest2() {
		List<Token> ring = ImmutableList.<Token>builder()
			.add(token("-9000000000000000000"))
			.add(token("-7000000000000000000"))
			.add(token("-5000000000000000000"))
			.add(token("-3000000000000000000"))
			.add(token("3000000000000000000"))
			.add(token("5000000000000000000"))
			.add(token("7000000000000000000"))
			.add(token("9000000000000000000"))
			.build();
		
		Map<Token, Host> tokenToPrimary = ImmutableMap.<Token, Host>builder()
			.put(token("-9000000000000000000"), host("127.0.0.101", DC1, RACK11))
			.put(token("-7000000000000000000"), host("127.0.0.102", DC2, RACK21))
			.put(token("-5000000000000000000"), host("127.0.0.103", DC1, RACK11))
			.put(token("-3000000000000000000"), host("127.0.0.104", DC2, RACK21))
			.put(token("3000000000000000000"), host("127.0.0.101", DC1, RACK11))
			.put(token("5000000000000000000"), host("127.0.0.102", DC2, RACK21))
			.put(token("7000000000000000000"), host("127.0.0.103", DC1, RACK11))
			.put(token("9000000000000000000"), host("127.0.0.104", DC2, RACK21))
			.build();
		
		ReplicationStrategy strategy = networkTopologyStrategy(rf(DC1, 1), rf(DC2, 1));
		
		Map<Token, Set<Host>> replicaMap = strategy.computeTokenToReplicaMap(tokenToPrimary, ring);
		
		assertReplicaPlacement(replicaMap, token("-9000000000000000000"), "127.0.0.101", "127.0.0.102");
		assertReplicaPlacement(replicaMap, token("-7000000000000000000"), "127.0.0.102", "127.0.0.103");
		assertReplicaPlacement(replicaMap, token("-5000000000000000000"), "127.0.0.103", "127.0.0.104");
		assertReplicaPlacement(replicaMap, token("-3000000000000000000"), "127.0.0.104", "127.0.0.101");
		assertReplicaPlacement(replicaMap, token("3000000000000000000"), "127.0.0.101", "127.0.0.102");
		assertReplicaPlacement(replicaMap, token("5000000000000000000"), "127.0.0.102", "127.0.0.103");
		assertReplicaPlacement(replicaMap, token("7000000000000000000"), "127.0.0.103", "127.0.0.104");
		assertReplicaPlacement(replicaMap, token("9000000000000000000"), "127.0.0.104", "127.0.0.101");
	}
	
	@Test(groups = "unit")
	public void networkTopologyWithSimple3DCLayoutTest() {
		List<Token> ring = ImmutableList.<Token>builder()
			.add(token("-9000000000000000000"))
			.add(token("-5000000000000000000"))
			.add(token("-1000000000000000000"))
			.add(token("1000000000000000000"))
			.add(token("5000000000000000000"))
			.add(token("9000000000000000000"))
			.build();
		
		Map<Token, Host> tokenToPrimary = ImmutableMap.<Token, Host>builder()
			.put(token("-9000000000000000000"), host("127.0.0.101", DC1, RACK11))
			.put(token("-5000000000000000000"), host("127.0.0.102", DC2, RACK21))
			.put(token("-1000000000000000000"), host("127.0.0.103", DC3, RACK31))
			.put(token("1000000000000000000"), host("127.0.0.101", DC1, RACK11))
			.put(token("5000000000000000000"), host("127.0.0.102", DC2, RACK21))
			.put(token("9000000000000000000"), host("127.0.0.103", DC3, RACK31))
			.build();
		
		ReplicationStrategy strategy = networkTopologyStrategy(rf(DC1, 1), rf(DC2, 1), rf(DC3, 1));
		
		Map<Token, Set<Host>> replicaMap = strategy.computeTokenToReplicaMap(tokenToPrimary, ring);
		
		assertReplicaPlacement(replicaMap, token("-9000000000000000000"), "127.0.0.101", "127.0.0.102", "127.0.0.103");
		assertReplicaPlacement(replicaMap, token("-5000000000000000000"), "127.0.0.102", "127.0.0.103", "127.0.0.101");
		assertReplicaPlacement(replicaMap, token("-1000000000000000000"), "127.0.0.103", "127.0.0.101", "127.0.0.102");
		assertReplicaPlacement(replicaMap, token("1000000000000000000"), "127.0.0.101", "127.0.0.102", "127.0.0.103");
		assertReplicaPlacement(replicaMap, token("5000000000000000000"), "127.0.0.102", "127.0.0.103", "127.0.0.101");
		assertReplicaPlacement(replicaMap, token("9000000000000000000"), "127.0.0.103", "127.0.0.101", "127.0.0.102");
	}
	
	@Test(groups = "unit")
	public void networkTopologyWithUnbalancedRingTest() {
		List<Token> ring = ImmutableList.<Token>builder()
			.add(token("-9000000000000000000"))
			.add(token("-7000000000000000000"))
			.add(token("-5000000000000000000"))
			.add(token("-3000000000000000000"))
			.add(token("-1000000000000000000"))
			.add(token("1000000000000000000"))
			.add(token("3000000000000000000"))
			.add(token("5000000000000000000"))
			.add(token("7000000000000000000"))
			.add(token("9000000000000000000"))
			.build();
		
		Map<Token, Host> tokenToPrimary = ImmutableMap.<Token, Host>builder()
			.put(token("-9000000000000000000"), host("127.0.0.101", DC1, RACK11))
			.put(token("-7000000000000000000"), host("127.0.0.101", DC1, RACK11))
			.put(token("-5000000000000000000"), host("127.0.0.102", DC2, RACK21))
			.put(token("-3000000000000000000"), host("127.0.0.103", DC1, RACK11))
			.put(token("-1000000000000000000"), host("127.0.0.104", DC2, RACK21))
			.put(token("1000000000000000000"), host("127.0.0.101", DC1, RACK11))
			.put(token("3000000000000000000"), host("127.0.0.101", DC1, RACK11))
			.put(token("5000000000000000000"), host("127.0.0.102", DC2, RACK21))
			.put(token("7000000000000000000"), host("127.0.0.103", DC1, RACK11))
			.put(token("9000000000000000000"), host("127.0.0.104", DC2, RACK21))
			.build();
		
		ReplicationStrategy strategy = networkTopologyStrategy(rf(DC1, 2), rf(DC2, 2));
		
		Map<Token, Set<Host>> replicaMap = strategy.computeTokenToReplicaMap(tokenToPrimary, ring);
		
		assertReplicaPlacement(replicaMap, token("-9000000000000000000"), "127.0.0.101", "127.0.0.102", "127.0.0.103", "127.0.0.104");
		assertReplicaPlacement(replicaMap, token("-7000000000000000000"), "127.0.0.101", "127.0.0.102", "127.0.0.103", "127.0.0.104");
		assertReplicaPlacement(replicaMap, token("-5000000000000000000"), "127.0.0.102", "127.0.0.103", "127.0.0.104", "127.0.0.101");
		assertReplicaPlacement(replicaMap, token("-3000000000000000000"), "127.0.0.103", "127.0.0.104", "127.0.0.101", "127.0.0.102");
		assertReplicaPlacement(replicaMap, token("-1000000000000000000"), "127.0.0.104", "127.0.0.101", "127.0.0.102", "127.0.0.103");
		assertReplicaPlacement(replicaMap, token("1000000000000000000"), "127.0.0.101", "127.0.0.102", "127.0.0.103", "127.0.0.104");
		assertReplicaPlacement(replicaMap, token("3000000000000000000"), "127.0.0.101", "127.0.0.102", "127.0.0.103", "127.0.0.104");
		assertReplicaPlacement(replicaMap, token("5000000000000000000"), "127.0.0.102", "127.0.0.103", "127.0.0.104", "127.0.0.101");
		assertReplicaPlacement(replicaMap, token("7000000000000000000"), "127.0.0.103", "127.0.0.104", "127.0.0.101", "127.0.0.102");
		assertReplicaPlacement(replicaMap, token("9000000000000000000"), "127.0.0.104", "127.0.0.101", "127.0.0.102", "127.0.0.103");
	}
	
	@Test(groups = "unit")
	public void networkTopologyWithDCMultirackLayoutTest() {
		List<Token> ring = ImmutableList.<Token>builder()
			.add(token("-9000000000000000000"))
			.add(token("-8000000000000000000"))
			.add(token("-7000000000000000000"))
			.add(token("-6000000000000000000"))
			.add(token("-5000000000000000000"))
			.add(token("-4000000000000000000"))
			.add(token("-3000000000000000000"))
			.add(token("-2000000000000000000"))
			.add(token("2000000000000000000"))
			.add(token("3000000000000000000"))
			.add(token("4000000000000000000"))
			.add(token("5000000000000000000"))
			.add(token("6000000000000000000"))
			.add(token("7000000000000000000"))
			.add(token("8000000000000000000"))
			.add(token("9000000000000000000"))
			.build();
		
		Map<Token, Host> tokenToPrimary = ImmutableMap.<Token, Host>builder()
			.put(token("-9000000000000000000"), host("127.0.0.101", DC1, RACK11))
			.put(token("-8000000000000000000"), host("127.0.0.102", DC2, RACK21))
			.put(token("-7000000000000000000"), host("127.0.0.103", DC1, RACK12))
			.put(token("-6000000000000000000"), host("127.0.0.104", DC2, RACK22))
			.put(token("-5000000000000000000"), host("127.0.0.105", DC1, RACK11))
			.put(token("-4000000000000000000"), host("127.0.0.106", DC2, RACK21))
			.put(token("-3000000000000000000"), host("127.0.0.107", DC1, RACK12))
			.put(token("-2000000000000000000"), host("127.0.0.108", DC2, RACK22))
			.put(token("2000000000000000000"), host("127.0.0.101", DC1, RACK11))
			.put(token("3000000000000000000"), host("127.0.0.102", DC2, RACK21))
			.put(token("4000000000000000000"), host("127.0.0.103", DC1, RACK12))
			.put(token("5000000000000000000"), host("127.0.0.104", DC2, RACK22))
			.put(token("6000000000000000000"), host("127.0.0.105", DC1, RACK11))
			.put(token("7000000000000000000"), host("127.0.0.106", DC2, RACK21))
			.put(token("8000000000000000000"), host("127.0.0.107", DC1, RACK12))
			.put(token("9000000000000000000"), host("127.0.0.108", DC2, RACK22))
			.build();
		
		ReplicationStrategy strategy = networkTopologyStrategy(rf(DC1, 2), rf(DC2, 2));
		
		Map<Token, Set<Host>> replicaMap = strategy.computeTokenToReplicaMap(tokenToPrimary, ring);
		
		assertReplicaPlacement(replicaMap, token("-9000000000000000000"), "127.0.0.101", "127.0.0.102", "127.0.0.103", "127.0.0.104");
		assertReplicaPlacement(replicaMap, token("-8000000000000000000"), "127.0.0.102", "127.0.0.103", "127.0.0.104", "127.0.0.105");
		assertReplicaPlacement(replicaMap, token("-7000000000000000000"), "127.0.0.103", "127.0.0.104", "127.0.0.105", "127.0.0.106");
		assertReplicaPlacement(replicaMap, token("-6000000000000000000"), "127.0.0.104", "127.0.0.105", "127.0.0.106", "127.0.0.107");
		assertReplicaPlacement(replicaMap, token("-5000000000000000000"), "127.0.0.105", "127.0.0.106", "127.0.0.107", "127.0.0.108");
		assertReplicaPlacement(replicaMap, token("-4000000000000000000"), "127.0.0.106", "127.0.0.107", "127.0.0.108", "127.0.0.101");
		assertReplicaPlacement(replicaMap, token("-3000000000000000000"), "127.0.0.107", "127.0.0.108", "127.0.0.101", "127.0.0.102");
		assertReplicaPlacement(replicaMap, token("-2000000000000000000"), "127.0.0.108", "127.0.0.101", "127.0.0.102", "127.0.0.103");
		assertReplicaPlacement(replicaMap, token("2000000000000000000"), "127.0.0.101", "127.0.0.102", "127.0.0.103", "127.0.0.104");
		assertReplicaPlacement(replicaMap, token("3000000000000000000"), "127.0.0.102", "127.0.0.103", "127.0.0.104", "127.0.0.105");
		assertReplicaPlacement(replicaMap, token("4000000000000000000"), "127.0.0.103", "127.0.0.104", "127.0.0.105", "127.0.0.106");
		assertReplicaPlacement(replicaMap, token("5000000000000000000"), "127.0.0.104", "127.0.0.105", "127.0.0.106", "127.0.0.107");
		assertReplicaPlacement(replicaMap, token("6000000000000000000"), "127.0.0.105", "127.0.0.106", "127.0.0.107", "127.0.0.108");
		assertReplicaPlacement(replicaMap, token("7000000000000000000"), "127.0.0.106", "127.0.0.107", "127.0.0.108", "127.0.0.101");
		assertReplicaPlacement(replicaMap, token("8000000000000000000"), "127.0.0.107", "127.0.0.108", "127.0.0.101", "127.0.0.102");
		assertReplicaPlacement(replicaMap, token("9000000000000000000"), "127.0.0.108", "127.0.0.101", "127.0.0.102", "127.0.0.103");
	}
	
	@Test(groups = "unit")
	public void networkTopologyWithMultirackHostSkippingTest1() {
		List<Token> ring = ImmutableList.<Token>builder()
			.add(token("-9000000000000000000"))
			.add(token("-8000000000000000000"))
			.add(token("-7000000000000000000"))
			.add(token("-6000000000000000000"))
			.add(token("-5000000000000000000"))
			.add(token("-4000000000000000000"))
			.add(token("-3000000000000000000"))
			.add(token("-2000000000000000000"))
			.add(token("2000000000000000000"))
			.add(token("3000000000000000000"))
			.add(token("4000000000000000000"))
			.add(token("5000000000000000000"))
			.add(token("6000000000000000000"))
			.add(token("7000000000000000000"))
			.add(token("8000000000000000000"))
			.add(token("9000000000000000000"))
			.build();
		
		//this is to simulate when we hit the same rack in a DC first as a second replica
		//so that'll get skipped and re-added later as a third
		Map<Token, Host> tokenToPrimary = ImmutableMap.<Token, Host>builder()
			.put(token("-9000000000000000000"), host("127.0.0.101", DC1, RACK11))
			.put(token("-8000000000000000000"), host("127.0.0.102", DC2, RACK21))
			.put(token("-7000000000000000000"), host("127.0.0.103", DC1, RACK11))
			.put(token("-6000000000000000000"), host("127.0.0.104", DC2, RACK21))
			.put(token("-5000000000000000000"), host("127.0.0.105", DC1, RACK12))
			.put(token("-4000000000000000000"), host("127.0.0.106", DC2, RACK22))
			.put(token("-3000000000000000000"), host("127.0.0.107", DC1, RACK12))
			.put(token("-2000000000000000000"), host("127.0.0.108", DC2, RACK22))
			.put(token("2000000000000000000"), host("127.0.0.101", DC1, RACK11))
			.put(token("3000000000000000000"), host("127.0.0.102", DC2, RACK21))
			.put(token("4000000000000000000"), host("127.0.0.103", DC1, RACK11))
			.put(token("5000000000000000000"), host("127.0.0.104", DC2, RACK21))
			.put(token("6000000000000000000"), host("127.0.0.105", DC1, RACK12))
			.put(token("7000000000000000000"), host("127.0.0.106", DC2, RACK22))
			.put(token("8000000000000000000"), host("127.0.0.107", DC1, RACK12))
			.put(token("9000000000000000000"), host("127.0.0.108", DC2, RACK22))
			.build();
		
		ReplicationStrategy strategy = networkTopologyStrategy(rf(DC1, 3), rf(DC2, 3));
		
		Map<Token, Set<Host>> replicaMap = strategy.computeTokenToReplicaMap(tokenToPrimary, ring);
		
		assertReplicaPlacement(replicaMap, token("-9000000000000000000"), "127.0.0.101", "127.0.0.102", "127.0.0.105", "127.0.0.103", "127.0.0.106", "127.0.0.104");
		assertReplicaPlacement(replicaMap, token("-8000000000000000000"), "127.0.0.102", "127.0.0.103", "127.0.0.105", "127.0.0.106", "127.0.0.104", "127.0.0.107");
		assertReplicaPlacement(replicaMap, token("-7000000000000000000"), "127.0.0.103", "127.0.0.104", "127.0.0.105", "127.0.0.106", "127.0.0.107", "127.0.0.108");
		assertReplicaPlacement(replicaMap, token("-6000000000000000000"), "127.0.0.104", "127.0.0.105", "127.0.0.106", "127.0.0.108", "127.0.0.101", "127.0.0.107");
		assertReplicaPlacement(replicaMap, token("-5000000000000000000"), "127.0.0.105", "127.0.0.106", "127.0.0.101", "127.0.0.107", "127.0.0.102", "127.0.0.108");
		assertReplicaPlacement(replicaMap, token("-4000000000000000000"), "127.0.0.106", "127.0.0.107", "127.0.0.101", "127.0.0.102", "127.0.0.108", "127.0.0.103");
		assertReplicaPlacement(replicaMap, token("-3000000000000000000"), "127.0.0.107", "127.0.0.108", "127.0.0.101", "127.0.0.102", "127.0.0.103", "127.0.0.104");
		assertReplicaPlacement(replicaMap, token("-2000000000000000000"), "127.0.0.108", "127.0.0.101", "127.0.0.102", "127.0.0.104", "127.0.0.105", "127.0.0.103");
		assertReplicaPlacement(replicaMap, token("2000000000000000000"), "127.0.0.101", "127.0.0.102", "127.0.0.105", "127.0.0.103", "127.0.0.106", "127.0.0.104");
		assertReplicaPlacement(replicaMap, token("3000000000000000000"), "127.0.0.102", "127.0.0.103", "127.0.0.105", "127.0.0.106", "127.0.0.104", "127.0.0.107");
		assertReplicaPlacement(replicaMap, token("4000000000000000000"), "127.0.0.103", "127.0.0.104", "127.0.0.105", "127.0.0.106", "127.0.0.107", "127.0.0.108");
		assertReplicaPlacement(replicaMap, token("5000000000000000000"), "127.0.0.104", "127.0.0.105", "127.0.0.106", "127.0.0.108", "127.0.0.101", "127.0.0.107");
		assertReplicaPlacement(replicaMap, token("6000000000000000000"), "127.0.0.105", "127.0.0.106", "127.0.0.101", "127.0.0.107", "127.0.0.102", "127.0.0.108");
		assertReplicaPlacement(replicaMap, token("7000000000000000000"), "127.0.0.106", "127.0.0.107", "127.0.0.101", "127.0.0.102", "127.0.0.108", "127.0.0.103");
		assertReplicaPlacement(replicaMap, token("8000000000000000000"), "127.0.0.107", "127.0.0.108", "127.0.0.101", "127.0.0.102", "127.0.0.103", "127.0.0.104");
		assertReplicaPlacement(replicaMap, token("9000000000000000000"), "127.0.0.108", "127.0.0.101", "127.0.0.102", "127.0.0.104", "127.0.0.105", "127.0.0.103");

	}
	
	@Test(groups = "unit")
	public void networkTopologyWithMultirackHostSkippingTest2() {
		List<Token> ring = ImmutableList.<Token>builder()
			.add(token("-9000000000000000000"))
			.add(token("-8000000000000000000"))
			.add(token("-7000000000000000000"))
			.add(token("-6000000000000000000"))
			.add(token("-5000000000000000000"))
			.add(token("-4000000000000000000"))
			.add(token("-3000000000000000000"))
			.add(token("-2000000000000000000"))
			.add(token("2000000000000000000"))
			.add(token("3000000000000000000"))
			.add(token("4000000000000000000"))
			.add(token("5000000000000000000"))
			.add(token("6000000000000000000"))
			.add(token("7000000000000000000"))
			.add(token("8000000000000000000"))
			.add(token("9000000000000000000"))
			.build();
		
		Map<Token, Host> tokenToPrimary = ImmutableMap.<Token, Host>builder()
			.put(token("-9000000000000000000"), host("127.0.0.101", DC1, RACK11))
			.put(token("-8000000000000000000"), host("127.0.0.101", DC1, RACK11))
			.put(token("-7000000000000000000"), host("127.0.0.103", DC1, RACK11))
			.put(token("-6000000000000000000"), host("127.0.0.103", DC1, RACK11))
			.put(token("-5000000000000000000"), host("127.0.0.105", DC1, RACK12))
			.put(token("-4000000000000000000"), host("127.0.0.105", DC1, RACK12))
			.put(token("-3000000000000000000"), host("127.0.0.107", DC1, RACK12))
			.put(token("-2000000000000000000"), host("127.0.0.107", DC1, RACK12))
			.put(token("2000000000000000000"), host("127.0.0.102", DC2, RACK21))
			.put(token("3000000000000000000"), host("127.0.0.102", DC2, RACK21))
			.put(token("4000000000000000000"), host("127.0.0.104", DC2, RACK21))
			.put(token("5000000000000000000"), host("127.0.0.104", DC2, RACK21))
			.put(token("6000000000000000000"), host("127.0.0.106", DC2, RACK22))
			.put(token("7000000000000000000"), host("127.0.0.106", DC2, RACK22))
			.put(token("8000000000000000000"), host("127.0.0.108", DC2, RACK22))
			.put(token("9000000000000000000"), host("127.0.0.108", DC2, RACK22))
			.build();
		
		ReplicationStrategy strategy = networkTopologyStrategy(rf(DC1, 3), rf(DC2, 3));
		
		Map<Token, Set<Host>> replicaMap = strategy.computeTokenToReplicaMap(tokenToPrimary, ring);
		
		assertReplicaPlacement(replicaMap, token("-9000000000000000000"), "127.0.0.101", "127.0.0.105", "127.0.0.103", "127.0.0.102", "127.0.0.106", "127.0.0.104");
		assertReplicaPlacement(replicaMap, token("-8000000000000000000"), "127.0.0.101", "127.0.0.105", "127.0.0.103", "127.0.0.102", "127.0.0.106", "127.0.0.104");
		assertReplicaPlacement(replicaMap, token("-7000000000000000000"), "127.0.0.103", "127.0.0.105", "127.0.0.107", "127.0.0.102", "127.0.0.106", "127.0.0.104");
		assertReplicaPlacement(replicaMap, token("-6000000000000000000"), "127.0.0.103", "127.0.0.105", "127.0.0.107", "127.0.0.102", "127.0.0.106", "127.0.0.104");
		assertReplicaPlacement(replicaMap, token("-5000000000000000000"), "127.0.0.105", "127.0.0.102", "127.0.0.106", "127.0.0.104", "127.0.0.101", "127.0.0.107");
		assertReplicaPlacement(replicaMap, token("-4000000000000000000"), "127.0.0.105", "127.0.0.102", "127.0.0.106", "127.0.0.104", "127.0.0.101", "127.0.0.107");
		assertReplicaPlacement(replicaMap, token("-3000000000000000000"), "127.0.0.107", "127.0.0.102", "127.0.0.106", "127.0.0.104", "127.0.0.101", "127.0.0.103");
		assertReplicaPlacement(replicaMap, token("-2000000000000000000"), "127.0.0.107", "127.0.0.102", "127.0.0.106", "127.0.0.104", "127.0.0.101", "127.0.0.103");
		assertReplicaPlacement(replicaMap, token("2000000000000000000"), "127.0.0.102", "127.0.0.106", "127.0.0.104", "127.0.0.101", "127.0.0.105", "127.0.0.103");
		assertReplicaPlacement(replicaMap, token("3000000000000000000"), "127.0.0.102", "127.0.0.106", "127.0.0.104", "127.0.0.101", "127.0.0.105", "127.0.0.103");
		assertReplicaPlacement(replicaMap, token("4000000000000000000"), "127.0.0.104", "127.0.0.106", "127.0.0.108", "127.0.0.101", "127.0.0.105", "127.0.0.103");
		assertReplicaPlacement(replicaMap, token("5000000000000000000"), "127.0.0.104", "127.0.0.106", "127.0.0.108", "127.0.0.101", "127.0.0.105", "127.0.0.103");
		assertReplicaPlacement(replicaMap, token("6000000000000000000"), "127.0.0.106", "127.0.0.101", "127.0.0.105", "127.0.0.103", "127.0.0.102", "127.0.0.108");
		assertReplicaPlacement(replicaMap, token("7000000000000000000"), "127.0.0.106", "127.0.0.101", "127.0.0.105", "127.0.0.103", "127.0.0.102", "127.0.0.108");
		assertReplicaPlacement(replicaMap, token("8000000000000000000"), "127.0.0.108", "127.0.0.101", "127.0.0.105", "127.0.0.103", "127.0.0.102", "127.0.0.104");
		assertReplicaPlacement(replicaMap, token("9000000000000000000"), "127.0.0.108", "127.0.0.101", "127.0.0.105", "127.0.0.103", "127.0.0.102", "127.0.0.104");

	}
	
	@Test(groups = "unit")
	public void networkTopologyWithMultirackHostSkippingTest3() {
		//this is the same topology as in the previous test, but with different rfs
		List<Token> ring = ImmutableList.<Token>builder()
			.add(token("-9000000000000000000"))
			.add(token("-8000000000000000000"))
			.add(token("-7000000000000000000"))
			.add(token("-6000000000000000000"))
			.add(token("-5000000000000000000"))
			.add(token("-4000000000000000000"))
			.add(token("-3000000000000000000"))
			.add(token("-2000000000000000000"))
			.add(token("2000000000000000000"))
			.add(token("3000000000000000000"))
			.add(token("4000000000000000000"))
			.add(token("5000000000000000000"))
			.add(token("6000000000000000000"))
			.add(token("7000000000000000000"))
			.add(token("8000000000000000000"))
			.add(token("9000000000000000000"))
			.build();
		
		Map<Token, Host> tokenToPrimary = ImmutableMap.<Token, Host>builder()
			.put(token("-9000000000000000000"), host("127.0.0.101", DC1, RACK11))
			.put(token("-8000000000000000000"), host("127.0.0.101", DC1, RACK11))
			.put(token("-7000000000000000000"), host("127.0.0.103", DC1, RACK11))
			.put(token("-6000000000000000000"), host("127.0.0.103", DC1, RACK11))
			.put(token("-5000000000000000000"), host("127.0.0.105", DC1, RACK12))
			.put(token("-4000000000000000000"), host("127.0.0.105", DC1, RACK12))
			.put(token("-3000000000000000000"), host("127.0.0.107", DC1, RACK12))
			.put(token("-2000000000000000000"), host("127.0.0.107", DC1, RACK12))
			.put(token("2000000000000000000"), host("127.0.0.102", DC2, RACK21))
			.put(token("3000000000000000000"), host("127.0.0.102", DC2, RACK21))
			.put(token("4000000000000000000"), host("127.0.0.104", DC2, RACK21))
			.put(token("5000000000000000000"), host("127.0.0.104", DC2, RACK21))
			.put(token("6000000000000000000"), host("127.0.0.106", DC2, RACK22))
			.put(token("7000000000000000000"), host("127.0.0.106", DC2, RACK22))
			.put(token("8000000000000000000"), host("127.0.0.108", DC2, RACK22))
			.put(token("9000000000000000000"), host("127.0.0.108", DC2, RACK22))
			.build();
		
		//all nodes will contain all data, question is the replica order
		ReplicationStrategy strategy = networkTopologyStrategy(rf(DC1, 4), rf(DC2, 4));
		
		Map<Token, Set<Host>> replicaMap = strategy.computeTokenToReplicaMap(tokenToPrimary, ring);
		
		assertReplicaPlacement(replicaMap, token("-9000000000000000000"), "127.0.0.101", "127.0.0.105", "127.0.0.103", "127.0.0.107","127.0.0.102", "127.0.0.106", "127.0.0.104", "127.0.0.108");
		assertReplicaPlacement(replicaMap, token("-8000000000000000000"), "127.0.0.101", "127.0.0.105", "127.0.0.103", "127.0.0.107", "127.0.0.102", "127.0.0.106", "127.0.0.104", "127.0.0.108");
		assertReplicaPlacement(replicaMap, token("-7000000000000000000"), "127.0.0.103", "127.0.0.105", "127.0.0.107", "127.0.0.102", "127.0.0.106", "127.0.0.104", "127.0.0.108", "127.0.0.101");
		assertReplicaPlacement(replicaMap, token("-6000000000000000000"), "127.0.0.103", "127.0.0.105", "127.0.0.107", "127.0.0.102", "127.0.0.106", "127.0.0.104", "127.0.0.108", "127.0.0.101");
		assertReplicaPlacement(replicaMap, token("-5000000000000000000"), "127.0.0.105", "127.0.0.102", "127.0.0.106", "127.0.0.104", "127.0.0.108", "127.0.0.101", "127.0.0.107", "127.0.0.103");
		assertReplicaPlacement(replicaMap, token("-4000000000000000000"), "127.0.0.105", "127.0.0.102", "127.0.0.106", "127.0.0.104", "127.0.0.108", "127.0.0.101", "127.0.0.107", "127.0.0.103");
		assertReplicaPlacement(replicaMap, token("-3000000000000000000"), "127.0.0.107", "127.0.0.102", "127.0.0.106", "127.0.0.104", "127.0.0.108", "127.0.0.101", "127.0.0.103", "127.0.0.105");
		assertReplicaPlacement(replicaMap, token("-2000000000000000000"), "127.0.0.107", "127.0.0.102", "127.0.0.106", "127.0.0.104", "127.0.0.108", "127.0.0.101", "127.0.0.103", "127.0.0.105");
		assertReplicaPlacement(replicaMap, token("2000000000000000000"), "127.0.0.102", "127.0.0.106", "127.0.0.104", "127.0.0.108", "127.0.0.101", "127.0.0.105", "127.0.0.103", "127.0.0.107");
		assertReplicaPlacement(replicaMap, token("3000000000000000000"), "127.0.0.102", "127.0.0.106", "127.0.0.104", "127.0.0.108", "127.0.0.101", "127.0.0.105", "127.0.0.103", "127.0.0.107");
		assertReplicaPlacement(replicaMap, token("4000000000000000000"), "127.0.0.104", "127.0.0.106", "127.0.0.108", "127.0.0.101", "127.0.0.105", "127.0.0.103", "127.0.0.107", "127.0.0.102");
		assertReplicaPlacement(replicaMap, token("5000000000000000000"), "127.0.0.104", "127.0.0.106", "127.0.0.108", "127.0.0.101", "127.0.0.105", "127.0.0.103", "127.0.0.107", "127.0.0.102");
		assertReplicaPlacement(replicaMap, token("6000000000000000000"), "127.0.0.106", "127.0.0.101", "127.0.0.105", "127.0.0.103", "127.0.0.107", "127.0.0.102", "127.0.0.108", "127.0.0.104");
		assertReplicaPlacement(replicaMap, token("7000000000000000000"), "127.0.0.106", "127.0.0.101", "127.0.0.105", "127.0.0.103", "127.0.0.107", "127.0.0.102", "127.0.0.108", "127.0.0.104");
		assertReplicaPlacement(replicaMap, token("8000000000000000000"), "127.0.0.108", "127.0.0.101", "127.0.0.105", "127.0.0.103", "127.0.0.107", "127.0.0.102", "127.0.0.104", "127.0.0.106");
		assertReplicaPlacement(replicaMap, token("9000000000000000000"), "127.0.0.108", "127.0.0.101", "127.0.0.105", "127.0.0.103", "127.0.0.107", "127.0.0.102", "127.0.0.104", "127.0.0.106");

	}
	
	@Test(groups = "unit")
	public void networkTopologyStrategyExampleTopologyTest() {
		Map<Token, Set<Host>> replicaMap = exampleStrategy.computeTokenToReplicaMap(exampleTokenToPrimary, exampleRing);
		
		//105 and 106 will appear as replica for all as they're in separate racks
		assertReplicaPlacement(replicaMap, token("-9000000000000000000"), "127.0.0.101", "127.0.0.105", "127.0.0.102", "127.0.0.106");
		assertReplicaPlacement(replicaMap, token("-8000000000000000000"), "127.0.0.101", "127.0.0.105", "127.0.0.102", "127.0.0.106");
		assertReplicaPlacement(replicaMap, token("-7000000000000000000"), "127.0.0.105", "127.0.0.103", "127.0.0.102", "127.0.0.106");
		assertReplicaPlacement(replicaMap, token("-6000000000000000000"), "127.0.0.103", "127.0.0.105", "127.0.0.102", "127.0.0.106");
		assertReplicaPlacement(replicaMap, token("-5000000000000000000"), "127.0.0.101", "127.0.0.105", "127.0.0.102", "127.0.0.106");
		assertReplicaPlacement(replicaMap, token("-4000000000000000000"), "127.0.0.105", "127.0.0.102", "127.0.0.106", "127.0.0.103");
		assertReplicaPlacement(replicaMap, token("-3000000000000000000"), "127.0.0.102", "127.0.0.106", "127.0.0.103", "127.0.0.105");
		assertReplicaPlacement(replicaMap, token("-2000000000000000000"), "127.0.0.106", "127.0.0.103", "127.0.0.104", "127.0.0.105");
		assertReplicaPlacement(replicaMap, token("-1000000000000000000"), "127.0.0.103", "127.0.0.104", "127.0.0.105", "127.0.0.106");
		assertReplicaPlacement(replicaMap, token("0"),                    "127.0.0.104", "127.0.0.105", "127.0.0.106", "127.0.0.103");
		assertReplicaPlacement(replicaMap, token("1000000000000000000"), "127.0.0.105", "127.0.0.104", "127.0.0.106", "127.0.0.103");
		assertReplicaPlacement(replicaMap, token("2000000000000000000"), "127.0.0.104", "127.0.0.106", "127.0.0.103", "127.0.0.105");
		assertReplicaPlacement(replicaMap, token("3000000000000000000"), "127.0.0.104", "127.0.0.106", "127.0.0.103", "127.0.0.105");
		assertReplicaPlacement(replicaMap, token("4000000000000000000"), "127.0.0.102", "127.0.0.106", "127.0.0.103", "127.0.0.105");
		assertReplicaPlacement(replicaMap, token("5000000000000000000"), "127.0.0.106", "127.0.0.103", "127.0.0.102", "127.0.0.105");
		assertReplicaPlacement(replicaMap, token("6000000000000000000"), "127.0.0.103", "127.0.0.102", "127.0.0.106", "127.0.0.105");
		assertReplicaPlacement(replicaMap, token("7000000000000000000"), "127.0.0.102", "127.0.0.106", "127.0.0.101", "127.0.0.105");
		assertReplicaPlacement(replicaMap, token("8000000000000000000"), "127.0.0.106", "127.0.0.101", "127.0.0.105", "127.0.0.102");
	}
	
	@Test(groups = "unit")
	public void networkTopologyStrategyExampleTopologyTooManyReplicasTest() {
		Map<Token, Set<Host>> replicaMap = exampleStrategyTooManyReplicas.computeTokenToReplicaMap(exampleTokenToPrimary, exampleRing);
		
		assertReplicaPlacement(replicaMap, token("-9000000000000000000"), "127.0.0.101", "127.0.0.105", "127.0.0.103", "127.0.0.102", "127.0.0.106", "127.0.0.104");
		assertReplicaPlacement(replicaMap, token("-8000000000000000000"), "127.0.0.101", "127.0.0.105", "127.0.0.103", "127.0.0.102", "127.0.0.106", "127.0.0.104");
		assertReplicaPlacement(replicaMap, token("-7000000000000000000"), "127.0.0.105", "127.0.0.103", "127.0.0.101", "127.0.0.102", "127.0.0.106", "127.0.0.104");
		assertReplicaPlacement(replicaMap, token("-6000000000000000000"), "127.0.0.103", "127.0.0.105", "127.0.0.101", "127.0.0.102", "127.0.0.106", "127.0.0.104");
		assertReplicaPlacement(replicaMap, token("-5000000000000000000"), "127.0.0.101", "127.0.0.105", "127.0.0.102", "127.0.0.106", "127.0.0.103", "127.0.0.104");
		assertReplicaPlacement(replicaMap, token("-4000000000000000000"), "127.0.0.105", "127.0.0.102", "127.0.0.106", "127.0.0.103", "127.0.0.104", "127.0.0.101");
		assertReplicaPlacement(replicaMap, token("-3000000000000000000"), "127.0.0.102", "127.0.0.106", "127.0.0.103", "127.0.0.104", "127.0.0.105", "127.0.0.101");
		assertReplicaPlacement(replicaMap, token("-2000000000000000000"), "127.0.0.106", "127.0.0.103", "127.0.0.104", "127.0.0.105", "127.0.0.102", "127.0.0.101");
		assertReplicaPlacement(replicaMap, token("-1000000000000000000"), "127.0.0.103", "127.0.0.104", "127.0.0.105", "127.0.0.106", "127.0.0.102", "127.0.0.101");
		assertReplicaPlacement(replicaMap, token("0"),                    "127.0.0.104", "127.0.0.105", "127.0.0.106", "127.0.0.102", "127.0.0.103", "127.0.0.101");
		assertReplicaPlacement(replicaMap, token("1000000000000000000"), "127.0.0.105", "127.0.0.104", "127.0.0.106", "127.0.0.102", "127.0.0.103", "127.0.0.101");
		assertReplicaPlacement(replicaMap, token("2000000000000000000"), "127.0.0.104", "127.0.0.106", "127.0.0.102", "127.0.0.103", "127.0.0.105", "127.0.0.101");
		assertReplicaPlacement(replicaMap, token("3000000000000000000"), "127.0.0.104", "127.0.0.106", "127.0.0.102", "127.0.0.103", "127.0.0.105", "127.0.0.101");
		assertReplicaPlacement(replicaMap, token("4000000000000000000"), "127.0.0.102", "127.0.0.106", "127.0.0.103", "127.0.0.105", "127.0.0.101", "127.0.0.104");
		assertReplicaPlacement(replicaMap, token("5000000000000000000"), "127.0.0.106", "127.0.0.103", "127.0.0.102", "127.0.0.105", "127.0.0.101", "127.0.0.104");
		assertReplicaPlacement(replicaMap, token("6000000000000000000"), "127.0.0.103", "127.0.0.102", "127.0.0.106", "127.0.0.105", "127.0.0.101", "127.0.0.104");
		assertReplicaPlacement(replicaMap, token("7000000000000000000"), "127.0.0.102", "127.0.0.106", "127.0.0.101", "127.0.0.105", "127.0.0.103", "127.0.0.104");
		assertReplicaPlacement(replicaMap, token("8000000000000000000"), "127.0.0.106", "127.0.0.101", "127.0.0.105", "127.0.0.103", "127.0.0.102", "127.0.0.104");
	}
}
