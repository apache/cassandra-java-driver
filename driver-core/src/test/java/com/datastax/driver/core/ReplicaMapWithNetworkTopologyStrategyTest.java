package com.datastax.driver.core;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class ReplicaMapWithNetworkTopologyStrategyTest extends AbstractReplicationStrategyTest {
	/*
	 * ---------------------------------------------------------------------------
	 * Ring, replication, etc... setup. These are reusable for the tests
	 * ---------------------------------------------------------------------------
	 */
	private static final String DC1 = "DC1";
	private static final String DC2 = "DC2";
	private static final String RACK11 = "RACK11";
	private static final String RACK12 = "RACK12";
	private static final String RACK21 = "RACK21";
	private static final String RACK22 = "RACK22";
	
	private static final Map<String, String> replicationOptions = ImmutableMap.<String, String>builder()//
			.put("class", "NetworkTopologyStrategy")//
			.put(DC1, "2")//
			.put(DC2, "2")//
			.build();
	
	//this will be used in scenarios where there are more replicas specified in strategy than available nodes
	private static final Map<String, String> replicationOptionsTooManyReplicas = ImmutableMap.<String, String>builder()//
			.put("class", "NetworkTopologyStrategy")//
			.put(DC1, "4")//
			.put(DC2, "4")//
			.build();
	
	private static final ReplicationStrategy strategy = ReplicationStrategy.create(replicationOptions);
	
	private static final ReplicationStrategy strategyTooManyReplicas = ReplicationStrategy.create(replicationOptionsTooManyReplicas);

	private static final List<Token> ring = ImmutableList.<Token>builder()
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
	
	private static final Map<Token, Host> tokenToPrimary = ImmutableMap.<Token, Host>builder()
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
	public void networkTopologyStrategyMapTest() {
		Map<Token, Set<Host>> replicaMap = strategy.computeTokenToReplicaMap(tokenToPrimary, ring);
		
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
	public void networkTopologyStrategyMapTooManyReplicasTest() {
		Map<Token, Set<Host>> replicaMap = strategyTooManyReplicas.computeTokenToReplicaMap(tokenToPrimary, ring);
		
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
