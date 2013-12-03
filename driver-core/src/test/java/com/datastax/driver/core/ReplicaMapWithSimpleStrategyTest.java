package com.datastax.driver.core;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class ReplicaMapWithSimpleStrategyTest extends AbstractReplicationStrategyTest {

	/*
	 * ---------------------------------------------------------------------------
	 * Ring, replication, etc... setup. These are reusable for the tests
	 * ---------------------------------------------------------------------------
	 */
	
	private static final Map<String, String> replicationOptions = ImmutableMap.<String, String>builder()//
			.put("class", "SimpleStrategy")//
			.put("replication_factor", "3")//
			.build();
	
	private static final Map<String, String> replicationOptionsTooManyReplicas = ImmutableMap.<String, String>builder()//
			.put("class", "SimpleStrategy")//
			.put("replication_factor", "8")//
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
			.put(token("-9000000000000000000"), host("127.0.0.101"))//
			.put(token("-8000000000000000000"), host("127.0.0.101"))//
			.put(token("-7000000000000000000"), host("127.0.0.105"))//
			.put(token("-6000000000000000000"), host("127.0.0.103"))//
			.put(token("-5000000000000000000"), host("127.0.0.101"))//
			.put(token("-4000000000000000000"), host("127.0.0.105"))//
			.put(token("-3000000000000000000"), host("127.0.0.102"))//
			.put(token("-2000000000000000000"), host("127.0.0.106"))//
			.put(token("-1000000000000000000"), host("127.0.0.103"))//
			.put(token("0"),                    host("127.0.0.104"))//
			.put(token("1000000000000000000"), host("127.0.0.105"))//
			.put(token("2000000000000000000"), host("127.0.0.104"))//
			.put(token("3000000000000000000"), host("127.0.0.104"))//
			.put(token("4000000000000000000"), host("127.0.0.102"))//
			.put(token("5000000000000000000"), host("127.0.0.106"))//
			.put(token("6000000000000000000"), host("127.0.0.103"))//
			.put(token("7000000000000000000"), host("127.0.0.102"))//
			.put(token("8000000000000000000"), host("127.0.0.106"))//
			.build();
	
	/*
	 * --------------
	 *     Tests
	 * --------------
	 */
	
	@Test(groups = "unit")
	public void simpleStrategyReplicaMapTest() {
		Map<Token, Set<Host>> replicaMap = strategy.computeTokenToReplicaMap(tokenToPrimary, ring);
		
		assertReplicaPlacement(replicaMap, token("-9000000000000000000"), "127.0.0.101", "127.0.0.105", "127.0.0.103");
		assertReplicaPlacement(replicaMap, token("-8000000000000000000"), "127.0.0.101", "127.0.0.105", "127.0.0.103");
		assertReplicaPlacement(replicaMap, token("-7000000000000000000"), "127.0.0.105", "127.0.0.103", "127.0.0.101");
		assertReplicaPlacement(replicaMap, token("-6000000000000000000"), "127.0.0.103", "127.0.0.101", "127.0.0.105");
		assertReplicaPlacement(replicaMap, token("-5000000000000000000"), "127.0.0.101", "127.0.0.105", "127.0.0.102");
		assertReplicaPlacement(replicaMap, token("-4000000000000000000"), "127.0.0.105", "127.0.0.102", "127.0.0.106");
		assertReplicaPlacement(replicaMap, token("-3000000000000000000"), "127.0.0.102", "127.0.0.106", "127.0.0.103");
		assertReplicaPlacement(replicaMap, token("-2000000000000000000"), "127.0.0.106", "127.0.0.103", "127.0.0.104");
		assertReplicaPlacement(replicaMap, token("-1000000000000000000"), "127.0.0.103", "127.0.0.104", "127.0.0.105");
		assertReplicaPlacement(replicaMap, token("0"),                    "127.0.0.104", "127.0.0.105", "127.0.0.102");
		assertReplicaPlacement(replicaMap, token("1000000000000000000"), "127.0.0.105", "127.0.0.104", "127.0.0.102");
		assertReplicaPlacement(replicaMap, token("2000000000000000000"), "127.0.0.104", "127.0.0.102", "127.0.0.106");
		assertReplicaPlacement(replicaMap, token("3000000000000000000"), "127.0.0.104", "127.0.0.102", "127.0.0.106");
		assertReplicaPlacement(replicaMap, token("4000000000000000000"), "127.0.0.102", "127.0.0.106", "127.0.0.103");
		assertReplicaPlacement(replicaMap, token("5000000000000000000"), "127.0.0.106", "127.0.0.103", "127.0.0.102");
		assertReplicaPlacement(replicaMap, token("6000000000000000000"), "127.0.0.103", "127.0.0.102", "127.0.0.106");
		assertReplicaPlacement(replicaMap, token("7000000000000000000"), "127.0.0.102", "127.0.0.106", "127.0.0.101");
		assertReplicaPlacement(replicaMap, token("8000000000000000000"), "127.0.0.106", "127.0.0.101", "127.0.0.105");
	}
	
	@Test(groups = "unit")
	public void simpleStrategyMapTooManyReplicasTest() {
		Map<Token, Set<Host>> replicaMap = strategyTooManyReplicas.computeTokenToReplicaMap(tokenToPrimary, ring);
		
		assertReplicaPlacement(replicaMap, token("-9000000000000000000"), "127.0.0.101", "127.0.0.105", "127.0.0.103", "127.0.0.102", "127.0.0.106", "127.0.0.104");
		assertReplicaPlacement(replicaMap, token("-8000000000000000000"), "127.0.0.101", "127.0.0.105", "127.0.0.103", "127.0.0.102", "127.0.0.106", "127.0.0.104");
		assertReplicaPlacement(replicaMap, token("-7000000000000000000"), "127.0.0.105", "127.0.0.103", "127.0.0.101", "127.0.0.102", "127.0.0.106", "127.0.0.104");
		assertReplicaPlacement(replicaMap, token("-6000000000000000000"), "127.0.0.103", "127.0.0.101", "127.0.0.105", "127.0.0.102", "127.0.0.106", "127.0.0.104");
		assertReplicaPlacement(replicaMap, token("-5000000000000000000"), "127.0.0.101", "127.0.0.105", "127.0.0.102", "127.0.0.106", "127.0.0.103", "127.0.0.104");
		assertReplicaPlacement(replicaMap, token("-4000000000000000000"), "127.0.0.105", "127.0.0.102", "127.0.0.106", "127.0.0.103", "127.0.0.104", "127.0.0.101");
		assertReplicaPlacement(replicaMap, token("-3000000000000000000"), "127.0.0.102", "127.0.0.106", "127.0.0.103", "127.0.0.104", "127.0.0.105", "127.0.0.101");
		assertReplicaPlacement(replicaMap, token("-2000000000000000000"), "127.0.0.106", "127.0.0.103", "127.0.0.104", "127.0.0.105", "127.0.0.102", "127.0.0.101");
		assertReplicaPlacement(replicaMap, token("-1000000000000000000"), "127.0.0.103", "127.0.0.104", "127.0.0.105", "127.0.0.102", "127.0.0.106", "127.0.0.101");
		assertReplicaPlacement(replicaMap, token("0"),                    "127.0.0.104", "127.0.0.105", "127.0.0.102", "127.0.0.106", "127.0.0.103", "127.0.0.101");
		assertReplicaPlacement(replicaMap, token("1000000000000000000"), "127.0.0.105", "127.0.0.104", "127.0.0.102", "127.0.0.106", "127.0.0.103", "127.0.0.101");
		assertReplicaPlacement(replicaMap, token("2000000000000000000"), "127.0.0.104", "127.0.0.102", "127.0.0.106", "127.0.0.103", "127.0.0.101", "127.0.0.105");
		assertReplicaPlacement(replicaMap, token("3000000000000000000"), "127.0.0.104", "127.0.0.102", "127.0.0.106", "127.0.0.103", "127.0.0.101", "127.0.0.105");
		assertReplicaPlacement(replicaMap, token("4000000000000000000"), "127.0.0.102", "127.0.0.106", "127.0.0.103", "127.0.0.101", "127.0.0.105", "127.0.0.104");
		assertReplicaPlacement(replicaMap, token("5000000000000000000"), "127.0.0.106", "127.0.0.103", "127.0.0.102", "127.0.0.101", "127.0.0.105", "127.0.0.104");
		assertReplicaPlacement(replicaMap, token("6000000000000000000"), "127.0.0.103", "127.0.0.102", "127.0.0.106", "127.0.0.101", "127.0.0.105", "127.0.0.104");
		assertReplicaPlacement(replicaMap, token("7000000000000000000"), "127.0.0.102", "127.0.0.106", "127.0.0.101", "127.0.0.105", "127.0.0.103", "127.0.0.104");
		assertReplicaPlacement(replicaMap, token("8000000000000000000"), "127.0.0.106", "127.0.0.101", "127.0.0.105", "127.0.0.103", "127.0.0.102", "127.0.0.104");
	}
}
