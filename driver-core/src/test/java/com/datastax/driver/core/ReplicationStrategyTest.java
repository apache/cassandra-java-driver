package com.datastax.driver.core;

import static org.testng.Assert.*;

import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

public class ReplicationStrategyTest {

	/*
	 * these should result in a correct strategy being created
	 */
	
	@Test(groups = "unit")
	public void createSimpleReplicationStrategyTest() throws Exception {
		ReplicationStrategy strategy = ReplicationStrategy.create(
			ImmutableMap.<String, String>builder()
				.put("class", "SimpleStrategy")
				.put("replication_factor", "3")
				.build());
		
		assertNotNull(strategy);
		assertTrue(strategy instanceof ReplicationStrategy.SimpleStrategy);
	}
	
	@Test(groups = "unit")
	public void createNetworkTopologyStrategyTest() throws Exception {
		ReplicationStrategy strategy = ReplicationStrategy.create(
			ImmutableMap.<String, String>builder()
				.put("class", "NetworkTopologyStrategy")
				.put("dc1", "2")
				.put("dc2", "2")
				.build());
		
		assertNotNull(strategy);
		assertTrue(strategy instanceof ReplicationStrategy.NetworkTopologyStrategy);
	}
	
	/*
	 * if the parameters are incorrect/missing, null is expected 
	 */
	
	@Test(groups = "unit")
	public void createSimpleReplicationStrategyWithoutFactorTest() throws Exception {
		ReplicationStrategy strategy = ReplicationStrategy.create(
			ImmutableMap.<String, String>builder()
				.put("class", "SimpleStrategy")
				//no replication_factor
				.build());
		
		assertNull(strategy);
	}
	
	@Test(groups = "unit")
	public void createUnknownStrategyTest() throws Exception {
		ReplicationStrategy strategy = ReplicationStrategy.create(
			ImmutableMap.<String, String>builder()
				//no such strategy
				.put("class", "FooStrategy") 
				.put("foo_factor", "3")
				.build());
		
		assertNull(strategy);
	}
	
	@Test(groups = "unit")
	public void createUnspecifiedStrategyTest() throws Exception {
		ReplicationStrategy strategy = ReplicationStrategy.create(
			ImmutableMap.<String, String>builder()
				//nothing useful is set
				.put("foo", "bar")
				.build());
		
		assertNull(strategy);
	}
}
