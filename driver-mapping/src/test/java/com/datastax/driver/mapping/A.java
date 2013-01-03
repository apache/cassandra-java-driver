package com.datastax.driver.mapping;

import java.util.UUID;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;

@Table(
		keyspace = "test",
		name = "a",
		defaultReadConsistencyLevel = ConsistencyLevel.QUORUM,
		defaultWriteConsistencyLevel = ConsistencyLevel.QUORUM
		)
class A {
	@Column(name = "c1")
	private String p1;
	
	public String getP1() {	return p1; }
	public void setP1(String p1) { this.p1 = p1; }
	
}