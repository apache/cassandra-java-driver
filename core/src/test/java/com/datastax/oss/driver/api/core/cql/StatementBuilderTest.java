/*
 * Copyright DataStax, Inc.
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
package com.datastax.oss.driver.api.core.cql;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.net.InetSocketAddress;

import org.junit.Test;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatementBuilder;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.cql.StatementBuilder;

public class StatementBuilderTest {

	private CqlSession session() {
		
		return CqlSession.builder()
				.addContactPoint(new InetSocketAddress("127.0.0.1",9042))
				.withLocalDatacenter("Cassandra")
				.build();
	}
	
	private StatementBuilder<SimpleStatementBuilder, SimpleStatement> builder() {
		
		return SimpleStatement.builder("select * from system.compaction_history");
	}
	
	@Test
	public void testExisingTracingUsingBuilder() {
		
		CqlSession s = session();
		Statement<SimpleStatement> stmt = builder()
				.setTracing()
				.build();
		ResultSet rs = s.execute(stmt);
		assertNotNull(rs.getExecutionInfo().getTracingId());
		
		stmt = builder()
				.build();
		rs = s.execute(stmt);
		assertNull(rs.getExecutionInfo().getTracingId());
	}

	@Test
	public void testNewTracingUsingBuilder() {
		
		CqlSession s = session();
		Statement<SimpleStatement> stmt = builder()
				.setTracing(true)
				.build();
		ResultSet rs = s.execute(stmt);
		assertNotNull(rs.getExecutionInfo().getTracingId());
		
		stmt = builder()
				.setTracing(false)
				.build();
		rs = s.execute(stmt);
		assertNull(rs.getExecutionInfo().getTracingId());
	}
	
	@Test
	public void testNewTracingOverrideTemplate() {
		
		CqlSession s = session();
		SimpleStatement stmt1 = builder()
				.setTracing()
				.build();
		SimpleStatement stmt2 = SimpleStatement.builder(stmt1).setTracing(false).build();
		ResultSet rs = s.execute(stmt2);
		assertNull(rs.getExecutionInfo().getTracingId());
	}
}
