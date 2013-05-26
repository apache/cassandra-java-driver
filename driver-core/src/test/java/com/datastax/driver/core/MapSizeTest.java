package com.datastax.driver.core;

import java.util.Map;

import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class MapSizeTest {
	private static final String CREATE_KEYSPACE = "CREATE KEYSPACE test;";
	private static final String CREATE_TABLE = "CREATE TABLE test1 (id int, test map<text, int>, PRIMARY KEY(id));";
	private static final String INSERT = "UPDATE test1 SET test[?] = ? WHERE id = ?;";
	private static final String SELECT = "SELECT test FROM test1 WHERE id = 1;";
	private static final int MapSize = 100000;
	
	@Test
	public void CheckMapSize() throws Exception {
		Cluster cluster = Cluster.builder().addContactPoint("localhost").build();
		Session session = cluster.connect("test");
		//session.execute(CREATE_TABLE);
		PreparedStatement insertStatment = session.prepare(INSERT);
		BoundStatement bs = new BoundStatement(insertStatment);
		for(int i = 0; i < MapSize; i++) {
			bs.setString(0, new Integer(i).toString());
			bs.setInt(1, i);
			bs.setInt(2, 1);
			session.execute(bs);
		}
		ResultSet rs = session.execute(SELECT);
		Row row = rs.one();
		Map<String, Integer> map = row.getMap(0, String.class, Integer.class);
		assertEquals(map.size(), MapSize);
	}

}