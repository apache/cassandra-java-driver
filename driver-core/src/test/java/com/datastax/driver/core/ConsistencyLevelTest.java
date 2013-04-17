/*
 *      Copyright (C) 2012 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import java.util.*;

import org.testng.annotations.Test;

import static org.testng.Assert.*;

/**
 * Simple test of the Sessions methods against a one node cluster.
 */
public class ConsistencyLevelTest extends CCMBridge.PerClassSingleNodeCluster {

    private static final String TABLE1 = "test1";

    protected Collection<String> getTableDefinitions() {
        return Arrays.asList(String.format(TestUtils.CREATE_TABLE_SIMPLE_FORMAT, TABLE1));
    }
    
    @Test(groups = "integration")
    public void defaultConsistencyLevelONE() {
    	
    	String cqlQuery = String.format(TestUtils.SELECT_ALL_FORMAT + " WHERE k = ?", TABLE1);

        assertEquals(ConsistencyLevel.ONE, cluster.getDefaultConsistencyLevel());
        Session firstSession = cluster.connect(TestUtils.SIMPLE_KEYSPACE);
        assertEquals(ConsistencyLevel.ONE, firstSession.getDefaultConsistencyLevel());
        PreparedStatement firstPreparedStatement = session.prepare(cqlQuery);
        assertEquals(ConsistencyLevel.ONE, firstPreparedStatement.getConsistencyLevel());
    }
    
    @Test(groups = "integration")
    public void defaultConsistencyLevelClusterTest() {
    	
    	String cqlQuery = String.format(TestUtils.SELECT_ALL_FORMAT + " WHERE k = ?", TABLE1);

        assertEquals(ConsistencyLevel.ONE, cluster.getDefaultConsistencyLevel());
        Session firstSession = cluster.connect(TestUtils.SIMPLE_KEYSPACE);
        assertEquals(ConsistencyLevel.ONE, firstSession.getDefaultConsistencyLevel());
        PreparedStatement firstPreparedStatement = session.prepare(cqlQuery);
        assertEquals(ConsistencyLevel.ONE, firstPreparedStatement.getConsistencyLevel());

        // switch default to QUORUM
        cluster.setDefaultConsistencyLevel(ConsistencyLevel.QUORUM);
        
        // check for new consistency
        assertEquals(ConsistencyLevel.QUORUM, cluster.getDefaultConsistencyLevel());
        Session secondSession = cluster.connect(TestUtils.SIMPLE_KEYSPACE);
        assertEquals(ConsistencyLevel.QUORUM, secondSession.getDefaultConsistencyLevel());
        PreparedStatement secondPreparedStatement = secondSession.prepare(cqlQuery);
        assertEquals(ConsistencyLevel.QUORUM, secondPreparedStatement.getConsistencyLevel());
        
        // keep consistency for existing objects
        assertEquals(ConsistencyLevel.ONE, firstSession.getDefaultConsistencyLevel());
        assertEquals(ConsistencyLevel.ONE, firstPreparedStatement.getConsistencyLevel());
    }
    
    @Test(groups = "integration")
    public void defaultConsistencyLevelSessionTest() {
    	
    	String cqlQuery = String.format(TestUtils.SELECT_ALL_FORMAT + " WHERE k = ?", TABLE1);

        assertEquals(ConsistencyLevel.ONE, cluster.getDefaultConsistencyLevel());
        Session firstSession = cluster.connect(TestUtils.SIMPLE_KEYSPACE);
        assertEquals(ConsistencyLevel.ONE, firstSession.getDefaultConsistencyLevel());
        PreparedStatement firstPreparedStatement = session.prepare(cqlQuery);
        assertEquals(ConsistencyLevel.ONE, firstPreparedStatement.getConsistencyLevel());

        // switch default to QUORUM
        Session secondSession = cluster.connect(TestUtils.SIMPLE_KEYSPACE);
        assertEquals(ConsistencyLevel.ONE, secondSession.getDefaultConsistencyLevel());
        secondSession.setDefaultConsistencyLevel(ConsistencyLevel.QUORUM);
        
        // check for new consistency
        assertEquals(ConsistencyLevel.QUORUM, secondSession.getDefaultConsistencyLevel());
        PreparedStatement secondPreparedStatement = secondSession.prepare(cqlQuery);
        assertEquals(ConsistencyLevel.QUORUM, secondPreparedStatement.getConsistencyLevel());
        
        // keep consistency for existing objects
        assertEquals(ConsistencyLevel.ONE, cluster.getDefaultConsistencyLevel());
        assertEquals(ConsistencyLevel.ONE, firstSession.getDefaultConsistencyLevel());
        assertEquals(ConsistencyLevel.ONE, firstPreparedStatement.getConsistencyLevel());
    }
    
    @Test(groups = "integration")
    public void defaultConsistencyLevelStatementTest() {
    	
    	String cqlQuery = String.format(TestUtils.SELECT_ALL_FORMAT + " WHERE k = ?", TABLE1);

        assertEquals(ConsistencyLevel.ONE, cluster.getDefaultConsistencyLevel());
        Session firstSession = cluster.connect(TestUtils.SIMPLE_KEYSPACE);
        assertEquals(ConsistencyLevel.ONE, firstSession.getDefaultConsistencyLevel());
        PreparedStatement firstPreparedStatement = session.prepare(cqlQuery);
        assertEquals(ConsistencyLevel.ONE, firstPreparedStatement.getConsistencyLevel());

        // switch default to QUORUM
        Session secondSession = cluster.connect(TestUtils.SIMPLE_KEYSPACE);
        assertEquals(ConsistencyLevel.ONE, secondSession.getDefaultConsistencyLevel());
        PreparedStatement secondPreparedStatement = secondSession.prepare(cqlQuery);
        assertEquals(ConsistencyLevel.ONE, secondPreparedStatement.getConsistencyLevel());
        secondPreparedStatement.setConsistencyLevel(ConsistencyLevel.QUORUM);
        
        // check for new consistency
        assertEquals(ConsistencyLevel.QUORUM, secondPreparedStatement.getConsistencyLevel());
        
        // keep consistency for existing objects
        assertEquals(ConsistencyLevel.ONE, cluster.getDefaultConsistencyLevel());
        assertEquals(ConsistencyLevel.ONE, firstSession.getDefaultConsistencyLevel());
        assertEquals(ConsistencyLevel.ONE, firstPreparedStatement.getConsistencyLevel());
        assertEquals(ConsistencyLevel.ONE, secondSession.getDefaultConsistencyLevel());
    }
    

}
