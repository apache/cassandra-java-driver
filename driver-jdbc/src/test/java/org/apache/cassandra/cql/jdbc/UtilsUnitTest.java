/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */

package org.apache.cassandra.cql.jdbc;

import static org.junit.Assert.*;

import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UtilsUnitTest
{
    private static final Logger LOG = LoggerFactory.getLogger(CollectionsTest.class);

    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {}

    @Test
    public void testParseURL() throws Exception
    {
        String happypath = "jdbc:cassandra://localhost:9170/Keyspace1?version=3.0.0&consistency=QUORUM";
        Properties props = Utils.parseURL(happypath);
        assertEquals("localhost", props.getProperty(Utils.TAG_SERVER_NAME));
        assertEquals("9170", props.getProperty(Utils.TAG_PORT_NUMBER));
        assertEquals("Keyspace1", props.getProperty(Utils.TAG_DATABASE_NAME));
        assertEquals("3.0.0", props.getProperty(Utils.TAG_CQL_VERSION));
        assertEquals("QUORUM", props.getProperty(Utils.TAG_CONSISTENCY_LEVEL));
                       
        String consistencyonly = "jdbc:cassandra://localhost/Keyspace1?consistency=QUORUM";
        props = Utils.parseURL(consistencyonly);
        assertEquals("localhost", props.getProperty(Utils.TAG_SERVER_NAME));
        assertEquals("9042", props.getProperty(Utils.TAG_PORT_NUMBER));
        assertEquals("Keyspace1", props.getProperty(Utils.TAG_DATABASE_NAME));
        assertEquals("QUORUM", props.getProperty(Utils.TAG_CONSISTENCY_LEVEL));
        assertNull(props.getProperty(Utils.TAG_CQL_VERSION));
       
        String noport = "jdbc:cassandra://localhost/Keyspace1?version=2.0.0";
        props = Utils.parseURL(noport);
        assertEquals("localhost", props.getProperty(Utils.TAG_SERVER_NAME));
        assertEquals("9042", props.getProperty(Utils.TAG_PORT_NUMBER));
        assertEquals("Keyspace1", props.getProperty(Utils.TAG_DATABASE_NAME));
        assertEquals("2.0.0", props.getProperty(Utils.TAG_CQL_VERSION));
        
        String noversion = "jdbc:cassandra://localhost:9170/Keyspace1";
        props = Utils.parseURL(noversion);
        assertEquals("localhost", props.getProperty(Utils.TAG_SERVER_NAME));
        assertEquals("9170", props.getProperty(Utils.TAG_PORT_NUMBER));
        assertEquals("Keyspace1", props.getProperty(Utils.TAG_DATABASE_NAME));
        assertNull(props.getProperty(Utils.TAG_CQL_VERSION));
        
        String nokeyspaceonly = "jdbc:cassandra://localhost:9170?version=2.0.0";
        props = Utils.parseURL(nokeyspaceonly);
        assertEquals("localhost", props.getProperty(Utils.TAG_SERVER_NAME));
        assertEquals("9170", props.getProperty(Utils.TAG_PORT_NUMBER));
        assertNull(props.getProperty(Utils.TAG_DATABASE_NAME));
        assertEquals("2.0.0", props.getProperty(Utils.TAG_CQL_VERSION));
        
        String nokeyspaceorver = "jdbc:cassandra://localhost:9170";
        props = Utils.parseURL(nokeyspaceorver);
        assertEquals("localhost", props.getProperty(Utils.TAG_SERVER_NAME));
        assertEquals("9170", props.getProperty(Utils.TAG_PORT_NUMBER));
        assertNull(props.getProperty(Utils.TAG_DATABASE_NAME));
        assertNull(props.getProperty(Utils.TAG_CQL_VERSION));
        
        String withloadbalancingpolicy = "jdbc:cassandra://localhost:9170?loadbalancing=TokenAwarePolicy-DCAwareRoundRobinPolicy&primarydc=DC1";
        props = Utils.parseURL(withloadbalancingpolicy);
        assertEquals("localhost", props.getProperty(Utils.TAG_SERVER_NAME));
        assertEquals("9170", props.getProperty(Utils.TAG_PORT_NUMBER));
        assertNull(props.getProperty(Utils.TAG_DATABASE_NAME));
        assertNull(props.getProperty(Utils.TAG_CQL_VERSION));
        assertEquals("TokenAwarePolicy-DCAwareRoundRobinPolicy", props.getProperty(Utils.TAG_LOADBALANCING_POLICY));
        assertEquals("DC1", props.getProperty(Utils.TAG_PRIMARY_DC));
    }
  
    @Test
    public void testCreateSubName() throws Exception
    {
        String happypath = "jdbc:cassandra://localhost:9170/Keyspace1?consistency=QUORUM&version=3.0.0";
        Properties props = Utils.parseURL(happypath);
        
        if (LOG.isDebugEnabled()) LOG.debug("happypath    = '{}'", happypath);

        
        String result = Utils.createSubName(props);
        if (LOG.isDebugEnabled()) LOG.debug("result       = '{}'", Utils.PROTOCOL+result);
        
        assertEquals(happypath, Utils.PROTOCOL+result);
    }
}
