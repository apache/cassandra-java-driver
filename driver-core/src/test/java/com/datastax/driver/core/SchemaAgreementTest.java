/*
 *      Copyright (C) 2012-2015 DataStax Inc.
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

import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.utils.UUIDs;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class SchemaAgreementTest {

    // Don't use "IF EXISTS" to remain compatible with older C* versions
    static final String CREATE_TABLE = "CREATE TABLE foo (k int primary key, v int)";
    static final String DROP_TABLE = "DROP TABLE foo";

    CCMBridge ccm;
    Cluster cluster;
    Session session;
    ProtocolOptions protocolOptions;

    @BeforeClass(groups = "short")
    public void setup() {
        ccm = CCMBridge.builder().withNodes(2).build();
        cluster = Cluster.builder().addContactPoint(CCMBridge.ipOfNode(1)).build();
        session = cluster.connect();
        session.execute("create keyspace test with replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        session.execute("use test");
        protocolOptions = cluster.getConfiguration().getProtocolOptions();
    }

    @Test(groups = "short")
    public void should_set_flag_on_successful_agreement() {
        protocolOptions.maxSchemaAgreementWaitSeconds = 10;
        ResultSet rs = session.execute(CREATE_TABLE);
        assertThat(rs.getExecutionInfo().isSchemaInAgreement()).isTrue();
    }

    @Test(groups = "short")
    public void should_set_flag_on_non_schema_altering_statement() {
        protocolOptions.maxSchemaAgreementWaitSeconds = 10;
        ResultSet rs = session.execute("select release_version from system.local");
        assertThat(rs.getExecutionInfo().isSchemaInAgreement()).isTrue();
    }

    @Test(groups = "short")
    public void should_unset_flag_on_failed_agreement() {
        // Setting to 0 results in no query being set, so agreement fails
        protocolOptions.maxSchemaAgreementWaitSeconds = 0;
        ResultSet rs = session.execute(CREATE_TABLE);
        assertThat(rs.getExecutionInfo().isSchemaInAgreement()).isFalse();
    }

    @Test(groups = "short")
    public void should_check_agreement_through_cluster_metadata() {
        Cluster controlCluster = null;
        try {
            controlCluster = TestUtils.buildControlCluster(cluster);
            Session controlSession = controlCluster.connect();

            Row localRow = controlSession.execute("SELECT schema_version FROM system.local").one();
            UUID localVersion = localRow.getUUID("schema_version");
            Row peerRow = controlSession.execute("SELECT peer, schema_version FROM system.peers").one();
            InetAddress peerAddress = peerRow.getInet("peer");
            UUID peerVersion = peerRow.getUUID("schema_version");
            // The two nodes should be in agreement at this point, but check just in case:
            assertThat(localVersion).isEqualTo(peerVersion);

            // Now check the method under test:
            assertThat(cluster.getMetadata().checkSchemaAgreement()).isTrue();

            // Insert a fake version to simulate a disagreement:
            forceSchemaVersion(controlSession, peerAddress, UUIDs.random());
            assertThat(cluster.getMetadata().checkSchemaAgreement()).isFalse();

            forceSchemaVersion(controlSession, peerAddress, peerVersion);
        } finally {
            if (controlCluster != null)
                controlCluster.close();
        }
    }

    private static void forceSchemaVersion(Session session, InetAddress peerAddress, UUID schemaVersion) {
        session.execute(String.format("UPDATE system.peers SET schema_version = %s WHERE peer = %s",
                DataType.uuid().format(schemaVersion), DataType.inet().format(peerAddress)));
    }

    @AfterMethod(groups = "short")
    public void cleanupSchema() {
        protocolOptions.maxSchemaAgreementWaitSeconds = 10;
        try {
            session.execute(DROP_TABLE);
        } catch (DriverException e) { /*ignore*/ }
    }

    @AfterClass(groups = "short")
    public void teardown() {
        if (cluster != null)
            cluster.close();
        if (ccm != null)
            ccm.remove();
    }
}

