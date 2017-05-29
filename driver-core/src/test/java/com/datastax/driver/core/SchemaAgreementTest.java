/*
 * Copyright (C) 2012-2017 DataStax Inc.
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
package com.datastax.driver.core;

import com.datastax.driver.core.utils.UUIDs;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@CCMConfig(numberOfNodes = 2)
public class SchemaAgreementTest extends CCMTestsSupport {

    // Don't use "IF EXISTS" to remain compatible with older C* versions
    static final String CREATE_TABLE = "CREATE TABLE table_%s (k int primary key, v int)";

    static final AtomicInteger COUNTER = new AtomicInteger(1);

    @Test(groups = "short")
    public void should_set_flag_on_successful_agreement() {
        ProtocolOptions protocolOptions = cluster().getConfiguration().getProtocolOptions();
        protocolOptions.maxSchemaAgreementWaitSeconds = 10;
        ResultSet rs = session().execute(String.format(CREATE_TABLE, COUNTER.getAndIncrement()));
        assertThat(rs.getExecutionInfo().isSchemaInAgreement()).isTrue();
    }

    @Test(groups = "short")
    public void should_set_flag_on_non_schema_altering_statement() {
        ProtocolOptions protocolOptions = cluster().getConfiguration().getProtocolOptions();
        protocolOptions.maxSchemaAgreementWaitSeconds = 10;
        ResultSet rs = session().execute("select release_version from system.local");
        assertThat(rs.getExecutionInfo().isSchemaInAgreement()).isTrue();
    }

    @Test(groups = "short")
    public void should_unset_flag_on_failed_agreement() {
        // Setting to 0 results in no query being set, so agreement fails
        ProtocolOptions protocolOptions = cluster().getConfiguration().getProtocolOptions();
        protocolOptions.maxSchemaAgreementWaitSeconds = 0;
        ResultSet rs = session().execute(String.format(CREATE_TABLE, COUNTER.getAndIncrement()));
        assertThat(rs.getExecutionInfo().isSchemaInAgreement()).isFalse();
    }

    @Test(groups = "short")
    public void should_check_agreement_through_cluster_metadata() {
        Cluster controlCluster = register(TestUtils.buildControlCluster(cluster(), ccm()));
        Session controlSession = controlCluster.connect();

        Row localRow = controlSession.execute("SELECT schema_version FROM system.local").one();
        UUID localVersion = localRow.getUUID("schema_version");
        Row peerRow = controlSession.execute("SELECT peer, schema_version FROM system.peers").one();
        InetAddress peerAddress = peerRow.getInet("peer");
        UUID peerVersion = peerRow.getUUID("schema_version");
        // The two nodes should be in agreement at this point, but check just in case:
        assertThat(localVersion).isEqualTo(peerVersion);

        // Now check the method under test:
        assertThat(cluster().getMetadata().checkSchemaAgreement()).isTrue();

        // Insert a fake version to simulate a disagreement:
        forceSchemaVersion(controlSession, peerAddress, UUIDs.random());
        assertThat(cluster().getMetadata().checkSchemaAgreement()).isFalse();

        forceSchemaVersion(controlSession, peerAddress, peerVersion);
    }

    private static void forceSchemaVersion(Session session, InetAddress peerAddress, UUID schemaVersion) {
        session.execute(String.format("UPDATE system.peers SET schema_version = %s WHERE peer = %s",
                TypeCodec.uuid().format(schemaVersion), TypeCodec.inet().format(peerAddress)));
    }

}

