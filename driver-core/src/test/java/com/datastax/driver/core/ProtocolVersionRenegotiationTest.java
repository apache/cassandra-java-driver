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

import com.datastax.driver.core.exceptions.UnsupportedProtocolVersionException;
import com.datastax.driver.core.utils.CassandraVersion;
import org.testng.SkipException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.datastax.driver.core.ProtocolVersion.*;
import static org.assertj.core.api.Assertions.assertThat;

public class ProtocolVersionRenegotiationTest extends CCMTestsSupport {

    private ProtocolVersion protocolVersion;

    @BeforeMethod(groups = "short")
    public void setUp() {
        protocolVersion = ccm().getProtocolVersion();
    }

    /**
     * @jira_ticket JAVA-1367
     */
    @Test(groups = "short")
    public void should_succeed_when_version_provided_and_matches() throws Exception {
        Cluster cluster = connectWithVersion(protocolVersion);
        assertThat(actualProtocolVersion(cluster)).isEqualTo(protocolVersion);
    }

    /**
     * @jira_ticket JAVA-1367
     */
    @Test(groups = "short")
    @CassandraVersion("3.8")
    public void should_fail_when_version_provided_and_too_low_3_8_plus() throws Exception {
        UnsupportedProtocolVersionException e = connectWithUnsupportedVersion(V1);
        assertThat(e.getUnsupportedVersion()).isEqualTo(V1);
        // post-CASSANDRA-11464: server replies with client's version
        assertThat(e.getServerVersion()).isEqualTo(V1);
    }

    /**
     * @jira_ticket JAVA-1367
     */
    @Test(groups = "short")
    public void should_fail_when_version_provided_and_too_high() throws Exception {
        if (ccm().getCassandraVersion().compareTo(VersionNumber.parse("2.2")) >= 0) {
            throw new SkipException("Server supports protocol V4");
        }
        UnsupportedProtocolVersionException e = connectWithUnsupportedVersion(V4);
        assertThat(e.getUnsupportedVersion()).isEqualTo(V4);
        // pre-CASSANDRA-11464: server replies with its own version
        assertThat(e.getServerVersion()).isEqualTo(protocolVersion);
    }

    /**
     * @jira_ticket JAVA-1367
     */
    @Test(groups = "short")
    public void should_fail_when_beta_allowed_and_too_high() throws Exception {
        if (ccm().getCassandraVersion().compareTo(VersionNumber.parse("3.10")) >= 0) {
            throw new SkipException("Server supports protocol protocol V5 beta");
        }
        UnsupportedProtocolVersionException e = connectWithUnsupportedBetaVersion();
        assertThat(e.getUnsupportedVersion()).isEqualTo(V5);
    }

    /**
     * @jira_ticket JAVA-1367
     */
    @Test(groups = "short")
    @CCMConfig(version = "2.1.16", createCluster = false)
    public void should_negotiate_when_no_version_provided() throws Exception {
        if (protocolVersion.compareTo(ProtocolVersion.NEWEST_SUPPORTED) >= 0) {
            throw new SkipException("Server supports newest protocol version driver supports");
        }
        Cluster cluster = connectWithoutVersion();
        assertThat(actualProtocolVersion(cluster)).isEqualTo(protocolVersion);
    }

    private UnsupportedProtocolVersionException connectWithUnsupportedVersion(ProtocolVersion version) {
        Cluster cluster = register(Cluster.builder()
                .addContactPoints(getContactPoints())
                .withPort(ccm().getBinaryPort())
                .withProtocolVersion(version)
                .build());
        return initWithUnsupportedVersion(cluster);
    }

    private UnsupportedProtocolVersionException connectWithUnsupportedBetaVersion() {
        Cluster cluster = register(Cluster.builder()
                .addContactPoints(getContactPoints())
                .withPort(ccm().getBinaryPort())
                .allowBetaProtocolVersion()
                .build());
        return initWithUnsupportedVersion(cluster);
    }

    private UnsupportedProtocolVersionException initWithUnsupportedVersion(Cluster cluster) {
        Throwable t = null;
        try {
            cluster.init();
        } catch (Throwable t2) {
            t = t2;
        }
        if (t instanceof UnsupportedProtocolVersionException) {
            return (UnsupportedProtocolVersionException) t;
        } else {
            throw new AssertionError("Expected UnsupportedProtocolVersionException, got " + t);
        }
    }

    private Cluster connectWithVersion(ProtocolVersion version) {
        Cluster cluster = register(Cluster.builder()
                .addContactPoints(getContactPoints())
                .withPort(ccm().getBinaryPort())
                .withProtocolVersion(version)
                .build());
        cluster.init();
        return cluster;
    }

    private Cluster connectWithoutVersion() {
        Cluster cluster = register(Cluster.builder()
                .addContactPoints(getContactPoints())
                .withPort(ccm().getBinaryPort())
                .build());
        cluster.init();
        return cluster;
    }

    private ProtocolVersion actualProtocolVersion(Cluster cluster) {
        return cluster.getConfiguration().getProtocolOptions().getProtocolVersion();
    }

}
