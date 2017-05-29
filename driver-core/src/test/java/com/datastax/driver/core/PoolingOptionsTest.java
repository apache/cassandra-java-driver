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

import org.testng.annotations.Test;

import static com.datastax.driver.core.HostDistance.LOCAL;
import static com.datastax.driver.core.HostDistance.REMOTE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class PoolingOptionsTest {

    @Test(groups = "unit")
    public void should_initialize_to_v2_defaults_if_v2_or_below() {
        PoolingOptions options = new PoolingOptions();
        options.setProtocolVersion(ProtocolVersion.V1);

        assertThat(options.getCoreConnectionsPerHost(LOCAL)).isEqualTo(2);
        assertThat(options.getMaxConnectionsPerHost(LOCAL)).isEqualTo(8);
        assertThat(options.getCoreConnectionsPerHost(REMOTE)).isEqualTo(1);
        assertThat(options.getMaxConnectionsPerHost(REMOTE)).isEqualTo(2);
        assertThat(options.getNewConnectionThreshold(LOCAL)).isEqualTo(100);
        assertThat(options.getNewConnectionThreshold(REMOTE)).isEqualTo(100);
        assertThat(options.getMaxRequestsPerConnection(LOCAL)).isEqualTo(128);
        assertThat(options.getMaxRequestsPerConnection(REMOTE)).isEqualTo(128);
    }

    @Test(groups = "unit")
    public void should_initialize_to_v3_defaults_if_v3_or_above() {
        PoolingOptions options = new PoolingOptions();
        options.setProtocolVersion(ProtocolVersion.V3);

        assertThat(options.getCoreConnectionsPerHost(LOCAL)).isEqualTo(1);
        assertThat(options.getMaxConnectionsPerHost(LOCAL)).isEqualTo(1);
        assertThat(options.getCoreConnectionsPerHost(REMOTE)).isEqualTo(1);
        assertThat(options.getMaxConnectionsPerHost(REMOTE)).isEqualTo(1);
        assertThat(options.getNewConnectionThreshold(LOCAL)).isEqualTo(800);
        assertThat(options.getNewConnectionThreshold(REMOTE)).isEqualTo(200);
        assertThat(options.getMaxRequestsPerConnection(LOCAL)).isEqualTo(1024);
        assertThat(options.getMaxRequestsPerConnection(REMOTE)).isEqualTo(256);
    }

    @Test(groups = "unit")
    public void should_enforce_invariants_once_protocol_version_known() {
        // OK for v2 (default max = 8)
        PoolingOptions options = new PoolingOptions().setCoreConnectionsPerHost(LOCAL, 3);
        options.setCoreConnectionsPerHost(LOCAL, 3);
        options.setProtocolVersion(ProtocolVersion.V2);
        assertThat(options.getCoreConnectionsPerHost(LOCAL)).isEqualTo(3);
        assertThat(options.getMaxConnectionsPerHost(LOCAL)).isEqualTo(8);

        // KO for v3 (default max = 1)
        options = new PoolingOptions().setCoreConnectionsPerHost(LOCAL, 3);
        try {
            options.setProtocolVersion(ProtocolVersion.V3);
            fail("Expected an IllegalArgumentException");
        } catch (IllegalArgumentException e) {/*expected*/}

        // OK for v3 (up to 32K stream ids)
        options = new PoolingOptions().setMaxRequestsPerConnection(LOCAL, 5000);
        options.setProtocolVersion(ProtocolVersion.V3);
        assertThat(options.getMaxRequestsPerConnection(LOCAL)).isEqualTo(5000);

        // KO for v2 (up to 128)
        options = new PoolingOptions().setMaxRequestsPerConnection(LOCAL, 5000);
        try {
            options.setProtocolVersion(ProtocolVersion.V2);
            fail("Expected an IllegalArgumentException");
        } catch (IllegalArgumentException e) {/*expected*/}
    }

    @Test(groups = "unit")
    public void should_set_core_and_max_connections_simultaneously() {
        PoolingOptions options = new PoolingOptions();
        options.setProtocolVersion(ProtocolVersion.V2);

        options.setConnectionsPerHost(LOCAL, 10, 15);

        assertThat(options.getCoreConnectionsPerHost(LOCAL)).isEqualTo(10);
        assertThat(options.getMaxConnectionsPerHost(LOCAL)).isEqualTo(15);
    }

    @Test(groups = "unit")
    public void should_leave_connection_options_unset_until_protocol_version_known() {
        PoolingOptions options = new PoolingOptions();

        assertThat(options.getCoreConnectionsPerHost(LOCAL)).isEqualTo(PoolingOptions.UNSET);
        assertThat(options.getCoreConnectionsPerHost(REMOTE)).isEqualTo(PoolingOptions.UNSET);
        assertThat(options.getMaxConnectionsPerHost(LOCAL)).isEqualTo(PoolingOptions.UNSET);
        assertThat(options.getMaxConnectionsPerHost(REMOTE)).isEqualTo(PoolingOptions.UNSET);
        assertThat(options.getNewConnectionThreshold(LOCAL)).isEqualTo(PoolingOptions.UNSET);
        assertThat(options.getNewConnectionThreshold(REMOTE)).isEqualTo(PoolingOptions.UNSET);
        assertThat(options.getMaxRequestsPerConnection(LOCAL)).isEqualTo(PoolingOptions.UNSET);
        assertThat(options.getMaxRequestsPerConnection(REMOTE)).isEqualTo(PoolingOptions.UNSET);
    }

    @Test(groups = "unit")
    public void should_reject_negative_connection_options_even_when_protocol_version_unknown() {
        PoolingOptions options = new PoolingOptions();

        try {
            options.setCoreConnectionsPerHost(LOCAL, -1);
            fail("expected an IllegalArgumentException");
        } catch (IllegalArgumentException e) { /*expected*/ }
        try {
            options.setMaxConnectionsPerHost(LOCAL, -1);
            fail("expected an IllegalArgumentException");
        } catch (IllegalArgumentException e) { /*expected*/ }
        try {
            options.setConnectionsPerHost(LOCAL, -1, 1);
            fail("expected an IllegalArgumentException");
        } catch (IllegalArgumentException e) { /*expected*/ }
        try {
            options.setConnectionsPerHost(LOCAL, -2, -1);
            fail("expected an IllegalArgumentException");
        } catch (IllegalArgumentException e) { /*expected*/ }
        try {
            options.setNewConnectionThreshold(LOCAL, -1);
            fail("expected an IllegalArgumentException");
        } catch (IllegalArgumentException e) { /*expected*/ }
        try {
            options.setMaxRequestsPerConnection(LOCAL, -1);
            fail("expected an IllegalArgumentException");
        } catch (IllegalArgumentException e) { /*expected*/ }
    }
}

