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

import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.net.InetSocketAddress;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A simple test to validate jmx ports work
 */
@Test
@CCMConfig(numberOfNodes = 3)
public class CCMBridgeTest extends CCMTestsSupport {

    @Test(groups = "short")
    public void should_make_JMX_connection() throws Exception {
        InetSocketAddress addr1 = ccm().jmxAddressOfNode(1);
        InetSocketAddress addr2 = ccm().jmxAddressOfNode(2);
        InetSocketAddress addr3 = ccm().jmxAddressOfNode(3);

        assertThat(addr1.getPort()).isNotEqualTo(addr2.getPort());
        assertThat(addr1.getPort()).isNotEqualTo(addr3.getPort());
        assertThat(addr2.getPort()).isNotEqualTo(addr3.getPort());

        JMXServiceURL url = new JMXServiceURL(
                String.format(
                        "service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi",
                        addr2.getAddress().getHostAddress(), addr2.getPort()
                )
        );
        JMXConnector jmxc = JMXConnectorFactory.connect(url, null);
        assertThat(jmxc.getConnectionId().isEmpty()).isFalse();
    }

    @Test(groups = "short")
    public void should_configure_JMX_ports_through_builder() throws Exception {
        CCMBridge.Builder ccmBuilder = CCMBridge.builder().withNodes(3).notStarted().withJmxPorts(12345);
        CCMAccess ccm = ccmBuilder.build();
        assertThat(ccm.jmxAddressOfNode(1).getPort()).isEqualTo(12345);

        int port2 = ccm.jmxAddressOfNode(2).getPort();
        int port3 = ccm.jmxAddressOfNode(3).getPort();
        assertThat(port2).isBetween(0, 65535);
        assertThat(port3).isBetween(0, 65535);
    }
}