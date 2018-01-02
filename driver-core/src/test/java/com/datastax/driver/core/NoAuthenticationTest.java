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

import org.apache.log4j.Level;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

@CCMConfig(createCluster = false)
public class NoAuthenticationTest extends CCMTestsSupport {

    @Test(groups = "short")
    public void should_warn_if_auth_configured_but_server_does_not_send_challenge() {
        Cluster cluster = register(Cluster.builder()
                .addContactPoints(getContactPoints())
                .withPort(ccm().getBinaryPort())
                .withCredentials("bogus", "bogus")
                .build());
        Level previous = TestUtils.setLogLevel(Connection.class, Level.WARN);
        MemoryAppender logs = new MemoryAppender().enableFor(Connection.class);
        try {
            cluster.init();
        } finally {
            TestUtils.setLogLevel(Connection.class, previous);
            logs.disableFor(Connection.class);
        }
        assertThat(logs.get())
                .contains("did not send an authentication challenge; " +
                        "This is suspicious because the driver expects authentication " +
                        "(configured auth provider = com.datastax.driver.core.PlainTextAuthProvider)");
    }
}
