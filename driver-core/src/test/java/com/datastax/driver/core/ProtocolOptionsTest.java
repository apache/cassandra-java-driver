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

import static com.datastax.driver.core.Assertions.assertThat;

public class ProtocolOptionsTest extends CCMTestsSupport {

    /**
     * @jira_ticket JAVA-1209
     */
    @Test(groups = "unit")
    public void getProtocolVersion_should_return_null_if_not_connected() {
        Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        assertThat(cluster.getConfiguration().getProtocolOptions().getProtocolVersion()).isNull();
    }

    /**
     * @jira_ticket JAVA-1209
     */
    @Test(groups = "short")
    public void getProtocolVersion_should_return_version() throws InterruptedException {
        ProtocolVersion version = cluster().getConfiguration().getProtocolOptions().getProtocolVersion();
        assertThat(version).isNotNull();
    }
}
