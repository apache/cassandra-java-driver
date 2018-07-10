/*
 * Copyright DataStax, Inc.
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
package com.datastax.oss.driver.api.core.ssl;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.testinfra.ccm.CcmBridge;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import org.junit.ClassRule;
import org.junit.Test;

public class DefaultSslEngineFactoryIT {

  @ClassRule public static CustomCcmRule ccm = CustomCcmRule.builder().withSsl().build();

  @Test
  public void should_connect_with_ssl() {
    try (CqlSession session =
        SessionUtils.newSession(
            ccm,
            "advanced.ssl-engine-factory.class = DefaultSslEngineFactory",
            "advanced.ssl-engine-factory.hostname-validation = false",
            "advanced.ssl-engine-factory.truststore-path = "
                + CcmBridge.DEFAULT_CLIENT_TRUSTSTORE_FILE.getAbsolutePath(),
            "advanced.ssl-engine-factory.truststore-password = "
                + CcmBridge.DEFAULT_CLIENT_TRUSTSTORE_PASSWORD)) {
      session.execute("select * from system.local");
    }
  }

  @Test(expected = AllNodesFailedException.class)
  public void should_not_connect_if_hostname_validation_enabled_and_hostname_does_not_match() {
    // should not succeed as certificate does not have a CN that would match hostname,
    // (unless hostname is node1).
    try (CqlSession session =
        SessionUtils.newSession(
            ccm,
            "advanced.ssl-engine-factory.class = DefaultSslEngineFactory",
            "advanced.ssl-engine-factory.truststore-path = "
                + CcmBridge.DEFAULT_CLIENT_TRUSTSTORE_FILE.getAbsolutePath(),
            "advanced.ssl-engine-factory.truststore-password = "
                + CcmBridge.DEFAULT_CLIENT_TRUSTSTORE_PASSWORD)) {
      session.execute("select * from system.local");
    }
  }

  @Test(expected = AllNodesFailedException.class)
  public void should_not_connect_if_truststore_not_provided() {
    try (CqlSession session =
        SessionUtils.newSession(
            ccm,
            "advanced.ssl-engine-factory.class = DefaultSslEngineFactory",
            "advanced.ssl-engine-factory.hostname-validation = false")) {
      session.execute("select * from system.local");
    }
  }

  @Test(expected = AllNodesFailedException.class)
  public void should_not_connect_if_not_using_ssl() {
    try (CqlSession session = SessionUtils.newSession(ccm)) {
      session.execute("select * from system.local");
    }
  }
}
