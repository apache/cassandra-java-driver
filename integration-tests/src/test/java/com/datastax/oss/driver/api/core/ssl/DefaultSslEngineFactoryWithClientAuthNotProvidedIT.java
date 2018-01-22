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
import com.datastax.oss.driver.api.testinfra.cluster.SessionUtils;
import com.datastax.oss.driver.categories.IsolatedTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IsolatedTests.class)
public class DefaultSslEngineFactoryWithClientAuthNotProvidedIT {

  @ClassRule public static CustomCcmRule ccm = CustomCcmRule.builder().withSslAuth().build();

  @Test(expected = AllNodesFailedException.class)
  public void should_not_connect_with_ssl_using_client_auth_if_keystore_not_set() {
    System.setProperty(
        "javax.net.ssl.trustStore", CcmBridge.DEFAULT_CLIENT_TRUSTSTORE_FILE.getAbsolutePath());
    System.setProperty(
        "javax.net.ssl.trustStorePassword", CcmBridge.DEFAULT_CLIENT_TRUSTSTORE_PASSWORD);
    try (CqlSession session =
        SessionUtils.newSession(
            ccm,
            "ssl-engine-factory.class = com.datastax.oss.driver.api.core.ssl.DefaultSslEngineFactory")) {
      session.execute("select * from system.local");
    }
  }
}
