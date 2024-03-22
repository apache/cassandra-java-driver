/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.core.ssl;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.testinfra.ccm.CcmBridge;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.internal.core.ssl.PEMBasedSslEngineFactory;
import org.junit.ClassRule;
import org.junit.Test;

public class PEMBasedSslEngineFactoryWithClientAuthIT {

  @ClassRule
  public static final CustomCcmRule CCM_RULE = CustomCcmRule.builder().withSslAuth().build();

  @Test
  public void should_connect_with_ssl_using_client_auth() {
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withClass(DefaultDriverOption.SSL_ENGINE_FACTORY_CLASS, PEMBasedSslEngineFactory.class)
            .withBoolean(DefaultDriverOption.SSL_HOSTNAME_VALIDATION, false)
            .withString(
                DefaultDriverOption.SSL_PRIVATE_KEY_PATH,
                CcmBridge.DEFAULT_CLIENT_PRIVATE_KEY_FILE.getAbsolutePath())
            .withString(
                DefaultDriverOption.SSL_CLIENT_CERT_PATH,
                CcmBridge.DEFAULT_CLIENT_CERT_CHAIN_FILE.getAbsolutePath())
            .withString(
                DefaultDriverOption.SSL_TRUSTSTORE_PATH,
                CcmBridge.DEFAULT_CERT_FILE.getAbsolutePath())
            .build();
    try (CqlSession session = SessionUtils.newSession(CCM_RULE, loader)) {
      session.execute("select * from system.local");
    }
  }

  @Test(expected = AllNodesFailedException.class)
  public void should_not_connect_with_ssl_using_client_auth_if_keystore_not_set() {
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withClass(DefaultDriverOption.SSL_ENGINE_FACTORY_CLASS, PEMBasedSslEngineFactory.class)
            .withBoolean(DefaultDriverOption.SSL_HOSTNAME_VALIDATION, false)
            .withString(
                DefaultDriverOption.SSL_TRUSTSTORE_PATH,
                CcmBridge.DEFAULT_CERT_FILE.getAbsolutePath())
            .build();
    try (CqlSession session = SessionUtils.newSession(CCM_RULE, loader)) {
      session.execute("select * from system.local");
    }
  }
}
