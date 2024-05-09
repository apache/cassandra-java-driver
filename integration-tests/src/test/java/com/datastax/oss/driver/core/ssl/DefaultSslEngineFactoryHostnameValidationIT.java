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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.testinfra.ccm.CcmBridge;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.internal.core.ssl.DefaultSslEngineFactory;
import org.junit.ClassRule;
import org.junit.Test;

public class DefaultSslEngineFactoryHostnameValidationIT {

  @ClassRule
  public static final CustomCcmRule CCM_RULE = CustomCcmRule.builder().withSslLocalhostCn().build();

  /**
   * Ensures that SSL connectivity can be established with hostname validation enabled when the
   * server's certificate has a common name that matches its hostname. In this case the certificate
   * uses a CN of 'localhost' which is expected to work, but may not if localhost does not resolve
   * to 127.0.0.1.
   */
  @Test
  public void should_connect_if_hostname_validation_enabled_and_hostname_matches() {
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withClass(DefaultDriverOption.SSL_ENGINE_FACTORY_CLASS, DefaultSslEngineFactory.class)
            .withBoolean(DefaultDriverOption.SSL_HOSTNAME_VALIDATION, true)
            .withString(
                DefaultDriverOption.SSL_TRUSTSTORE_PATH,
                CcmBridge.DEFAULT_CLIENT_TRUSTSTORE_FILE.getAbsolutePath())
            .withString(
                DefaultDriverOption.SSL_TRUSTSTORE_PASSWORD,
                CcmBridge.DEFAULT_CLIENT_TRUSTSTORE_PASSWORD)
            .build();
    try (CqlSession session = SessionUtils.newSession(CCM_RULE, loader)) {
      session.execute("select * from system.local");
    }
  }
}
