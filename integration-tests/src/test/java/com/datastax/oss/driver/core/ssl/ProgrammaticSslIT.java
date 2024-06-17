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

/*
 * Copyright (C) 2022 ScyllaDB
 *
 * Modified by ScyllaDB
 */
package com.datastax.oss.driver.core.ssl;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.ssl.ProgrammaticSslEngineFactory;
import com.datastax.oss.driver.api.core.ssl.SslEngineFactory;
import com.datastax.oss.driver.api.testinfra.ccm.CcmBridge;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.SecureRandom;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.junit.ClassRule;
import org.junit.Test;

public class ProgrammaticSslIT {

  @ClassRule public static final CustomCcmRule CCM_RULE = CustomCcmRule.builder().withSsl().build();

  @Test
  public void should_connect_with_programmatic_factory() {
    SslEngineFactory factory = new ProgrammaticSslEngineFactory(createSslContext());
    try (CqlSession session =
        (CqlSession)
            SessionUtils.baseBuilder()
                .addContactEndPoints(CCM_RULE.getContactPoints())
                .withSslEngineFactory(factory)
                .build()) {
      session.execute("select * from system.local");
    }
  }

  @Test
  public void should_connect_with_programmatic_ssl_context() {
    SSLContext sslContext = createSslContext();
    try (CqlSession session =
        (CqlSession)
            SessionUtils.baseBuilder()
                .addContactEndPoints(CCM_RULE.getContactPoints())
                .withSslContext(sslContext)
                .build()) {
      session.execute("select * from system.local");
    }
  }

  private static SSLContext createSslContext() {
    try {
      SSLContext context = SSLContext.getInstance("SSL");
      TrustManagerFactory tmf =
          TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      try (InputStream tsf =
          Files.newInputStream(
              Paths.get(CcmBridge.DEFAULT_CLIENT_TRUSTSTORE_FILE.getAbsolutePath()))) {
        KeyStore ts = KeyStore.getInstance("JKS");
        char[] password = CcmBridge.DEFAULT_CLIENT_TRUSTSTORE_PASSWORD.toCharArray();
        ts.load(tsf, password);
        tmf.init(ts);
      }
      KeyManagerFactory kmf =
          KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      try (InputStream ksf =
          Files.newInputStream(
              Paths.get(CcmBridge.DEFAULT_CLIENT_KEYSTORE_FILE.getAbsolutePath()))) {
        KeyStore ks = KeyStore.getInstance("JKS");
        char[] password = CcmBridge.DEFAULT_CLIENT_KEYSTORE_PASSWORD.toCharArray();
        ks.load(ksf, password);
        kmf.init(ks, password);
      }
      context.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new SecureRandom());
      return context;
    } catch (Exception e) {
      throw new AssertionError("Unexpected error while creating SSL context", e);
    }
  }
}
