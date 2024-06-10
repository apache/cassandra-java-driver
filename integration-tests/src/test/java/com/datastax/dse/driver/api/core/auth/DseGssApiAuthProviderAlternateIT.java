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
package com.datastax.dse.driver.api.core.auth;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.dse.driver.internal.core.auth.DseGssApiAuthProvider;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.testinfra.requirement.BackendRequirement;
import com.datastax.oss.driver.api.testinfra.requirement.BackendType;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

@BackendRequirement(
    type = BackendType.DSE,
    minInclusive = "5.0",
    description = "Required for DseAuthenticator")
@RunWith(DataProviderRunner.class)
public class DseGssApiAuthProviderAlternateIT {
  @ClassRule public static EmbeddedAdsRule ads = new EmbeddedAdsRule(true);

  @DataProvider
  public static Object[][] saslSystemProperties() {
    return new Object[][] {{"dse.sasl.service"}, {"dse.sasl.protocol"}};
  }

  @Test
  @UseDataProvider("saslSystemProperties")
  public void
      should_authenticate_using_kerberos_with_keytab_and_alternate_service_principal_using_system_property(
          String saslSystemProperty) {
    System.setProperty(saslSystemProperty, "alternate");
    try (CqlSession session =
        SessionUtils.newSession(
            ads.getCcm(),
            SessionUtils.configLoaderBuilder()
                .withClass(DefaultDriverOption.AUTH_PROVIDER_CLASS, DseGssApiAuthProvider.class)
                .withStringMap(
                    DseDriverOption.AUTH_PROVIDER_SASL_PROPERTIES,
                    ImmutableMap.of("javax.security.sasl.qop", "auth-conf"))
                .withStringMap(
                    DseDriverOption.AUTH_PROVIDER_LOGIN_CONFIGURATION,
                    ImmutableMap.of(
                        "principal",
                        ads.getUserPrincipal(),
                        "useKeyTab",
                        "true",
                        "refreshKrb5Config",
                        "true",
                        "keyTab",
                        ads.getUserKeytab().getAbsolutePath()))
                .build())) {
      Row row = session.execute("select * from system.local").one();
      assertThat(row).isNotNull();
    } finally {
      System.clearProperty(saslSystemProperty);
    }
  }

  @Test
  public void should_authenticate_using_kerberos_with_keytab_and_alternate_service_principal() {
    try (CqlSession session =
        SessionUtils.newSession(
            ads.getCcm(),
            SessionUtils.configLoaderBuilder()
                .withClass(DefaultDriverOption.AUTH_PROVIDER_CLASS, DseGssApiAuthProvider.class)
                .withString(DseDriverOption.AUTH_PROVIDER_SERVICE, "alternate")
                .withStringMap(
                    DseDriverOption.AUTH_PROVIDER_SASL_PROPERTIES,
                    ImmutableMap.of("javax.security.sasl.qop", "auth-conf"))
                .withStringMap(
                    DseDriverOption.AUTH_PROVIDER_LOGIN_CONFIGURATION,
                    ImmutableMap.of(
                        "principal",
                        ads.getUserPrincipal(),
                        "useKeyTab",
                        "true",
                        "refreshKrb5Config",
                        "true",
                        "keyTab",
                        ads.getUserKeytab().getAbsolutePath()))
                .build())) {
      Row row = session.execute("select * from system.local").one();
      assertThat(row).isNotNull();
    }
  }
}
