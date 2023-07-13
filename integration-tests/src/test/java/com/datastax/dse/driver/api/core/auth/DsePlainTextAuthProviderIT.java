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
import static org.assertj.core.api.Fail.fail;

import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.auth.AuthenticationException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.requirement.BackendRequirement;
import com.datastax.oss.driver.api.testinfra.requirement.BackendType;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.internal.core.auth.PlainTextAuthProvider;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.Uninterruptibles;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

@BackendRequirement(
    type = BackendType.DSE,
    minInclusive = "5.0",
    description = "Required for DseAuthenticator")
public class DsePlainTextAuthProviderIT {

  @ClassRule
  public static CustomCcmRule ccm =
      CustomCcmRule.builder()
          .withCassandraConfiguration(
              "authenticator", "com.datastax.bdp.cassandra.auth.DseAuthenticator")
          .withDseConfiguration("authentication_options.enabled", true)
          .withDseConfiguration("authentication_options.default_scheme", "internal")
          .withJvmArgs("-Dcassandra.superuser_setup_delay_ms=0")
          .build();

  @BeforeClass
  public static void sleepForAuth() {
    if (ccm.getCassandraVersion().compareTo(Version.V2_2_0) < 0) {
      // Sleep for 1 second to allow C* auth to do its work.  This is only needed for 2.1
      Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
    }
  }

  @Test
  public void should_connect_dse_plaintext_auth() {
    try (CqlSession session =
        SessionUtils.newSession(
            ccm,
            SessionUtils.configLoaderBuilder()
                .withString(DseDriverOption.AUTH_PROVIDER_AUTHORIZATION_ID, "")
                .withString(DefaultDriverOption.AUTH_PROVIDER_USER_NAME, "cassandra")
                .withString(DefaultDriverOption.AUTH_PROVIDER_PASSWORD, "cassandra")
                .withClass(DefaultDriverOption.AUTH_PROVIDER_CLASS, PlainTextAuthProvider.class)
                .build())) {
      session.execute("select * from system.local");
    }
  }

  @Test
  public void should_connect_dse_plaintext_auth_programmatically() {
    try (CqlSession session =
        CqlSession.builder()
            .addContactEndPoints(ccm.getContactPoints())
            .withAuthCredentials("cassandra", "cassandra")
            .build()) {
      session.execute("select * from system.local");
    }
  }

  @SuppressWarnings("unused")
  @Test
  public void should_not_connect_with_invalid_credentials() {
    try (CqlSession session =
        SessionUtils.newSession(
            ccm,
            SessionUtils.configLoaderBuilder()
                .withString(DseDriverOption.AUTH_PROVIDER_AUTHORIZATION_ID, "")
                .withString(DefaultDriverOption.AUTH_PROVIDER_USER_NAME, "cassandra")
                .withString(DefaultDriverOption.AUTH_PROVIDER_PASSWORD, "NotARealPassword")
                .withClass(DefaultDriverOption.AUTH_PROVIDER_CLASS, PlainTextAuthProvider.class)
                .build())) {
      fail("Expected an AllNodesFailedException");
    } catch (AllNodesFailedException e) {
      verifyException(e);
    }
  }

  @SuppressWarnings("unused")
  @Test
  public void should_not_connect_without_credentials() {
    try (CqlSession session =
        SessionUtils.newSession(
            ccm,
            SessionUtils.configLoaderBuilder()
                .withString(DseDriverOption.AUTH_PROVIDER_AUTHORIZATION_ID, "")
                .withClass(DefaultDriverOption.AUTH_PROVIDER_CLASS, PlainTextAuthProvider.class)
                .build())) {
      fail("Expected AllNodesFailedException");
    } catch (AllNodesFailedException e) {
      verifyException(e);
    }
  }

  private void verifyException(AllNodesFailedException anfe) {
    assertThat(anfe.getAllErrors()).hasSize(1);
    List<Throwable> errors = anfe.getAllErrors().values().iterator().next();
    assertThat(errors).hasSize(1);
    Throwable firstError = errors.get(0);
    assertThat(firstError)
        .isInstanceOf(AuthenticationException.class)
        .hasMessageContaining("Authentication error on node /127.0.0.1:9042");
  }
}
