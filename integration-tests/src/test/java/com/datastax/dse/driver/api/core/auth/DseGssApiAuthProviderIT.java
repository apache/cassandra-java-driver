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

import static com.datastax.dse.driver.api.core.auth.KerberosUtils.acquireTicket;
import static com.datastax.dse.driver.api.core.auth.KerberosUtils.destroyTicket;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.auth.AuthenticationException;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.testinfra.requirement.BackendRequirement;
import com.datastax.oss.driver.api.testinfra.requirement.BackendType;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.junit.Assume;
import org.junit.ClassRule;
import org.junit.Test;

@BackendRequirement(
    type = BackendType.DSE,
    minInclusive = "5.0",
    description = "Required for DseAuthenticator")
public class DseGssApiAuthProviderIT {

  @ClassRule public static EmbeddedAdsRule ads = new EmbeddedAdsRule();

  /**
   * Ensures that a Session can be established to a DSE server secured with Kerberos and that simple
   * queries can be made using a client configuration that provides a keytab file.
   */
  @Test
  public void should_authenticate_using_kerberos_with_keytab() {
    try (CqlSession session = ads.newKeyTabSession()) {
      ResultSet set = session.execute("select * from system.local");
      assertThat(set).isNotNull();
    }
  }

  /**
   * Ensures that a Session can be established to a DSE server secured with Kerberos and that simple
   * queries can be made using a client configuration that uses the ticket cache. This test will
   * only run on unix platforms since it uses kinit to acquire tickets and kdestroy to destroy them.
   */
  @Test
  public void should_authenticate_using_kerberos_with_ticket() throws Exception {
    String osName = System.getProperty("os.name", "").toLowerCase();
    boolean isUnix = osName.contains("mac") || osName.contains("darwin") || osName.contains("nux");
    Assume.assumeTrue(isUnix);
    acquireTicket(ads.getUserPrincipal(), ads.getUserKeytab(), ads.getAdsServer());
    try (CqlSession session = ads.newTicketSession()) {
      ResultSet set = session.execute("select * from system.local");
      assertThat(set).isNotNull();
    } finally {
      destroyTicket(ads);
    }
  }

  /**
   * Validates that an AllNodesFailedException is thrown when using a ticket-based configuration and
   * no such ticket exists in the user's cache. This is expected because we shouldn't be able to
   * establish connection to a cassandra node if we cannot authenticate.
   *
   * @test_category dse:authentication
   */
  @SuppressWarnings("unused")
  @Test
  public void should_not_authenticate_if_no_ticket_in_cache() {
    try (CqlSession session = ads.newTicketSession()) {
      fail("Expected an AllNodesFailedException");
    } catch (AllNodesFailedException e) {
      verifyException(e);
    }
  }

  /**
   * Validates that an AllNodesFailedException is thrown when using a keytab-based configuration and
   * no such user exists for the given principal. This is expected because we shouldn't be able to
   * establish connection to a cassandra node if we cannot authenticate.
   *
   * @test_category dse:authentication
   */
  @SuppressWarnings("unused")
  @Test
  public void should_not_authenticate_if_keytab_does_not_map_to_valid_principal() {
    try (CqlSession session =
        ads.newKeyTabSession(ads.getUnknownPrincipal(), ads.getUnknownKeytab().getAbsolutePath())) {
      fail("Expected an AllNodesFailedException");
    } catch (AllNodesFailedException e) {
      verifyException(e);
    }
  }
  /**
   * Ensures that a Session can be established to a DSE server secured with Kerberos and that simple
   * queries can be made using a client configuration that is provided via programatic interface
   */
  @Test
  public void should_authenticate_using_kerberos_with_keytab_programmatically() {
    DseGssApiAuthProviderBase.GssApiOptions.Builder builder =
        DseGssApiAuthProviderBase.GssApiOptions.builder();
    Map<String, String> loginConfig =
        ImmutableMap.of(
            "principal",
            ads.getUserPrincipal(),
            "useKeyTab",
            "true",
            "refreshKrb5Config",
            "true",
            "keyTab",
            ads.getUserKeytab().getAbsolutePath());

    builder.withLoginConfiguration(loginConfig);
    try (CqlSession session =
        CqlSession.builder()
            .withAuthProvider(new ProgrammaticDseGssApiAuthProvider(builder.build()))
            .build()) {

      ResultSet set = session.execute("select * from system.local");
      assertThat(set).isNotNull();
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
