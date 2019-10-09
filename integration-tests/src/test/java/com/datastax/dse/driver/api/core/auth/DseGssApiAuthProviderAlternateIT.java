/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.api.core.auth;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.dse.driver.api.core.DseSession;
import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.dse.driver.internal.core.auth.DseGssApiAuthProvider;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.testinfra.DseRequirement;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import org.junit.ClassRule;
import org.junit.Test;

@DseRequirement(min = "5.0", description = "Required for DseAuthenticator")
public class DseGssApiAuthProviderAlternateIT {
  @ClassRule public static EmbeddedAdsRule ads = new EmbeddedAdsRule(true);

  @Test
  public void
      should_authenticate_using_kerberos_with_keytab_and_alternate_service_principal_using_system_property() {
    System.setProperty("dse.sasl.service", "alternate");
    try (DseSession session =
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
      System.clearProperty("dse.sasl.service");
    }
  }

  @Test
  public void should_authenticate_using_kerberos_with_keytab_and_alternate_service_principal() {
    try (DseSession session =
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
