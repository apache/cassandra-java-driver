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

import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.dse.driver.internal.core.auth.DseGssApiAuthProvider;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.requirement.BackendRequirementRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import org.junit.AssumptionViolatedException;
import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A testing rule that wraps the EmbeddedAds server, and ccmRule into one rule This is needed
 * because ccm needs to be aware of the kerberos server settings prior to it's initialization.
 */
public class EmbeddedAdsRule extends ExternalResource {

  private static final Logger LOG = LoggerFactory.getLogger(EmbeddedAdsRule.class);

  public CustomCcmRule ccm;
  // Realm for the KDC.
  private final String realm = "DATASTAX.COM";
  private final String address = "127.0.0.1";

  private final EmbeddedAds adsServer =
      EmbeddedAds.builder().withKerberos().withRealm(realm).withAddress(address).build();

  // Principal for DSE service ( = kerberos_options.service_principal)
  private final String servicePrincipal = "dse/" + adsServer.getHostname() + "@" + realm;

  // A non-standard principal for DSE service, to test SASL protocol names
  private final String alternateServicePrincipal =
      "alternate/" + adsServer.getHostname() + "@" + realm;

  // Principal for the default cassandra user.
  private final String userPrincipal = "cassandra@" + realm;

  // Principal for a user that doesn't exist.
  private final String unknownPrincipal = "unknown@" + realm;

  // Keytabs to use for auth.
  private static File userKeytab;
  private static File unknownKeytab;
  private static File dseKeytab;
  private static File alternateKeytab;
  private static Map<String, File> customKeytabs = new HashMap<>();

  private boolean alternate = false;

  public EmbeddedAdsRule(boolean alternate) {
    this.alternate = alternate;
  }

  public EmbeddedAdsRule() {
    this(false);
  }

  @Override
  protected void before() {
    try {
      if (adsServer.isStarted()) {
        return;
      }
      // Start ldap/kdc server.
      adsServer.start();

      // Create users and keytabs for the DSE principal and cassandra user.
      dseKeytab = adsServer.addUserAndCreateKeytab("dse", "fakePasswordForTests", servicePrincipal);
      alternateKeytab =
          adsServer.addUserAndCreateKeytab(
              "alternate", "fakePasswordForTests", alternateServicePrincipal);
      userKeytab =
          adsServer.addUserAndCreateKeytab("cassandra", "fakePasswordForTests", userPrincipal);
      unknownKeytab = adsServer.createKeytab("unknown", "fakePasswordForTests", unknownPrincipal);

      String authenticationOptions =
          ""
              + "authentication_options:\n"
              + "  enabled: true\n"
              + "  default_scheme: kerberos\n"
              + "  other_schemes:\n"
              + "    - internal";

      if (alternate) {
        ccm =
            CustomCcmRule.builder()
                .withCassandraConfiguration(
                    "authorizer", "com.datastax.bdp.cassandra.auth.DseAuthorizer")
                .withCassandraConfiguration(
                    "authenticator", "com.datastax.bdp.cassandra.auth.DseAuthenticator")
                .withDseConfiguration("authorization_options.enabled", true)
                .withDseConfiguration(authenticationOptions)
                .withDseConfiguration("kerberos_options.qop", "auth-conf")
                .withDseConfiguration(
                    "kerberos_options.keytab", getAlternateKeytab().getAbsolutePath())
                .withDseConfiguration(
                    "kerberos_options.service_principal", "alternate/_HOST@" + getRealm())
                .withJvmArgs(
                    "-Dcassandra.superuser_setup_delay_ms=0",
                    "-Djava.security.krb5.conf=" + getAdsServer().getKrb5Conf().getAbsolutePath())
                .build();
      } else {
        ccm =
            CustomCcmRule.builder()
                .withCassandraConfiguration(
                    "authorizer", "com.datastax.bdp.cassandra.auth.DseAuthorizer")
                .withCassandraConfiguration(
                    "authenticator", "com.datastax.bdp.cassandra.auth.DseAuthenticator")
                .withDseConfiguration("authorization_options.enabled", true)
                .withDseConfiguration(authenticationOptions)
                .withDseConfiguration("kerberos_options.qop", "auth")
                .withDseConfiguration("kerberos_options.keytab", getDseKeytab().getAbsolutePath())
                .withDseConfiguration(
                    "kerberos_options.service_principal", "dse/_HOST@" + getRealm())
                .withJvmArgs(
                    "-Dcassandra.superuser_setup_delay_ms=0",
                    "-Djava.security.krb5.conf=" + getAdsServer().getKrb5Conf().getAbsolutePath())
                .build();
      }
      ccm.getCcmBridge().create();
      ccm.getCcmBridge().start();

    } catch (Exception e) {
      LOG.error("Unable to start ads server ", e);
    }
  }

  @Override
  public Statement apply(Statement base, Description description) {
    if (BackendRequirementRule.meetsDescriptionRequirements(description)) {
      return super.apply(base, description);
    } else {
      // requirements not met, throw reasoning assumption to skip test
      return new Statement() {
        @Override
        public void evaluate() {
          throw new AssumptionViolatedException(
              BackendRequirementRule.buildReasonString(description));
        }
      };
    }
  }

  @Override
  protected void after() {
    adsServer.stop();
    ccm.getCcmBridge().stop();
  }

  public CqlSession newKeyTabSession(String userPrincipal, String keytabPath) {
    return SessionUtils.newSession(
        getCcm(),
        SessionUtils.configLoaderBuilder()
            .withClass(DefaultDriverOption.AUTH_PROVIDER_CLASS, DseGssApiAuthProvider.class)
            .withStringMap(
                DseDriverOption.AUTH_PROVIDER_LOGIN_CONFIGURATION,
                ImmutableMap.of(
                    "principal",
                    userPrincipal,
                    "useKeyTab",
                    "true",
                    "refreshKrb5Config",
                    "true",
                    "keyTab",
                    keytabPath))
            .build());
  }

  public CqlSession newKeyTabSession(String userPrincipal, String keytabPath, String authId) {
    return SessionUtils.newSession(
        getCcm(),
        SessionUtils.configLoaderBuilder()
            .withClass(DefaultDriverOption.AUTH_PROVIDER_CLASS, DseGssApiAuthProvider.class)
            .withStringMap(
                DseDriverOption.AUTH_PROVIDER_LOGIN_CONFIGURATION,
                ImmutableMap.of(
                    "principal",
                    userPrincipal,
                    "useKeyTab",
                    "true",
                    "refreshKrb5Config",
                    "true",
                    "keyTab",
                    keytabPath))
            .withString(DseDriverOption.AUTH_PROVIDER_AUTHORIZATION_ID, authId)
            .build());
  }

  public CqlSession newKeyTabSession() {
    return newKeyTabSession(getUserPrincipal(), getUserKeytab().getAbsolutePath());
  }

  public CqlSession newTicketSession() {
    return SessionUtils.newSession(
        getCcm(),
        SessionUtils.configLoaderBuilder()
            .withClass(DefaultDriverOption.AUTH_PROVIDER_CLASS, DseGssApiAuthProvider.class)
            .withStringMap(
                DseDriverOption.AUTH_PROVIDER_LOGIN_CONFIGURATION,
                ImmutableMap.of(
                    "principal",
                    userPrincipal,
                    "useTicketCache",
                    "true",
                    "refreshKrb5Config",
                    "true",
                    "renewTGT",
                    "true"))
            .build());
  }

  public CustomCcmRule getCcm() {
    return ccm;
  }

  public String getRealm() {
    return realm;
  }

  public String getAddress() {
    return address;
  }

  public EmbeddedAds getAdsServer() {
    return adsServer;
  }

  public String getServicePrincipal() {
    return servicePrincipal;
  }

  public String getAlternateServicePrincipal() {
    return alternateServicePrincipal;
  }

  public String getUserPrincipal() {
    return userPrincipal;
  }

  public String getUnknownPrincipal() {
    return unknownPrincipal;
  }

  public File getUserKeytab() {
    return userKeytab;
  }

  public File getUnknownKeytab() {
    return unknownKeytab;
  }

  public File getDseKeytab() {
    return dseKeytab;
  }

  public File getAlternateKeytab() {
    return alternateKeytab;
  }

  public String addUserAndCreateKeyTab(String user, String password) {
    String principal = user + "@" + realm;
    try {
      File keytabFile = adsServer.addUserAndCreateKeytab(user, password, principal);
      customKeytabs.put(principal, keytabFile);
    } catch (Exception e) {
      LOG.error("Unable to add user and create keytab for " + user + " ", e);
    }
    return principal;
  }

  public File getKeytabForPrincipal(String prinicipal) {
    return customKeytabs.get(prinicipal);
  }
}
