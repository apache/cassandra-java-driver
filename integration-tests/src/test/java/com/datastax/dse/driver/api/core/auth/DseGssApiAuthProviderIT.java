/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.api.core.auth;

import static com.datastax.dse.driver.api.core.auth.KerberosUtils.acquireTicket;
import static com.datastax.dse.driver.api.core.auth.KerberosUtils.destroyTicket;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.datastax.dse.driver.api.core.DseSession;
import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.auth.AuthenticationException;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.testinfra.DseRequirement;
import org.junit.Assume;
import org.junit.ClassRule;
import org.junit.Test;

@DseRequirement(min = "5.0", description = "Required for DseAuthenticator")
public class DseGssApiAuthProviderIT {

  @ClassRule public static EmbeddedAdsRule ads = new EmbeddedAdsRule();

  /**
   * Ensures that a Session can be established to a DSE server secured with Kerberos and that simple
   * queries can be made using a client configuration that provides a keytab file.
   */
  @Test
  public void should_authenticate_using_kerberos_with_keytab() {
    try (DseSession session = ads.newKeyTabSession()) {
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
    try (DseSession session = ads.newTicketSession()) {
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
    try (DseSession session = ads.newTicketSession()) {
      fail("Expected an AllNodesFailedException");
    } catch (AllNodesFailedException e) {
      assertThat(e.getErrors().size()).isEqualTo(1);
      for (Throwable t : e.getErrors().values()) {
        assertThat(t).isInstanceOf(AuthenticationException.class);
      }
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
    try (DseSession session =
        ads.newKeyTabSession(ads.getUnknownPrincipal(), ads.getUnknownKeytab().getAbsolutePath())) {
      fail("Expected an AllNodesFailedException");
    } catch (AllNodesFailedException e) {
      assertThat(e.getErrors().size()).isEqualTo(1);
      for (Throwable t : e.getErrors().values()) {
        assertThat(t).isInstanceOf(AuthenticationException.class);
      }
    }
  }
}
