/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.api.core.auth;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.Executor;

public class KerberosUtils {
  /**
   * Executes the given command with KRB5_CONFIG environment variable pointing to the specialized
   * config file for the embedded KDC server.
   */
  public static void executeCommand(String command, EmbeddedAds adsServer) throws IOException {
    Map<String, String> environmentMap =
        ImmutableMap.<String, String>builder()
            .put("KRB5_CONFIG", adsServer.getKrb5Conf().getAbsolutePath())
            .build();
    CommandLine cli = CommandLine.parse(command);
    Executor executor = new DefaultExecutor();
    int retValue = executor.execute(cli, environmentMap);
    assertThat(retValue).isZero();
  }

  /**
   * Acquires a ticket into the cache with the tgt using kinit command with the given principal and
   * keytab file.
   */
  public static void acquireTicket(String principal, File keytab, EmbeddedAds adsServer)
      throws IOException {
    executeCommand(
        String.format("kinit -t %s -k %s", keytab.getAbsolutePath(), principal), adsServer);
  }

  /** Destroys all tickets in the cache with given principal. */
  public static void destroyTicket(EmbeddedAdsRule ads) throws IOException {
    executeCommand("kdestroy", ads.getAdsServer());
  }
}
