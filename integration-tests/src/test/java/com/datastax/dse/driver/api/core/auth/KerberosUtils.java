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
