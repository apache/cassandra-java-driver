/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.driver.core;

import org.testng.SkipException;
import org.testng.annotations.Test;

public class ExtendedPeerCheckDisabledTest {

  /**
   * Validates that if the com.datastax.driver.EXTENDED_PEER_CHECK system property is set to false
   * that a peer with null values for host_id, data_center, rack, tokens is not ignored.
   *
   * @test_category host:metadata
   * @jira_ticket JAVA-852
   * @since 2.1.10
   */
  @Test(
      groups = "isolated",
      dataProvider = "disallowedNullColumnsInPeerData",
      dataProviderClass = ControlConnectionTest.class)
  @CCMConfig(createCcm = false)
  public void should_use_peer_if_extended_peer_check_is_disabled(
      String columns, boolean withPeersV2, boolean requiresExtendedPeerCheck) {
    System.setProperty("com.datastax.driver.EXTENDED_PEER_CHECK", "false");
    if (!requiresExtendedPeerCheck) {
      throw new SkipException("Absence of column does not require extended peer check, skipping");
    }
    ControlConnectionTest.run_with_null_peer_info(columns, true, withPeersV2);
  }
}
