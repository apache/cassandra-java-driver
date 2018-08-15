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

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.utils.CassandraVersion;
import org.testng.annotations.Test;

@CassandraVersion("4.0.0")
public class StartupOptionTest extends CCMTestsSupport {

  /**
   * Ensures that when connecting, the driver STARTUP message contains DRIVER_NAME and
   * DRIVER_VERSION configuration in its option map. This should be reflected in the
   * system_views.clients table.
   */
  @Test(groups = "short")
  public void should_send_driver_name_and_version() {
    ResultSet result =
        session().execute("select driver_name, driver_version from system_views.clients");

    // Should be at least 2 connections (1 control connection, 1 pooled connection)
    assertThat(result.getAvailableWithoutFetching()).isGreaterThanOrEqualTo(2);

    for (Row row : result) {
      assertThat(row.getString("driver_version")).isEqualTo(Cluster.getDriverVersion());
      assertThat(row.getString("driver_name")).isEqualTo("DataStax Java Driver");
    }
  }
}
