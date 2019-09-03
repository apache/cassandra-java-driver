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

import static com.google.common.base.Charsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.google.common.io.Resources;
import java.io.File;
import org.testng.annotations.Test;

public class DbaasConfigUtilTest {

  @Test(groups = "unit")
  public void should_load_config_from_zip() {
    should_fetch_dbaas_config(new File(getClass().getResource("/dbaas/creds.zip").getPath()));
  }

  private void should_fetch_dbaas_config(File fileToLoad) {
    try {
      // Test yaml loading
      DbaasConfiguration dbaasConfig = DbaasConfigUtil.getBaseConfig(fileToLoad);
      assertThat(dbaasConfig.getHost()).isEqualTo("127.0.0.1");
      assertThat(dbaasConfig.getUsername()).isEqualTo("driversuser");
      assertThat(dbaasConfig.getPassword()).isEqualTo("driverspass");
      assertThat(dbaasConfig.getPort()).isEqualTo(30443);
      assertThat(dbaasConfig.getLocalDC()).isEqualTo("selfservicedc");
      assertThat(dbaasConfig.getKeyStorePassword()).isEqualTo("keystorepass");
      assertThat(dbaasConfig.getTrustStorePassword()).isEqualTo("trustpass");
      String jsonMetadata =
          Resources.toString(getClass().getResource("/dbaas/metadata.json"), UTF_8);
      dbaasConfig = DbaasConfigUtil.getConfigFromMetadataJson(dbaasConfig, jsonMetadata);
      assertThat(dbaasConfig.getLocalDC()).isEqualTo("dc1");
      // Test metadata parsing
      assertThat(dbaasConfig.getHostIds().contains("4ac06655-f861-49f9-881e-3fee22e69b94"));
      assertThat(dbaasConfig.getHostIds().contains("2af7c253-3394-4a0d-bfac-f1ad81b5154d"));
      assertThat(dbaasConfig.getHostIds().contains("b17b6e2a-3f48-4d6a-81c1-20a0a1f3192a"));
      assertThat(dbaasConfig.getHostIds().size()).isEqualTo(3);
      assertThat(dbaasConfig.getSniHost()).isEqualTo("localhost");
      assertThat(dbaasConfig.getSniPort()).isEqualTo(30002);
    } catch (Exception e) {
      fail("Exception thrown during configuration generation that was not expected", e);
    }
  }
}
