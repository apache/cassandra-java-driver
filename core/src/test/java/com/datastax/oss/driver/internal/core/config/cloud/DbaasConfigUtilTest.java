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
package com.datastax.oss.driver.internal.core.config.cloud;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.shaded.guava.common.io.Resources;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.Test;

public class DbaasConfigUtilTest {

  @Test
  public void should_load_config_from_json() throws Exception {

    URL url = getClass().getResource("/config/cloud/creds.zip");
    Path configFile = Paths.get(url.toURI());

    DbaasConfig config = DbaasConfigUtil.getBaseConfig(configFile);
    assertThat(config.getHost()).isEqualTo("127.0.0.1");
    assertThat(config.getUsername()).isEqualTo("driversuser");
    assertThat(config.getPassword()).isEqualTo("driverspass");
    assertThat(config.getPort()).isEqualTo(30443);
    assertThat(config.getLocalDataCenter()).isEqualTo("selfservicedc");
    assertThat(config.getKeyStorePassword()).isEqualTo("keystorepass");
    assertThat(config.getTrustStorePassword()).isEqualTo("trustpass");

    String jsonMetadata =
        Resources.toString(getClass().getResource("/config/cloud/metadata.json"), UTF_8);

    config = DbaasConfigUtil.getConfigFromMetadataJson(config, jsonMetadata);
    assertThat(config.getLocalDataCenter()).isEqualTo("dc1");
    // Test metadata parsing
    assertThat(config.getHostIds()).contains("4ac06655-f861-49f9-881e-3fee22e69b94");
    assertThat(config.getHostIds()).contains("2af7c253-3394-4a0d-bfac-f1ad81b5154d");
    assertThat(config.getHostIds()).contains("b17b6e2a-3f48-4d6a-81c1-20a0a1f3192a");
    assertThat(config.getHostIds().size()).isEqualTo(3);
    assertThat(config.getSniHost()).isEqualTo("localhost");
    assertThat(config.getSniPort()).isEqualTo(30002);
  }
}
