/*
 * Copyright (C) 2017-2017 DataStax Inc.
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
package com.datastax.oss.driver.internal.testinfra.cluster;

import com.datastax.oss.driver.api.core.config.CoreDriverOption;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class TestConfigLoader extends DefaultDriverConfigLoader {

  public TestConfigLoader(String... customOptions) {
    super(() -> buildConfig(customOptions), CoreDriverOption.values());
  }

  private static Config buildConfig(String... customOptions) {
    String customConfig = String.join("\n", customOptions);
    // Add additional config for overriding quiet period on netty shutdown.
    String additionalCustomConfig =
        String.join(
            "\n",
            customConfig,
            "netty.io-group.shutdown.quiet-period = 0",
            "netty.admin-group.shutdown.quiet-period = 0");
    return ConfigFactory.parseString(additionalCustomConfig)
        .withFallback(DEFAULT_CONFIG_SUPPLIER.get());
  }
}
