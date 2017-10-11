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
package com.datastax.oss.driver.api.core.session;

import com.datastax.oss.driver.api.core.Cluster;
import com.datastax.oss.driver.api.core.ClusterBuilder;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import java.util.List;

public class GuavaClusterBuilder extends ClusterBuilder<GuavaClusterBuilder, GuavaCluster> {

  @Override
  protected DriverContext buildContext(
      DriverConfigLoader configLoader, List<TypeCodec<?>> typeCodecs) {
    return new GuavaDriverContext(configLoader, typeCodecs);
  }

  @Override
  protected GuavaCluster wrap(Cluster<CqlSession> defaultCluster) {
    return new GuavaCluster(defaultCluster);
  }
}
