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
package com.datastax.oss.driver.internal.core.config.map;

import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverOption;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.internal.core.config.ConfigChangeEvent;
import com.datastax.oss.driver.internal.core.context.EventBus;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

public class MapBasedDriverConfigLoader implements DriverConfigLoader, Consumer<OptionsMap> {

  @NonNull private final OptionsMap source;
  @NonNull private final Map<String, Map<DriverOption, Object>> rawMap;
  private volatile EventBus eventBus;

  public MapBasedDriverConfigLoader(
      @NonNull OptionsMap source, @NonNull Map<String, Map<DriverOption, Object>> rawMap) {
    this.source = source;
    this.rawMap = rawMap;
  }

  @NonNull
  @Override
  public DriverConfig getInitialConfig() {
    return new MapBasedDriverConfig(rawMap);
  }

  @Override
  public void onDriverInit(@NonNull DriverContext context) {
    eventBus = ((InternalDriverContext) context).getEventBus();
    source.addChangeListener(this);
  }

  @Override
  public void accept(OptionsMap map) {
    assert eventBus != null; // listener is registered after setting this field
    eventBus.fire(ConfigChangeEvent.INSTANCE);
  }

  @NonNull
  @Override
  public CompletionStage<Boolean> reload() {
    return CompletableFuture.completedFuture(true);
  }

  @Override
  public boolean supportsReloading() {
    return true;
  }

  @Override
  public void close() {
    source.removeChangeListener(this);
  }
}
