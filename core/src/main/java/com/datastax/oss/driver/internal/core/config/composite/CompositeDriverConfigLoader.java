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
package com.datastax.oss.driver.internal.core.config.composite;

import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class CompositeDriverConfigLoader implements DriverConfigLoader {

  private final DriverConfigLoader primaryConfigLoader;
  private final DriverConfigLoader fallbackConfigLoader;

  public CompositeDriverConfigLoader(
      @NonNull DriverConfigLoader primaryConfigLoader,
      @NonNull DriverConfigLoader fallbackConfigLoader) {
    this.primaryConfigLoader = Objects.requireNonNull(primaryConfigLoader);
    this.fallbackConfigLoader = Objects.requireNonNull(fallbackConfigLoader);
  }

  @NonNull
  @Override
  public DriverConfig getInitialConfig() {
    DriverConfig primaryConfig = primaryConfigLoader.getInitialConfig();
    DriverConfig fallbackConfig = fallbackConfigLoader.getInitialConfig();
    return new CompositeDriverConfig(primaryConfig, fallbackConfig);
  }

  @Override
  public void onDriverInit(@NonNull DriverContext context) {
    fallbackConfigLoader.onDriverInit(context);
    primaryConfigLoader.onDriverInit(context);
  }

  @NonNull
  @Override
  public CompletionStage<Boolean> reload() {
    if (!primaryConfigLoader.supportsReloading() && !fallbackConfigLoader.supportsReloading()) {
      return CompletableFutures.failedFuture(
          new UnsupportedOperationException(
              "Reloading is not supported (this is a composite config, "
                  + "and neither the primary nor the fallback are reloadable)"));
    } else if (!primaryConfigLoader.supportsReloading()) {
      return fallbackConfigLoader.reload();
    } else if (!fallbackConfigLoader.supportsReloading()) {
      return primaryConfigLoader.reload();
    } else {
      CompletionStage<Boolean> primaryFuture = primaryConfigLoader.reload();
      CompletionStage<Boolean> fallbackFuture = fallbackConfigLoader.reload();
      CompletableFuture<Boolean> compositeFuture = new CompletableFuture<>();
      primaryFuture.whenComplete(
          (primaryChanged, primaryError) ->
              fallbackFuture.whenComplete(
                  (fallbackChanged, fallbackError) -> {
                    if (primaryError == null && fallbackError == null) {
                      compositeFuture.complete(primaryChanged || fallbackChanged);
                    } else if (fallbackError == null) {
                      compositeFuture.completeExceptionally(primaryError);
                    } else if (primaryError == null) {
                      compositeFuture.completeExceptionally(fallbackError);
                    } else {
                      primaryError.addSuppressed(fallbackError);
                      compositeFuture.completeExceptionally(primaryError);
                    }
                  }));
      return compositeFuture;
    }
  }

  @Override
  public boolean supportsReloading() {
    return primaryConfigLoader.supportsReloading() || fallbackConfigLoader.supportsReloading();
  }

  @Override
  public void close() {
    primaryConfigLoader.close();
    fallbackConfigLoader.close();
  }
}
