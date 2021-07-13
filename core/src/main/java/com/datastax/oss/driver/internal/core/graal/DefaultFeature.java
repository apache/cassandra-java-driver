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
package com.datastax.oss.driver.internal.core.graal;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader;
import com.oracle.svm.core.annotate.AutomaticFeature;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import java.util.Optional;
import org.graalvm.nativeimage.hosted.Feature;
import org.graalvm.nativeimage.hosted.RuntimeReflection;

/**
 * A Graal feature which determines which checks for well-known config keys corresponding to
 * instances to be instantiated via reflection, adding each to the current reflection config.
 */
@AutomaticFeature
final class DefaultFeature implements Feature {

  private Optional<String> tryString(Config config, DefaultDriverOption option) {

    try {
      return Optional.of(config.getString(option.getPath()));

    } catch (ConfigException.WrongType e) {
      return Optional.empty();
    } catch (ConfigException.Missing e) {
      return Optional.empty();
    }
  }

  private void registerForRuntimeReflection(Class<?> clz) {

    RuntimeReflection.register(clz);
  }

  @Override
  public void beforeAnalysis(Feature.BeforeAnalysisAccess access) {

    Config config = DefaultDriverConfigLoader.DEFAULT_CONFIG_SUPPLIER.get();
    DefaultDriverOption option = DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS;

    tryString(config, option)
        .ifPresent(
            (clzName) -> {
              try {
                registerForRuntimeReflection(Class.forName(clzName));
              } catch (Exception e) {

                // TODO: Log something here?
              }
            });
  }
}
