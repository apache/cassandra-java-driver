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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.oracle.svm.core.annotate.AutomaticFeature;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Optional;
import org.graalvm.nativeimage.hosted.Feature;
import org.graalvm.nativeimage.hosted.RuntimeClassInitialization;
import org.graalvm.nativeimage.hosted.RuntimeReflection;

/**
 * A Graal feature which determines which checks for well-known config keys corresponding to
 * instances to be instantiated via reflection, adding each to the current reflection config.
 */
@AutomaticFeature
final class DefaultFeature implements Feature {

  public Config buildConfig() {

    /* Copied from DefaultDriverConfigLoader, including the DEFAULT_ROOT_PATH value there as the literal
    "datastax-java-driver" below.  Can't reference the class directly here since that will introduce
    a requirement to specify it as a build-time dependency (as well as the slf4j logger it creates
    via static init */
    return ConfigFactory.defaultOverrides()
        .withFallback(ConfigFactory.defaultApplication())
        .withFallback(ConfigFactory.defaultReference(CqlSession.class.getClassLoader()))
        .resolve()
        .getConfig("datastax-java-driver");
  }

  public Optional<String> tryString(Config config, DefaultDriverOption option) {

    try {
      return Optional.of(config.getString(option.getPath()));
    } catch (ConfigException.WrongType e) {
      return Optional.empty();
    } catch (ConfigException.Missing e) {
      return Optional.empty();
    }
  }

  public Optional<Class<?>> tryLoadClass(String configVal, String... defaultPackages) {

    try {
      if (configVal.contains(".")) {
        return Optional.of(Class.forName(configVal));
      } else {
        for (String defaultPackage : defaultPackages) {
          String qualifiedClassName = defaultPackage + "." + configVal;
          try {
            return Optional.of(Class.forName(qualifiedClassName));
          } catch (ClassNotFoundException cnfe) {
          }
        }
        return Optional.empty();
      }
    } catch (LinkageError | Exception e) {
      return Optional.empty();
    }
  }

  public void tryRegister(Config config, DefaultDriverOption option, String... defaultPackages) {

    tryString(config, option)
        .flatMap(configVal -> tryLoadClass(configVal, defaultPackages))
        .ifPresent(
            (clz) -> {
              /* Mark the class as being initialized at build time */
              RuntimeClassInitialization.initializeAtBuildTime(clz);

              /* Register all the things as being available at runtime */
              RuntimeReflection.register(clz);
              for (Method m : clz.getMethods()) {
                RuntimeReflection.register(m);
              }
              for (Field f : clz.getFields()) {
                RuntimeReflection.register(f);
              }
              for (Constructor c : clz.getConstructors()) {
                RuntimeReflection.register(c);
              }
            });
  }

  @Override
  public void beforeAnalysis(Feature.BeforeAnalysisAccess access) {

    /* Make the Typesafe classes we need to do our work available at build-time */
    RuntimeClassInitialization.initializeAtBuildTime("com.typesafe.config.impl");

    /* slf4j logger class, used by the parent class of the default LBP (and probably plenty more) */
    RuntimeClassInitialization.initializeAtBuildTime("org.slf4j.LoggerFactory");

    Config config = buildConfig();
    tryRegister(
        config,
        DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS,
        "com.datastax.oss.driver.internal.core.loadbalancing",
        "com.datastax.dse.driver.internal.core.loadbalancing");
  }
}
