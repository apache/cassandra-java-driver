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
package com.datastax.oss.driver.internal.core.util;

import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A checker for the presence of various {@link Dependency} instances at runtime. Predicate tests
 * for Graal substitutions should NOT use this class; see {@link GraalDependencyChecker} for more
 * information.
 */
public class DefaultDependencyChecker {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultDependencyChecker.class);

  private static final ConcurrentHashMap<Dependency, Boolean> CACHE = new ConcurrentHashMap<>();

  /**
   * Return true iff we can find all classes for the dependency on the classpath, false otherwise
   *
   * @param dependency the dependency to search for
   * @return true if the dependency is available, false otherwise
   */
  public static boolean isPresent(Dependency dependency) {
    try {
      return CACHE.computeIfAbsent(
          dependency,
          (dep) -> {
            for (String classNameToTest : dependency.classes()) {
              // Always use the driver class loader, assuming that the driver classes and
              // the dependency classes are either being loaded by the same class loader,
              // or – as in OSGi deployments – by two distinct, but compatible class loaders.
              if (Reflection.loadClass(null, classNameToTest) == null) {
                return false;
              }
            }
            return true;
          });
    } catch (Exception e) {
      LOG.warn("Unexpected exception when checking for dependency " + dependency, e);
      return false;
    }
  }
}
