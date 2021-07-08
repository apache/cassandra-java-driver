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

/**
 * A dependency checker implementation which should be safe to use for build-time checks when
 * building Graal native images. This class is similar to {@link DefaultDependencyChecker} but
 * doesn't introduce any external dependencies which might complicate the native image build
 * process. Expectation is that this will be most prominently used in the various predicate classes
 * which determine whether or not Graal substitutions should be used.
 */
public class GraalDependencyChecker {

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
              // Note that this lands in a pretty similar spot to
              // Reflection.loadClass() with a null class loader
              // arg.  Major difference here is that we avoid the
              // more complex exception handling/logging ops in
              // that code.
              try {
                Class.forName(classNameToTest);
              } catch (LinkageError | Exception e) {
                return false;
              }
            }
            return true;
          });
    } catch (Exception e) {
      return false;
    }
  }
}
