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
package com.datastax.oss.driver.internal.core.util;

import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.google.common.base.Preconditions;
import java.lang.reflect.Constructor;

public class Reflection {
  /**
   * Loads a class by name.
   *
   * <p>This methods tries first with the current thread's context class loader (the intent is that
   * if the driver is in a low-level loader of an application server -- e.g. bootstrap or system --
   * it can still fidn classes in the application's class loader). If it is null, it defaults to the
   * class loader that loaded the class calling this method.
   */
  public static Class<?> loadClass(String className, String source) {
    try {
      ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
      if (contextClassLoader != null) {
        return Class.forName(className, true, contextClassLoader);
      } else {
        return Class.forName(className);
      }
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException(
          String.format("Can't load class %s (specified by %s)", className, source), e);
    }
  }

  /**
   * Builds a new instance of a class given its name, expecting a public constructor that takes the
   * driver's configuration as argument.
   */
  public static <T> T buildWithConfig(
      String className, Class<T> expectedSuperType, DriverConfigProfile config, String source) {
    Class<?> clazz = loadClass(className, source);
    Preconditions.checkArgument(
        expectedSuperType.isAssignableFrom(clazz),
        "Expected class %s (specified by %s) to be a subtype of %s",
        className,
        source,
        expectedSuperType.getName());

    Constructor<?> constructor;
    try {
      constructor = clazz.getConstructor(DriverConfigProfile.class);
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          String.format(
              "Expected class %s (specified by %s) "
                  + "to have an accessible constructor with a single %s argument",
              className, source, DriverConfigProfile.class.getSimpleName()));
    }
    try {
      Object instance = constructor.newInstance(config);
      return expectedSuperType.cast(instance);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          String.format("Error instantiating class %s (specified by %s)", className, source), e);
    }
  }
}
