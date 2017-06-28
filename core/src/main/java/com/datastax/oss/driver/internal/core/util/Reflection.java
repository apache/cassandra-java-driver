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

import com.datastax.oss.driver.api.core.config.CoreDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.config.DriverOption;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.google.common.base.Preconditions;
import java.lang.reflect.Constructor;
import java.util.Optional;

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
   * Tries to create an instance of a class, given a set of options defined in the driver
   * configuration.
   *
   * <p>For example:
   *
   * <pre>
   * my-policy {
   *   class = my.package.MyPolicyImpl
   *   arg1 = some custom option
   * }
   * </pre>
   *
   * The {@code class} option is mandatory and will be used to construct the instance via
   * reflection. It must have a constructor that takes two arguments: the {@link DriverContext}, and
   * a {@link DriverOption} that represents the configuration root ({@code my-policy} in the example
   * above).
   *
   * @param context the driver context.
   * @param rootOption the root of the set of options that configures the class. It will be looked
   *     up in the default profile of the configuration stored in the context.
   * @param expectedSuperType a super-type that the class is expected to implement/extend.
   * @return the new instance, or empty if {@code rootOption} or the class sub-option is not defined
   *     in the configuration.
   */
  public static <T> Optional<T> buildFromConfig(
      DriverContext context, DriverOption rootOption, Class<T> expectedSuperType) {

    DriverConfigProfile config = context.config().defaultProfile();

    DriverOption classNameOption = rootOption.concat(CoreDriverOption.RELATIVE_POLICY_CLASS);
    if (!config.isDefined(classNameOption)) {
      return Optional.empty();
    }

    String className = config.getString(classNameOption);
    String configPath = classNameOption.getPath();
    Class<?> clazz = loadClass(className, configPath);
    Preconditions.checkArgument(
        expectedSuperType.isAssignableFrom(clazz),
        "Expected class %s (specified by %s) to be a subtype of %s",
        className,
        configPath,
        expectedSuperType.getName());

    Constructor<?> constructor;
    try {
      constructor = clazz.getConstructor(DriverContext.class, DriverOption.class);
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          String.format(
              "Expected class %s (specified by %s) "
                  + "to have an accessible constructor with a single %s argument",
              className, configPath, DriverConfigProfile.class.getSimpleName()));
    }
    try {
      Object instance = constructor.newInstance(context, rootOption);
      return Optional.of(expectedSuperType.cast(instance));
    } catch (Exception e) {
      throw new IllegalArgumentException(
          String.format("Error instantiating class %s (specified by %s)", className, configPath),
          e);
    }
  }
}
