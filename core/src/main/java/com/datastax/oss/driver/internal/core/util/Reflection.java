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

import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.config.DriverOption;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.shaded.guava.common.base.Joiner;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ListMultimap;
import com.datastax.oss.driver.shaded.guava.common.collect.MultimapBuilder;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Reflection {

  private static final Logger LOG = LoggerFactory.getLogger(Reflection.class);

  /**
   * Loads a class by name.
   *
   * <p>This methods tries first with the current thread's context class loader (the intent is that
   * if the driver is in a low-level loader of an application server -- e.g. bootstrap or system --
   * it can still find classes in the application's class loader). If it is null, it defaults to the
   * class loader that loaded the class calling this method.
   *
   * @return null if the class does not exist.
   */
  public static Class<?> loadClass(String className) {
    try {
      ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
      if (contextClassLoader != null) {
        return Class.forName(className, true, contextClassLoader);
      } else {
        return Class.forName(className);
      }
    } catch (ClassNotFoundException e) {
      return null;
    }
  }

  /**
   * Tries to create an instance of a class, given an option defined in the driver configuration.
   *
   * <p>For example:
   *
   * <pre>
   * my-policy.class = my.package.MyPolicyImpl
   * </pre>
   *
   * The class will be instantiated via reflection, it must have a constructor that takes a {@link
   * DriverContext} argument.
   *
   * @param context the driver context.
   * @param classNameOption the option that indicates the class. It will be looked up in the default
   *     profile of the configuration stored in the context.
   * @param expectedSuperType a super-type that the class is expected to implement/extend.
   * @param defaultPackages the default packages to prepend to the class name if it's not qualified.
   *     They will be tried in order, the first one that matches an existing class will be used.
   * @return the new instance, or empty if {@code classNameOption} is not defined in the
   *     configuration.
   */
  public static <T> Optional<T> buildFromConfig(
      DriverContext context,
      DriverOption classNameOption,
      Class<T> expectedSuperType,
      String... defaultPackages) {
    return buildFromConfig(context, null, classNameOption, expectedSuperType, defaultPackages);
  }

  /**
   * Tries to create multiple instances of a class, given options defined in the driver
   * configuration and possibly overridden in profiles.
   *
   * <p>For example:
   *
   * <pre>
   * my-policy.class = package1.PolicyImpl1
   * profiles {
   *   my-profile { my-policy.class = package2.PolicyImpl2 }
   * }
   * </pre>
   *
   * The class will be instantiated via reflection, it must have a constructor that takes two
   * arguments: the {@link DriverContext}, and a string representing the profile name.
   *
   * <p>This method assumes the policy is mandatory, the class option must be present at least for
   * the default profile.
   *
   * @param context the driver context.
   * @param rootOption the root option for the policy (my-policy in the example above). The class
   *     name is assumed to be in a 'class' child option.
   * @param expectedSuperType a super-type that the class is expected to implement/extend.
   * @param defaultPackages the default packages to prepend to the class name if it's not qualified.
   *     They will be tried in order, the first one that matches an existing class will be used.
   * @return the policy instances by profile name. If multiple profiles share the same
   *     configuration, a single instance will be shared by all their entries.
   */
  public static <T> Map<String, T> buildFromConfigProfiles(
      DriverContext context,
      DriverOption rootOption,
      Class<T> expectedSuperType,
      String... defaultPackages) {

    // Find out how many distinct configurations we have
    ListMultimap<Object, String> profilesByConfig =
        MultimapBuilder.hashKeys().arrayListValues().build();
    for (DriverConfigProfile profile : context.config().getProfiles().values()) {
      profilesByConfig.put(profile.getComparisonKey(rootOption), profile.getName());
    }

    // Instantiate each distinct configuration, and associate it with the corresponding profiles
    ImmutableMap.Builder<String, T> result = ImmutableMap.builder();
    for (Collection<String> profiles : profilesByConfig.asMap().values()) {
      // Since all profiles use the same config, we can use any of them
      String profileName = profiles.iterator().next();
      T policy =
          buildFromConfig(
                  context, profileName, classOption(rootOption), expectedSuperType, defaultPackages)
              .orElseThrow(
                  () ->
                      new IllegalArgumentException(
                          String.format(
                              "Missing configuration for %s in profile %s",
                              rootOption.getPath(), profileName)));
      for (String profile : profiles) {
        result.put(profile, policy);
      }
    }
    return result.build();
  }

  /**
   * @param profileName if null, this is a global policy, use the default profile and look for a
   *     one-arg constructor. If not null, this is a per-profile policy, look for a two-arg
   *     constructor.
   */
  public static <T> Optional<T> buildFromConfig(
      DriverContext context,
      String profileName,
      DriverOption classNameOption,
      Class<T> expectedSuperType,
      String... defaultPackages) {

    DriverConfigProfile config =
        (profileName == null)
            ? context.config().getDefaultProfile()
            : context.config().getProfile(profileName);

    String configPath = classNameOption.getPath();
    LOG.debug("Creating a {} from config option {}", expectedSuperType.getSimpleName(), configPath);

    if (!config.isDefined(classNameOption)) {
      LOG.debug("Option is not defined, skipping");
      return Optional.empty();
    }

    String className = config.getString(classNameOption);
    Class<?> clazz = null;
    if (className.contains(".")) {
      LOG.debug("Building from fully-qualified name {}", className);
      clazz = loadClass(className);
    } else {
      LOG.debug("Building from unqualified name {}", className);
      for (String defaultPackage : defaultPackages) {
        String qualifiedClassName = defaultPackage + "." + className;
        LOG.debug("Trying with default package {}", qualifiedClassName);
        clazz = loadClass(qualifiedClassName);
        if (clazz != null) {
          break;
        }
      }
    }
    if (clazz == null) {
      throw new IllegalArgumentException(
          String.format("Can't find class %s (specified by %s)", className, configPath));
    }
    Preconditions.checkArgument(
        expectedSuperType.isAssignableFrom(clazz),
        "Expected class %s (specified by %s) to be a subtype of %s",
        className,
        configPath,
        expectedSuperType.getName());

    Constructor<?> constructor;
    Class<?>[] argumentTypes =
        (profileName == null)
            ? new Class<?>[] {DriverContext.class}
            : new Class<?>[] {DriverContext.class, String.class};
    try {
      constructor = clazz.getConstructor(argumentTypes);
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          String.format(
              "Expected class %s (specified by %s) "
                  + "to have an accessible constructor with arguments (%s)",
              className, configPath, Joiner.on(',').join(argumentTypes)));
    }
    try {
      @SuppressWarnings("JavaReflectionInvocation")
      Object instance =
          (profileName == null)
              ? constructor.newInstance(context)
              : constructor.newInstance(context, profileName);
      return Optional.of(expectedSuperType.cast(instance));
    } catch (Exception e) {
      // ITE just wraps an exception thrown by the constructor, get rid of it:
      Throwable cause = (e instanceof InvocationTargetException) ? e.getCause() : e;
      throw new IllegalArgumentException(
          String.format(
              "Error instantiating class %s (specified by %s): %s",
              className, configPath, cause.getMessage()),
          cause);
    }
  }

  private static DriverOption classOption(DriverOption rootOption) {
    return new DriverOption() {
      @Override
      public String getPath() {
        return rootOption.getPath() + ".class";
      }

      @Override
      public boolean required() {
        return rootOption.required();
      }
    };
  }
}
