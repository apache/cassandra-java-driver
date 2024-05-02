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
package com.datastax.oss.driver.internal.core.util;

import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.config.DriverOption;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.shaded.guava.common.base.Joiner;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ListMultimap;
import com.datastax.oss.driver.shaded.guava.common.collect.MultimapBuilder;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Reflection {

  private static final Logger LOG = LoggerFactory.getLogger(Reflection.class);

  /**
   * Loads a class by name using the given {@link ClassLoader}.
   *
   * <p>If the class loader is null, the class will be loaded using the class loader that loaded the
   * driver.
   *
   * @return null if the class does not exist or could not be loaded.
   */
  @Nullable
  public static Class<?> loadClass(@Nullable ClassLoader classLoader, @NonNull String className) {
    try {
      Class<?> clazz;
      if (classLoader == null) {
        LOG.trace("Attempting to load {} with driver's class loader", className);
        clazz = Class.forName(className);
      } else {
        LOG.trace("Attempting to load {} with {}", className, classLoader);
        clazz = Class.forName(className, true, classLoader);
      }
      LOG.trace("Successfully loaded {}", className);
      return clazz;
    } catch (LinkageError | Exception e) {
      // Note: only ClassNotFoundException, LinkageError and SecurityException
      // are declared to be thrown; however some class loaders (Apache Felix)
      // may throw other checked exceptions, which cannot be caught directly
      // because that would cause a compilation failure.
      LOG.debug(
          String.format("Could not load %s with loader %s: %s", className, classLoader, e), e);
      if (classLoader == null) {
        return null;
      } else {
        // If the user-supplied class loader is unable to locate the class, try with the driver's
        // default class loader. This is useful in OSGi deployments where the user-supplied loader
        // may be able to load some classes but not all of them. Besides, the driver bundle, in
        // OSGi, has a "Dynamic-Import:*" directive that makes its class loader capable of locating
        // a great number of classes.
        return loadClass(null, className);
      }
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
  public static <ComponentT> Optional<ComponentT> buildFromConfig(
      InternalDriverContext context,
      DriverOption classNameOption,
      Class<ComponentT> expectedSuperType,
      String... defaultPackages) {
    return buildFromConfig(context, null, classNameOption, expectedSuperType, defaultPackages);
  }

  /**
   * Tries to create a list of instances, given an option defined in the driver configuration.
   *
   * <p>For example:
   *
   * <pre>
   * my-policy.classes = [my.package.MyPolicyImpl1,my.package.MyPolicyImpl2]
   * </pre>
   *
   * Each class will be instantiated via reflection, and must have a constructor that takes a {@link
   * DriverContext} argument.
   *
   * @param context the driver context.
   * @param classNamesOption the option that indicates the class list. It will be looked up in the
   *     default profile of the configuration stored in the context.
   * @param expectedSuperType a super-type that the classes are expected to implement/extend.
   * @param defaultPackages the default packages to prepend to the class names if they are not
   *     qualified. They will be tried in order, the first one that matches an existing class will
   *     be used.
   * @return the list of new instances, or an empty list if {@code classNamesOption} is not defined
   *     in the configuration.
   */
  public static <ComponentT> ImmutableList<ComponentT> buildFromConfigList(
      InternalDriverContext context,
      DriverOption classNamesOption,
      Class<ComponentT> expectedSuperType,
      String... defaultPackages) {
    return buildFromConfigList(context, null, classNamesOption, expectedSuperType, defaultPackages);
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
   * @param classNameOption the option that indicates the class (my-policy.class in the example
   *     above).
   * @param rootOption the root of the section containing the policy's configuration (my-policy in
   *     the example above). Profiles that have the same contents under that section will share the
   *     same policy instance.
   * @param expectedSuperType a super-type that the class is expected to implement/extend.
   * @param defaultPackages the default packages to prepend to the class name if it's not qualified.
   *     They will be tried in order, the first one that matches an existing class will be used.
   * @return the policy instances by profile name. If multiple profiles share the same
   *     configuration, a single instance will be shared by all their entries.
   */
  public static <ComponentT> Map<String, ComponentT> buildFromConfigProfiles(
      InternalDriverContext context,
      DriverOption classNameOption,
      DriverOption rootOption,
      Class<ComponentT> expectedSuperType,
      String... defaultPackages) {

    // Find out how many distinct configurations we have
    ListMultimap<Object, String> profilesByConfig =
        MultimapBuilder.hashKeys().arrayListValues().build();
    for (DriverExecutionProfile profile : context.getConfig().getProfiles().values()) {
      profilesByConfig.put(profile.getComparisonKey(rootOption), profile.getName());
    }

    // Instantiate each distinct configuration, and associate it with the corresponding profiles
    ImmutableMap.Builder<String, ComponentT> result = ImmutableMap.builder();
    for (Collection<String> profiles : profilesByConfig.asMap().values()) {
      // Since all profiles use the same config, we can use any of them
      String profileName = profiles.iterator().next();
      ComponentT policy =
          buildFromConfig(context, profileName, classNameOption, expectedSuperType, defaultPackages)
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
  public static <ComponentT> Optional<ComponentT> buildFromConfig(
      InternalDriverContext context,
      String profileName,
      DriverOption classNameOption,
      Class<ComponentT> expectedSuperType,
      String... defaultPackages) {

    DriverExecutionProfile config =
        (profileName == null)
            ? context.getConfig().getDefaultProfile()
            : context.getConfig().getProfile(profileName);

    String configPath = classNameOption.getPath();
    LOG.debug("Creating a {} from config option {}", expectedSuperType.getSimpleName(), configPath);

    if (!config.isDefined(classNameOption)) {
      LOG.debug("Option is not defined, skipping");
      return Optional.empty();
    }

    String className = config.getString(classNameOption);
    return Optional.of(
        resolveClass(
            context, profileName, expectedSuperType, configPath, className, defaultPackages));
  }

  /**
   * @param profileName if null, this is a global policy, use the default profile and look for a
   *     one-arg constructor. If not null, this is a per-profile policy, look for a two-arg
   *     constructor.
   */
  public static <ComponentT> ImmutableList<ComponentT> buildFromConfigList(
      InternalDriverContext context,
      String profileName,
      DriverOption classNamesOption,
      Class<ComponentT> expectedSuperType,
      String... defaultPackages) {

    DriverExecutionProfile config =
        (profileName == null)
            ? context.getConfig().getDefaultProfile()
            : context.getConfig().getProfile(profileName);

    String configPath = classNamesOption.getPath();
    LOG.debug(
        "Creating a list of {} from config option {}",
        expectedSuperType.getSimpleName(),
        configPath);

    if (!config.isDefined(classNamesOption)) {
      LOG.debug("Option is not defined, skipping");
      return ImmutableList.of();
    }

    List<String> classNames = config.getStringList(classNamesOption);
    ImmutableList.Builder<ComponentT> components = ImmutableList.builder();
    for (String className : classNames) {
      components.add(
          resolveClass(
              context, profileName, expectedSuperType, configPath, className, defaultPackages));
    }
    return components.build();
  }

  @NonNull
  private static <ComponentT> ComponentT resolveClass(
      InternalDriverContext context,
      String profileName,
      Class<ComponentT> expectedSuperType,
      String configPath,
      String className,
      String[] defaultPackages) {
    Class<?> clazz = null;
    if (className.contains(".")) {
      LOG.debug("Building from fully-qualified name {}", className);
      clazz = loadClass(context.getClassLoader(), className);
    } else {
      LOG.debug("Building from unqualified name {}", className);
      for (String defaultPackage : defaultPackages) {
        String qualifiedClassName = defaultPackage + "." + className;
        LOG.debug("Trying with default package {}", qualifiedClassName);
        clazz = loadClass(context.getClassLoader(), qualifiedClassName);
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

    Constructor<? extends ComponentT> constructor;
    Class<?>[] argumentTypes =
        (profileName == null)
            ? new Class<?>[] {DriverContext.class}
            : new Class<?>[] {DriverContext.class, String.class};
    try {
      constructor = clazz.asSubclass(expectedSuperType).getConstructor(argumentTypes);
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          String.format(
              "Expected class %s (specified by %s) "
                  + "to have an accessible constructor with arguments (%s)",
              className, configPath, Joiner.on(',').join(argumentTypes)));
    }
    try {
      @SuppressWarnings("JavaReflectionInvocation")
      ComponentT instance =
          (profileName == null)
              ? constructor.newInstance(context)
              : constructor.newInstance(context, profileName);
      return instance;
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
}
