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
package com.datastax.oss.driver.api.core.config;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;

/**
 * A profile in the driver's configuration.
 *
 * <p>It is a collection of typed options.
 *
 * <p>Getters (such as {@link #getBoolean(DriverOption)}) are self-explanatory.
 *
 * <p>{@code withXxx} methods (such as {@link #withBoolean(DriverOption, boolean)}) create a
 * "derived" profile, which is an on-the-fly <b>copy</b> of the profile with the new value (which
 * might be a new option, or overwrite an existing one). If the original configuration is reloaded,
 * all derived profiles get updated as well. For best performance, such derived profiles should be
 * used sparingly; it is better to have built-in profiles for common scenarios.
 *
 * @see DriverConfig
 */
public interface DriverConfigProfile {

  /**
   * The name of the default profile (the string {@value}).
   *
   * <p>Named profiles can't use this name. If you try to declare such a profile, a runtime error
   * will be thrown.
   */
  String DEFAULT_NAME = "default";

  /**
   * The name of the profile in the configuration.
   *
   * <p>Derived profiles inherit the name of their parent.
   */
  String getName();

  boolean isDefined(DriverOption option);

  boolean getBoolean(DriverOption option);

  DriverConfigProfile withBoolean(DriverOption option, boolean value);

  int getInt(DriverOption option);

  DriverConfigProfile withInt(DriverOption option, int value);

  String getString(DriverOption option);

  DriverConfigProfile withString(DriverOption option, String value);

  List<String> getStringList(DriverOption option);

  DriverConfigProfile withStringList(DriverOption option, List<String> value);

  Map<String, String> getStringMap(DriverOption option);

  DriverConfigProfile withStringMap(DriverOption option, Map<String, String> value);

  /** Returns a size in bytes. */
  long getBytes(DriverOption option);

  DriverConfigProfile withBytes(DriverOption option, long value);

  Duration getDuration(DriverOption option);

  DriverConfigProfile withDuration(DriverOption option, Duration value);

  /**
   * Returns a representation of all the child options under a given option.
   *
   * <p>This is only used to compare configuration sections across profiles, so the actual
   * implementation does not matter, as long as identical sections (same options with same values,
   * regardless of order) compare as equal and have the same {@code hashCode()}.
   */
  Object getComparisonKey(DriverOption option);

  /**
   * Enumerates all the entries in this profile, including those that were inherited from another
   * profile.
   *
   * <p>The keys are raw strings that match {@link DriverOption#getPath()}.
   *
   * <p>The values are implementation-dependent. With the driver's default implementation, the
   * possible types are {@code String}, {@code Number}, {@code Boolean}, {@code Map<String,Object>},
   * {@code List<Object>}, or {@code null}.
   */
  SortedSet<Map.Entry<String, Object>> entrySet();
}
