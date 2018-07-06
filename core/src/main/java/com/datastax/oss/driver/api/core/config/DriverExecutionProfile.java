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

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
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
public interface DriverExecutionProfile {

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
  @NonNull
  String getName();

  boolean isDefined(@NonNull DriverOption option);

  boolean getBoolean(@NonNull DriverOption option);

  default boolean getBoolean(@NonNull DriverOption option, boolean defaultValue) {
    return isDefined(option) ? getBoolean(option) : defaultValue;
  }

  @NonNull
  DriverExecutionProfile withBoolean(@NonNull DriverOption option, boolean value);

  @NonNull
  List<Boolean> getBooleanList(@NonNull DriverOption option);

  @Nullable
  default List<Boolean> getBooleanList(
      @NonNull DriverOption option, @Nullable List<Boolean> defaultValue) {
    return isDefined(option) ? getBooleanList(option) : defaultValue;
  }

  @NonNull
  DriverExecutionProfile withBooleanList(
      @NonNull DriverOption option, @NonNull List<Boolean> value);

  int getInt(@NonNull DriverOption option);

  default int getInt(@NonNull DriverOption option, int defaultValue) {
    return isDefined(option) ? getInt(option) : defaultValue;
  }

  @NonNull
  DriverExecutionProfile withInt(@NonNull DriverOption option, int value);

  @NonNull
  List<Integer> getIntList(@NonNull DriverOption option);

  @Nullable
  default List<Integer> getIntList(
      @NonNull DriverOption option, @Nullable List<Integer> defaultValue) {
    return isDefined(option) ? getIntList(option) : defaultValue;
  }

  @NonNull
  DriverExecutionProfile withIntList(@NonNull DriverOption option, @NonNull List<Integer> value);

  long getLong(@NonNull DriverOption option);

  default long getLong(@NonNull DriverOption option, long defaultValue) {
    return isDefined(option) ? getLong(option) : defaultValue;
  }

  DriverExecutionProfile withLong(@NonNull DriverOption option, long value);

  @NonNull
  List<Long> getLongList(@NonNull DriverOption option);

  @Nullable
  default List<Long> getLongList(@NonNull DriverOption option, @Nullable List<Long> defaultValue) {
    return isDefined(option) ? getLongList(option) : defaultValue;
  }

  @NonNull
  DriverExecutionProfile withLongList(@NonNull DriverOption option, @NonNull List<Long> value);

  double getDouble(@NonNull DriverOption option);

  default double getDouble(@NonNull DriverOption option, double defaultValue) {
    return isDefined(option) ? getDouble(option) : defaultValue;
  }

  @NonNull
  DriverExecutionProfile withDouble(@NonNull DriverOption option, double value);

  @NonNull
  List<Double> getDoubleList(@NonNull DriverOption option);

  @Nullable
  default List<Double> getDoubleList(
      @NonNull DriverOption option, @Nullable List<Double> defaultValue) {
    return isDefined(option) ? getDoubleList(option) : defaultValue;
  }

  @NonNull
  DriverExecutionProfile withDoubleList(@NonNull DriverOption option, @NonNull List<Double> value);

  @NonNull
  String getString(@NonNull DriverOption option);

  @Nullable
  default String getString(@NonNull DriverOption option, @Nullable String defaultValue) {
    return isDefined(option) ? getString(option) : defaultValue;
  }

  @NonNull
  DriverExecutionProfile withString(@NonNull DriverOption option, @NonNull String value);

  @NonNull
  List<String> getStringList(@NonNull DriverOption option);

  @Nullable
  default List<String> getStringList(
      @NonNull DriverOption option, @Nullable List<String> defaultValue) {
    return isDefined(option) ? getStringList(option) : defaultValue;
  }

  @NonNull
  DriverExecutionProfile withStringList(@NonNull DriverOption option, @NonNull List<String> value);

  @NonNull
  Map<String, String> getStringMap(@NonNull DriverOption option);

  @Nullable
  default Map<String, String> getStringMap(
      @NonNull DriverOption option, @Nullable Map<String, String> defaultValue) {
    return isDefined(option) ? getStringMap(option) : defaultValue;
  }

  @NonNull
  DriverExecutionProfile withStringMap(
      @NonNull DriverOption option, @NonNull Map<String, String> value);

  /**
   * @return a size in bytes. This is separate from {@link #getLong(DriverOption)}, in case
   *     implementations want to allow users to provide sizes in a more human-readable way, for
   *     example "256 MB".
   */
  long getBytes(@NonNull DriverOption option);

  default long getBytes(@NonNull DriverOption option, long defaultValue) {
    return isDefined(option) ? getBytes(option) : defaultValue;
  }

  /** @see #getBytes(DriverOption) */
  @NonNull
  DriverExecutionProfile withBytes(@NonNull DriverOption option, long value);

  /** @see #getBytes(DriverOption) */
  @NonNull
  List<Long> getBytesList(DriverOption option);

  @Nullable
  default List<Long> getBytesList(DriverOption option, @Nullable List<Long> defaultValue) {
    return isDefined(option) ? getBytesList(option) : defaultValue;
  }

  /** @see #getBytes(DriverOption) */
  @NonNull
  DriverExecutionProfile withBytesList(@NonNull DriverOption option, @NonNull List<Long> value);

  @NonNull
  Duration getDuration(@NonNull DriverOption option);

  @Nullable
  default Duration getDuration(@NonNull DriverOption option, @Nullable Duration defaultValue) {
    return isDefined(option) ? getDuration(option) : defaultValue;
  }

  @NonNull
  DriverExecutionProfile withDuration(@NonNull DriverOption option, @NonNull Duration value);

  @NonNull
  List<Duration> getDurationList(@NonNull DriverOption option);

  @Nullable
  default List<Duration> getDurationList(
      @NonNull DriverOption option, @Nullable List<Duration> defaultValue) {
    return isDefined(option) ? getDurationList(option) : defaultValue;
  }

  @NonNull
  DriverExecutionProfile withDurationList(
      @NonNull DriverOption option, @NonNull List<Duration> value);

  /** Unsets an option. */
  @NonNull
  DriverExecutionProfile without(@NonNull DriverOption option);

  /**
   * Returns a representation of all the child options under a given option.
   *
   * <p>This is only used to compare configuration sections across profiles, so the actual
   * implementation does not matter, as long as identical sections (same options with same values,
   * regardless of order) compare as equal and have the same {@code hashCode()}.
   */
  @NonNull
  Object getComparisonKey(@NonNull DriverOption option);

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
  @NonNull
  SortedSet<Map.Entry<String, Object>> entrySet();
}
