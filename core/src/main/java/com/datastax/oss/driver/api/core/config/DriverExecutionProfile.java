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
package com.datastax.oss.driver.api.core.config;

import com.datastax.oss.driver.internal.core.config.DerivedExecutionProfile;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
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
public interface DriverExecutionProfile extends OngoingConfigOptions<DriverExecutionProfile> {

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
  List<Boolean> getBooleanList(@NonNull DriverOption option);

  @Nullable
  default List<Boolean> getBooleanList(
      @NonNull DriverOption option, @Nullable List<Boolean> defaultValue) {
    return isDefined(option) ? getBooleanList(option) : defaultValue;
  }

  int getInt(@NonNull DriverOption option);

  default int getInt(@NonNull DriverOption option, int defaultValue) {
    return isDefined(option) ? getInt(option) : defaultValue;
  }

  @NonNull
  List<Integer> getIntList(@NonNull DriverOption option);

  @Nullable
  default List<Integer> getIntList(
      @NonNull DriverOption option, @Nullable List<Integer> defaultValue) {
    return isDefined(option) ? getIntList(option) : defaultValue;
  }

  long getLong(@NonNull DriverOption option);

  default long getLong(@NonNull DriverOption option, long defaultValue) {
    return isDefined(option) ? getLong(option) : defaultValue;
  }

  @NonNull
  List<Long> getLongList(@NonNull DriverOption option);

  @Nullable
  default List<Long> getLongList(@NonNull DriverOption option, @Nullable List<Long> defaultValue) {
    return isDefined(option) ? getLongList(option) : defaultValue;
  }

  double getDouble(@NonNull DriverOption option);

  default double getDouble(@NonNull DriverOption option, double defaultValue) {
    return isDefined(option) ? getDouble(option) : defaultValue;
  }

  @NonNull
  List<Double> getDoubleList(@NonNull DriverOption option);

  @Nullable
  default List<Double> getDoubleList(
      @NonNull DriverOption option, @Nullable List<Double> defaultValue) {
    return isDefined(option) ? getDoubleList(option) : defaultValue;
  }

  @NonNull
  String getString(@NonNull DriverOption option);

  @Nullable
  default String getString(@NonNull DriverOption option, @Nullable String defaultValue) {
    return isDefined(option) ? getString(option) : defaultValue;
  }

  @NonNull
  List<String> getStringList(@NonNull DriverOption option);

  @Nullable
  default List<String> getStringList(
      @NonNull DriverOption option, @Nullable List<String> defaultValue) {
    return isDefined(option) ? getStringList(option) : defaultValue;
  }

  @NonNull
  Map<String, String> getStringMap(@NonNull DriverOption option);

  @Nullable
  default Map<String, String> getStringMap(
      @NonNull DriverOption option, @Nullable Map<String, String> defaultValue) {
    return isDefined(option) ? getStringMap(option) : defaultValue;
  }

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
  List<Long> getBytesList(DriverOption option);

  @Nullable
  default List<Long> getBytesList(DriverOption option, @Nullable List<Long> defaultValue) {
    return isDefined(option) ? getBytesList(option) : defaultValue;
  }

  @NonNull
  Duration getDuration(@NonNull DriverOption option);

  @Nullable
  default Duration getDuration(@NonNull DriverOption option, @Nullable Duration defaultValue) {
    return isDefined(option) ? getDuration(option) : defaultValue;
  }

  @NonNull
  List<Duration> getDurationList(@NonNull DriverOption option);

  @Nullable
  default List<Duration> getDurationList(
      @NonNull DriverOption option, @Nullable List<Duration> defaultValue) {
    return isDefined(option) ? getDurationList(option) : defaultValue;
  }

  /**
   * Returns a representation of all the child options under a given option.
   *
   * <p>This is used by the driver at initialization time, to compare profiles and determine if it
   * must create per-profile policies. For example, if two profiles have the same options in the
   * {@code basic.load-balancing-policy} section, they will share the same policy instance. But if
   * their options differ, two separate instances will be created.
   *
   * <p>The runtime return type does not matter, as long as identical sections (same options with
   * same values, regardless of order) compare as equal and have the same {@code hashCode()}. The
   * default implementation builds a map based on the entries from {@link #entrySet()}, it should be
   * good for most cases.
   */
  @NonNull
  default Object getComparisonKey(@NonNull DriverOption option) {
    // This method is only used during driver initialization, performance is not crucial
    String prefix = option.getPath();
    ImmutableMap.Builder<String, Object> childOptions = ImmutableMap.builder();
    for (Map.Entry<String, Object> entry : entrySet()) {
      if (entry.getKey().startsWith(prefix)) {
        childOptions.put(entry.getKey(), entry.getValue());
      }
    }
    return childOptions.build();
  }

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

  @NonNull
  @Override
  default DriverExecutionProfile withBoolean(@NonNull DriverOption option, boolean value) {
    return DerivedExecutionProfile.with(this, option, value);
  }

  @NonNull
  @Override
  default DriverExecutionProfile withBooleanList(
      @NonNull DriverOption option, @NonNull List<Boolean> value) {
    return DerivedExecutionProfile.with(this, option, value);
  }

  @NonNull
  @Override
  default DriverExecutionProfile withInt(@NonNull DriverOption option, int value) {
    return DerivedExecutionProfile.with(this, option, value);
  }

  @NonNull
  @Override
  default DriverExecutionProfile withIntList(
      @NonNull DriverOption option, @NonNull List<Integer> value) {
    return DerivedExecutionProfile.with(this, option, value);
  }

  @NonNull
  @Override
  default DriverExecutionProfile withLong(@NonNull DriverOption option, long value) {
    return DerivedExecutionProfile.with(this, option, value);
  }

  @NonNull
  @Override
  default DriverExecutionProfile withLongList(
      @NonNull DriverOption option, @NonNull List<Long> value) {
    return DerivedExecutionProfile.with(this, option, value);
  }

  @NonNull
  @Override
  default DriverExecutionProfile withDouble(@NonNull DriverOption option, double value) {
    return DerivedExecutionProfile.with(this, option, value);
  }

  @NonNull
  @Override
  default DriverExecutionProfile withDoubleList(
      @NonNull DriverOption option, @NonNull List<Double> value) {
    return DerivedExecutionProfile.with(this, option, value);
  }

  @NonNull
  @Override
  default DriverExecutionProfile withString(@NonNull DriverOption option, @NonNull String value) {
    return DerivedExecutionProfile.with(this, option, value);
  }

  @NonNull
  @Override
  default DriverExecutionProfile withStringList(
      @NonNull DriverOption option, @NonNull List<String> value) {
    return DerivedExecutionProfile.with(this, option, value);
  }

  @NonNull
  @Override
  default DriverExecutionProfile withStringMap(
      @NonNull DriverOption option, @NonNull Map<String, String> value) {
    return DerivedExecutionProfile.with(this, option, value);
  }

  @NonNull
  @Override
  default DriverExecutionProfile withBytes(@NonNull DriverOption option, long value) {
    return DerivedExecutionProfile.with(this, option, value);
  }

  @NonNull
  @Override
  default DriverExecutionProfile withBytesList(
      @NonNull DriverOption option, @NonNull List<Long> value) {
    return DerivedExecutionProfile.with(this, option, value);
  }

  @NonNull
  @Override
  default DriverExecutionProfile withDuration(
      @NonNull DriverOption option, @NonNull Duration value) {
    return DerivedExecutionProfile.with(this, option, value);
  }

  @NonNull
  @Override
  default DriverExecutionProfile withDurationList(
      @NonNull DriverOption option, @NonNull List<Duration> value) {
    return DerivedExecutionProfile.with(this, option, value);
  }

  @NonNull
  @Override
  default DriverExecutionProfile without(@NonNull DriverOption option) {
    return DerivedExecutionProfile.without(this, option);
  }
}
