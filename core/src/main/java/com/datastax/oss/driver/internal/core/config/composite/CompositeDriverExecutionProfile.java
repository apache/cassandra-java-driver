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
package com.datastax.oss.driver.internal.core.config.composite;

import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.config.DriverOption;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSortedSet;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.BiFunction;

public class CompositeDriverExecutionProfile implements DriverExecutionProfile {

  private final DriverConfig primaryConfig;
  private final DriverConfig fallbackConfig;
  private final String profileName;

  @Nullable private volatile DriverExecutionProfile primaryProfile;
  @Nullable private volatile DriverExecutionProfile fallbackProfile;

  public CompositeDriverExecutionProfile(
      @NonNull DriverConfig primaryConfig,
      @NonNull DriverConfig fallbackConfig,
      @NonNull String profileName) {
    this.primaryConfig = Objects.requireNonNull(primaryConfig);
    this.fallbackConfig = Objects.requireNonNull(fallbackConfig);
    this.profileName = Objects.requireNonNull(profileName);
    refreshInternal();
  }

  /**
   * Fetches the underlying profiles again from the two backing configs. This is because some config
   * implementations support adding/removing profiles at runtime.
   *
   * <p>For efficiency reasons this is only done when the user fetches the profile again from the
   * main config, not every time an option is fetched from the profile.
   */
  public CompositeDriverExecutionProfile refresh() {
    return refreshInternal();
  }

  // This method only exists to avoid calling its public, overridable variant from the constructor
  private CompositeDriverExecutionProfile refreshInternal() {
    // There's no `hasProfile()` in the public API because it didn't make sense until now. So
    // unfortunately we have to catch the exception.
    try {
      primaryProfile = primaryConfig.getProfile(profileName);
    } catch (IllegalArgumentException e) {
      primaryProfile = null;
    }
    try {
      fallbackProfile = fallbackConfig.getProfile(profileName);
    } catch (IllegalArgumentException e) {
      fallbackProfile = null;
    }

    Preconditions.checkArgument(
        primaryProfile != null || fallbackProfile != null,
        "Unknown profile '%s'. Check your configuration.",
        profileName);
    return this;
  }

  @NonNull
  @Override
  public String getName() {
    return profileName;
  }

  @Override
  public boolean isDefined(@NonNull DriverOption option) {
    DriverExecutionProfile primaryProfile = this.primaryProfile;
    if (primaryProfile != null && primaryProfile.isDefined(option)) {
      return true;
    } else {
      DriverExecutionProfile fallbackProfile = this.fallbackProfile;
      return fallbackProfile != null && fallbackProfile.isDefined(option);
    }
  }

  @Override
  public boolean getBoolean(@NonNull DriverOption option) {
    return get(option, DriverExecutionProfile::getBoolean);
  }

  @NonNull
  @Override
  public List<Boolean> getBooleanList(@NonNull DriverOption option) {
    return get(option, DriverExecutionProfile::getBooleanList);
  }

  @Override
  public int getInt(@NonNull DriverOption option) {
    return get(option, DriverExecutionProfile::getInt);
  }

  @NonNull
  @Override
  public List<Integer> getIntList(@NonNull DriverOption option) {
    return get(option, DriverExecutionProfile::getIntList);
  }

  @Override
  public long getLong(@NonNull DriverOption option) {
    return get(option, DriverExecutionProfile::getLong);
  }

  @NonNull
  @Override
  public List<Long> getLongList(@NonNull DriverOption option) {
    return get(option, DriverExecutionProfile::getLongList);
  }

  @Override
  public double getDouble(@NonNull DriverOption option) {
    return get(option, DriverExecutionProfile::getDouble);
  }

  @NonNull
  @Override
  public List<Double> getDoubleList(@NonNull DriverOption option) {
    return get(option, DriverExecutionProfile::getDoubleList);
  }

  @NonNull
  @Override
  public String getString(@NonNull DriverOption option) {
    return get(option, DriverExecutionProfile::getString);
  }

  @NonNull
  @Override
  public List<String> getStringList(@NonNull DriverOption option) {
    return get(option, DriverExecutionProfile::getStringList);
  }

  @NonNull
  @Override
  public Map<String, String> getStringMap(@NonNull DriverOption option) {
    return get(option, DriverExecutionProfile::getStringMap);
  }

  @Override
  public long getBytes(@NonNull DriverOption option) {
    return get(option, DriverExecutionProfile::getBytes);
  }

  @NonNull
  @Override
  public List<Long> getBytesList(DriverOption option) {
    return get(option, DriverExecutionProfile::getBytesList);
  }

  @NonNull
  @Override
  public Duration getDuration(@NonNull DriverOption option) {
    return get(option, DriverExecutionProfile::getDuration);
  }

  @NonNull
  @Override
  public List<Duration> getDurationList(@NonNull DriverOption option) {
    return get(option, DriverExecutionProfile::getDurationList);
  }

  private <ValueT> ValueT get(
      @NonNull DriverOption option,
      BiFunction<DriverExecutionProfile, DriverOption, ValueT> getter) {
    DriverExecutionProfile primaryProfile = this.primaryProfile;
    if (primaryProfile != null && primaryProfile.isDefined(option)) {
      return getter.apply(primaryProfile, option);
    } else {
      DriverExecutionProfile fallbackProfile = this.fallbackProfile;
      if (fallbackProfile != null && fallbackProfile.isDefined(option)) {
        return getter.apply(fallbackProfile, option);
      } else {
        throw new IllegalArgumentException("Unknown option: " + option);
      }
    }
  }

  @NonNull
  @Override
  public SortedSet<Map.Entry<String, Object>> entrySet() {
    DriverExecutionProfile primaryProfile = this.primaryProfile;
    DriverExecutionProfile fallbackProfile = this.fallbackProfile;
    if (primaryProfile != null && fallbackProfile != null) {
      SortedSet<Map.Entry<String, Object>> result = new TreeSet<>(Map.Entry.comparingByKey());
      result.addAll(fallbackProfile.entrySet());
      result.addAll(primaryProfile.entrySet());
      return ImmutableSortedSet.copyOf(Map.Entry.comparingByKey(), result);
    } else if (primaryProfile != null) {
      return primaryProfile.entrySet();
    } else {
      assert fallbackProfile != null;
      return fallbackProfile.entrySet();
    }
  }
}
