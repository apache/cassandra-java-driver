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
package com.datastax.oss.driver.internal.core.config;

import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.config.DriverOption;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSortedSet;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.function.BiFunction;
import javax.annotation.Nonnull;

public class DerivedExecutionProfile implements DriverExecutionProfile {

  private static final Object NO_VALUE = new Object();

  public static DerivedExecutionProfile with(
      DriverExecutionProfile baseProfile, DriverOption option, Object value) {
    if (baseProfile instanceof DerivedExecutionProfile) {
      // Don't nest derived profiles, use same base and add to overrides
      DerivedExecutionProfile previousDerived = (DerivedExecutionProfile) baseProfile;
      ImmutableMap.Builder<DriverOption, Object> newOverrides = ImmutableMap.builder();
      for (Map.Entry<DriverOption, Object> override : previousDerived.overrides.entrySet()) {
        if (!override.getKey().equals(option)) {
          newOverrides.put(override.getKey(), override.getValue());
        }
      }
      newOverrides.put(option, value);
      return new DerivedExecutionProfile(previousDerived.baseProfile, newOverrides.build());
    } else {
      return new DerivedExecutionProfile(baseProfile, ImmutableMap.of(option, value));
    }
  }

  public static DerivedExecutionProfile without(
      DriverExecutionProfile baseProfile, DriverOption option) {
    return with(baseProfile, option, NO_VALUE);
  }

  private final DriverExecutionProfile baseProfile;
  private final Map<DriverOption, Object> overrides;

  public DerivedExecutionProfile(
      DriverExecutionProfile baseProfile, Map<DriverOption, Object> overrides) {
    this.baseProfile = baseProfile;
    this.overrides = overrides;
  }

  @Nonnull
  @Override
  public String getName() {
    return baseProfile.getName();
  }

  @Override
  public boolean isDefined(@Nonnull DriverOption option) {
    if (overrides.containsKey(option)) {
      return overrides.get(option) != NO_VALUE;
    } else {
      return baseProfile.isDefined(option);
    }
  }

  @Override
  public boolean getBoolean(@Nonnull DriverOption option) {
    return get(option, DriverExecutionProfile::getBoolean);
  }

  @Nonnull
  @Override
  public List<Boolean> getBooleanList(@Nonnull DriverOption option) {
    return get(option, DriverExecutionProfile::getBooleanList);
  }

  @Override
  public int getInt(@Nonnull DriverOption option) {
    return get(option, DriverExecutionProfile::getInt);
  }

  @Nonnull
  @Override
  public List<Integer> getIntList(@Nonnull DriverOption option) {
    return get(option, DriverExecutionProfile::getIntList);
  }

  @Override
  public long getLong(@Nonnull DriverOption option) {
    return get(option, DriverExecutionProfile::getLong);
  }

  @Nonnull
  @Override
  public List<Long> getLongList(@Nonnull DriverOption option) {
    return get(option, DriverExecutionProfile::getLongList);
  }

  @Override
  public double getDouble(@Nonnull DriverOption option) {
    return get(option, DriverExecutionProfile::getDouble);
  }

  @Nonnull
  @Override
  public List<Double> getDoubleList(@Nonnull DriverOption option) {
    return get(option, DriverExecutionProfile::getDoubleList);
  }

  @Nonnull
  @Override
  public String getString(@Nonnull DriverOption option) {
    return get(option, DriverExecutionProfile::getString);
  }

  @Nonnull
  @Override
  public List<String> getStringList(@Nonnull DriverOption option) {
    return get(option, DriverExecutionProfile::getStringList);
  }

  @Nonnull
  @Override
  public Map<String, String> getStringMap(@Nonnull DriverOption option) {
    return get(option, DriverExecutionProfile::getStringMap);
  }

  @Override
  public long getBytes(@Nonnull DriverOption option) {
    return get(option, DriverExecutionProfile::getBytes);
  }

  @Nonnull
  @Override
  public List<Long> getBytesList(DriverOption option) {
    return get(option, DriverExecutionProfile::getBytesList);
  }

  @Nonnull
  @Override
  public Duration getDuration(@Nonnull DriverOption option) {
    return get(option, DriverExecutionProfile::getDuration);
  }

  @Nonnull
  @Override
  public List<Duration> getDurationList(@Nonnull DriverOption option) {
    return get(option, DriverExecutionProfile::getDurationList);
  }

  @Nonnull
  @SuppressWarnings("unchecked")
  private <ValueT> ValueT get(
      @Nonnull DriverOption option,
      BiFunction<DriverExecutionProfile, DriverOption, ValueT> getter) {
    Object value = overrides.get(option);
    if (value == null) {
      value = getter.apply(baseProfile, option);
    }
    if (value == null || value == NO_VALUE) {
      throw new IllegalArgumentException("Missing configuration option " + option.getPath());
    }
    return (ValueT) value;
  }

  @Nonnull
  @Override
  public SortedSet<Map.Entry<String, Object>> entrySet() {
    ImmutableSortedSet.Builder<Map.Entry<String, Object>> builder =
        ImmutableSortedSet.orderedBy(Map.Entry.comparingByKey());
    // builder.add() has no effect if the element already exists, so process the overrides first
    // since they have higher precedence
    for (Map.Entry<DriverOption, Object> entry : overrides.entrySet()) {
      if (entry.getValue() != NO_VALUE) {
        builder.add(new AbstractMap.SimpleEntry<>(entry.getKey().getPath(), entry.getValue()));
      }
    }
    builder.addAll(baseProfile.entrySet());
    return builder.build();
  }
}
