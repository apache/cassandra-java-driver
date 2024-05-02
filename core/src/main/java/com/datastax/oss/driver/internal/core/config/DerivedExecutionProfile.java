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
import edu.umd.cs.findbugs.annotations.NonNull;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.function.BiFunction;

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

  @NonNull
  @Override
  public String getName() {
    return baseProfile.getName();
  }

  @Override
  public boolean isDefined(@NonNull DriverOption option) {
    if (overrides.containsKey(option)) {
      return overrides.get(option) != NO_VALUE;
    } else {
      return baseProfile.isDefined(option);
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

  @NonNull
  @SuppressWarnings("unchecked")
  private <ValueT> ValueT get(
      @NonNull DriverOption option,
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

  @NonNull
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
