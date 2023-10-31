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
package com.datastax.oss.driver.internal.core.config.map;

import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.config.DriverOption;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSortedSet;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import javax.annotation.Nonnull;

/** @see MapBasedDriverConfigLoader */
public class MapBasedDriverExecutionProfile implements DriverExecutionProfile {

  private final String profileName;
  // The backing map for the current profile
  private final Map<DriverOption, Object> profile;
  // The backing map for the default profile (if the current one is not the default)
  private final Map<DriverOption, Object> defaultProfile;

  public MapBasedDriverExecutionProfile(
      Map<String, Map<DriverOption, Object>> optionsMap, String profileName) {
    this(
        profileName,
        optionsMap.get(profileName),
        profileName.equals(DriverExecutionProfile.DEFAULT_NAME)
            ? Collections.emptyMap()
            : optionsMap.get(DriverExecutionProfile.DEFAULT_NAME));
    Preconditions.checkArgument(
        optionsMap.containsKey(profileName),
        "Unknown profile '%s'. Check your configuration.",
        profileName);
  }

  public MapBasedDriverExecutionProfile(
      String profileName,
      Map<DriverOption, Object> profile,
      Map<DriverOption, Object> defaultProfile) {
    this.profileName = profileName;
    this.profile = profile;
    this.defaultProfile = defaultProfile;
  }

  @Nonnull
  @Override
  public String getName() {
    return profileName;
  }

  @Override
  public boolean isDefined(@Nonnull DriverOption option) {
    return profile.containsKey(option) || defaultProfile.containsKey(option);
  }

  // Driver options don't encode the type, everything relies on the user putting the right types in
  // the backing map, so no point in trying to type-check.
  @SuppressWarnings({"unchecked", "TypeParameterUnusedInFormals"})
  @Nonnull
  private <T> T get(@Nonnull DriverOption option) {
    Object value = profile.getOrDefault(option, defaultProfile.get(option));
    if (value == null) {
      throw new IllegalArgumentException("Missing configuration option " + option.getPath());
    }
    return (T) value;
  }

  @Override
  public boolean getBoolean(@Nonnull DriverOption option) {
    return get(option);
  }

  @Nonnull
  @Override
  public List<Boolean> getBooleanList(@Nonnull DriverOption option) {
    return get(option);
  }

  @Override
  public int getInt(@Nonnull DriverOption option) {
    return get(option);
  }

  @Nonnull
  @Override
  public List<Integer> getIntList(@Nonnull DriverOption option) {
    return get(option);
  }

  @Override
  public long getLong(@Nonnull DriverOption option) {
    return get(option);
  }

  @Nonnull
  @Override
  public List<Long> getLongList(@Nonnull DriverOption option) {
    return get(option);
  }

  @Override
  public double getDouble(@Nonnull DriverOption option) {
    return get(option);
  }

  @Nonnull
  @Override
  public List<Double> getDoubleList(@Nonnull DriverOption option) {
    return get(option);
  }

  @Nonnull
  @Override
  public String getString(@Nonnull DriverOption option) {
    return get(option);
  }

  @Nonnull
  @Override
  public List<String> getStringList(@Nonnull DriverOption option) {
    return get(option);
  }

  @Nonnull
  @Override
  public Map<String, String> getStringMap(@Nonnull DriverOption option) {
    return get(option);
  }

  @Override
  public long getBytes(@Nonnull DriverOption option) {
    return get(option);
  }

  @Nonnull
  @Override
  public List<Long> getBytesList(DriverOption option) {
    return get(option);
  }

  @Nonnull
  @Override
  public Duration getDuration(@Nonnull DriverOption option) {
    return get(option);
  }

  @Nonnull
  @Override
  public List<Duration> getDurationList(@Nonnull DriverOption option) {
    return get(option);
  }

  @Nonnull
  @Override
  public SortedSet<Map.Entry<String, Object>> entrySet() {
    ImmutableSortedSet.Builder<Map.Entry<String, Object>> builder =
        ImmutableSortedSet.orderedBy(Map.Entry.comparingByKey());
    for (Map<DriverOption, Object> backingMap :
        // builder.add() ignores duplicates, so process higher precedence backing maps first
        ImmutableList.of(profile, defaultProfile)) {
      for (Map.Entry<DriverOption, Object> entry : backingMap.entrySet()) {
        builder.add(new AbstractMap.SimpleEntry<>(entry.getKey().getPath(), entry.getValue()));
      }
    }
    return builder.build();
  }
}
