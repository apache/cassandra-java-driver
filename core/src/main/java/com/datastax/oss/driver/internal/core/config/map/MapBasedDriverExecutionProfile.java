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
package com.datastax.oss.driver.internal.core.config.map;

import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.config.DriverOption;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSortedSet;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;

/** @see MapBasedDriverConfigLoader */
public class MapBasedDriverExecutionProfile implements DriverExecutionProfile {

  private static final Object NO_VALUE = new Object();

  private final String profileName;
  // Anything that was overridden in a derived profile with `withXxx` methods. Empty for non-derived
  // profiles
  private final Map<DriverOption, Object> overrides;
  // The backing map for the current profile
  private final Map<DriverOption, Object> profile;
  // The backing map for the default profile (if the current one is not the default)
  private final Map<DriverOption, Object> defaultProfile;

  public MapBasedDriverExecutionProfile(
      Map<String, Map<DriverOption, Object>> optionsMap, String profileName) {
    this(
        profileName,
        Collections.emptyMap(),
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
      Map<DriverOption, Object> overrides,
      Map<DriverOption, Object> profile,
      Map<DriverOption, Object> defaultProfile) {
    this.profileName = profileName;
    this.overrides = overrides;
    this.profile = profile;
    this.defaultProfile = defaultProfile;
  }

  @NonNull
  @Override
  public String getName() {
    return profileName;
  }

  @Override
  public boolean isDefined(@NonNull DriverOption option) {
    if (overrides.containsKey(option)) {
      return overrides.get(option) != NO_VALUE;
    } else {
      return profile.containsKey(option) || defaultProfile.containsKey(option);
    }
  }

  // Driver options don't encode the type, everything relies on the user putting the right types in
  // the backing map, so no point in trying to type-check.
  @SuppressWarnings({"unchecked", "TypeParameterUnusedInFormals"})
  @NonNull
  private <T> T get(@NonNull DriverOption option) {
    Object value =
        overrides.getOrDefault(option, profile.getOrDefault(option, defaultProfile.get(option)));
    if (value == null || value == NO_VALUE) {
      throw new IllegalArgumentException("Missing configuration option " + option.getPath());
    }
    return (T) value;
  }

  @Override
  public boolean getBoolean(@NonNull DriverOption option) {
    return get(option);
  }

  @NonNull
  @Override
  public List<Boolean> getBooleanList(@NonNull DriverOption option) {
    return get(option);
  }

  @Override
  public int getInt(@NonNull DriverOption option) {
    return get(option);
  }

  @NonNull
  @Override
  public List<Integer> getIntList(@NonNull DriverOption option) {
    return get(option);
  }

  @Override
  public long getLong(@NonNull DriverOption option) {
    return get(option);
  }

  @NonNull
  @Override
  public List<Long> getLongList(@NonNull DriverOption option) {
    return get(option);
  }

  @Override
  public double getDouble(@NonNull DriverOption option) {
    return get(option);
  }

  @NonNull
  @Override
  public List<Double> getDoubleList(@NonNull DriverOption option) {
    return get(option);
  }

  @NonNull
  @Override
  public String getString(@NonNull DriverOption option) {
    return get(option);
  }

  @NonNull
  @Override
  public List<String> getStringList(@NonNull DriverOption option) {
    return get(option);
  }

  @NonNull
  @Override
  public Map<String, String> getStringMap(@NonNull DriverOption option) {
    return get(option);
  }

  @Override
  public long getBytes(@NonNull DriverOption option) {
    return get(option);
  }

  @NonNull
  @Override
  public List<Long> getBytesList(DriverOption option) {
    return get(option);
  }

  @NonNull
  @Override
  public Duration getDuration(@NonNull DriverOption option) {
    return get(option);
  }

  @NonNull
  @Override
  public List<Duration> getDurationList(@NonNull DriverOption option) {
    return get(option);
  }

  @NonNull
  @Override
  public Object getComparisonKey(@NonNull DriverOption option) {
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

  @NonNull
  @Override
  public SortedSet<Map.Entry<String, Object>> entrySet() {
    ImmutableSortedSet.Builder<Map.Entry<String, Object>> builder =
        ImmutableSortedSet.orderedBy(Map.Entry.comparingByKey());
    for (Map<DriverOption, Object> backingMap :
        // builder.add() ignores duplicates, so process higher precedence backing maps first
        ImmutableList.of(overrides, profile, defaultProfile)) {
      for (Map.Entry<DriverOption, Object> entry : backingMap.entrySet()) {
        if (entry.getValue() != NO_VALUE) {
          builder.add(new AbstractMap.SimpleEntry<>(entry.getKey().getPath(), entry.getValue()));
        }
      }
    }
    return builder.build();
  }

  private DriverExecutionProfile with(@NonNull DriverOption option, Object value) {
    ImmutableMap.Builder<DriverOption, Object> newOverrides = ImmutableMap.builder();
    for (Map.Entry<DriverOption, Object> override : overrides.entrySet()) {
      if (!override.getKey().equals(option)) {
        newOverrides.put(override.getKey(), override.getValue());
      }
    }
    newOverrides.put(option, value);
    return new MapBasedDriverExecutionProfile(
        profileName, newOverrides.build(), profile, defaultProfile);
  }

  @NonNull
  @Override
  public DriverExecutionProfile withBoolean(@NonNull DriverOption option, boolean value) {
    return with(option, value);
  }

  @NonNull
  @Override
  public DriverExecutionProfile withBooleanList(
      @NonNull DriverOption option, @NonNull List<Boolean> value) {
    return with(option, value);
  }

  @NonNull
  @Override
  public DriverExecutionProfile withInt(@NonNull DriverOption option, int value) {
    return with(option, value);
  }

  @NonNull
  @Override
  public DriverExecutionProfile withIntList(
      @NonNull DriverOption option, @NonNull List<Integer> value) {
    return with(option, value);
  }

  @NonNull
  @Override
  public DriverExecutionProfile withLong(@NonNull DriverOption option, long value) {
    return with(option, value);
  }

  @NonNull
  @Override
  public DriverExecutionProfile withLongList(
      @NonNull DriverOption option, @NonNull List<Long> value) {
    return with(option, value);
  }

  @NonNull
  @Override
  public DriverExecutionProfile withDouble(@NonNull DriverOption option, double value) {
    return with(option, value);
  }

  @NonNull
  @Override
  public DriverExecutionProfile withDoubleList(
      @NonNull DriverOption option, @NonNull List<Double> value) {
    return with(option, value);
  }

  @NonNull
  @Override
  public DriverExecutionProfile withString(@NonNull DriverOption option, @NonNull String value) {
    return with(option, value);
  }

  @NonNull
  @Override
  public DriverExecutionProfile withStringList(
      @NonNull DriverOption option, @NonNull List<String> value) {
    return with(option, value);
  }

  @NonNull
  @Override
  public DriverExecutionProfile withStringMap(
      @NonNull DriverOption option, @NonNull Map<String, String> value) {
    return with(option, value);
  }

  @NonNull
  @Override
  public DriverExecutionProfile withBytes(@NonNull DriverOption option, long value) {
    return with(option, value);
  }

  @NonNull
  @Override
  public DriverExecutionProfile withBytesList(
      @NonNull DriverOption option, @NonNull List<Long> value) {
    return with(option, value);
  }

  @NonNull
  @Override
  public DriverExecutionProfile withDuration(
      @NonNull DriverOption option, @NonNull Duration value) {
    return with(option, value);
  }

  @NonNull
  @Override
  public DriverExecutionProfile withDurationList(
      @NonNull DriverOption option, @NonNull List<Duration> value) {
    return with(option, value);
  }

  @NonNull
  @Override
  public DriverExecutionProfile without(@NonNull DriverOption option) {
    return with(option, NO_VALUE);
  }
}
