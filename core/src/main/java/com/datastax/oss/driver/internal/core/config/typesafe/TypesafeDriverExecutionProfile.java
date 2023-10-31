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
package com.datastax.oss.driver.internal.core.config.typesafe;

import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.config.DriverOption;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSortedSet;
import com.datastax.oss.driver.shaded.guava.common.collect.MapMaker;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueFactory;
import com.typesafe.config.ConfigValueType;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public abstract class TypesafeDriverExecutionProfile implements DriverExecutionProfile {

  /** The original profile in the driver's configuration that this profile was derived from. */
  protected abstract Base getBaseProfile();

  /** The extra options that were added with {@code withXxx} methods. */
  protected abstract Config getAddedOptions();

  /** The actual options that will be used to answer {@code getXxx} calls. */
  protected abstract Config getEffectiveOptions();

  protected final ConcurrentMap<String, Object> cache = new ConcurrentHashMap<>();

  @Override
  public boolean isDefined(@Nonnull DriverOption option) {
    return getEffectiveOptions().hasPath(option.getPath());
  }

  @Override
  public boolean getBoolean(@Nonnull DriverOption option) {
    return getCached(option.getPath(), getEffectiveOptions()::getBoolean);
  }

  // We override `with*` methods because they can be implemented a bit better with Typesafe config
  @Nonnull
  @Override
  public DriverExecutionProfile withBoolean(@Nonnull DriverOption option, boolean value) {
    return with(option, value);
  }

  @Nonnull
  @Override
  public List<Boolean> getBooleanList(@Nonnull DriverOption option) {
    return getCached(option.getPath(), getEffectiveOptions()::getBooleanList);
  }

  @Nonnull
  @Override
  public DriverExecutionProfile withBooleanList(
      @Nonnull DriverOption option, @Nonnull List<Boolean> value) {
    return with(option, value);
  }

  @Override
  public int getInt(@Nonnull DriverOption option) {
    return getCached(option.getPath(), getEffectiveOptions()::getInt);
  }

  @Nonnull
  @Override
  public DriverExecutionProfile withInt(@Nonnull DriverOption option, int value) {
    return with(option, value);
  }

  @Nonnull
  @Override
  public List<Integer> getIntList(@Nonnull DriverOption option) {
    return getCached(option.getPath(), getEffectiveOptions()::getIntList);
  }

  @Nonnull
  @Override
  public DriverExecutionProfile withIntList(
      @Nonnull DriverOption option, @Nonnull List<Integer> value) {
    return with(option, value);
  }

  @Override
  public long getLong(@Nonnull DriverOption option) {
    return getCached(option.getPath(), getEffectiveOptions()::getLong);
  }

  @Nonnull
  @Override
  public DriverExecutionProfile withLong(@Nonnull DriverOption option, long value) {
    return with(option, value);
  }

  @Nonnull
  @Override
  public List<Long> getLongList(@Nonnull DriverOption option) {
    return getCached(option.getPath(), getEffectiveOptions()::getLongList);
  }

  @Nonnull
  @Override
  public DriverExecutionProfile withLongList(
      @Nonnull DriverOption option, @Nonnull List<Long> value) {
    return with(option, value);
  }

  @Override
  public double getDouble(@Nonnull DriverOption option) {
    return getCached(option.getPath(), getEffectiveOptions()::getDouble);
  }

  @Nonnull
  @Override
  public DriverExecutionProfile withDouble(@Nonnull DriverOption option, double value) {
    return with(option, value);
  }

  @Nonnull
  @Override
  public List<Double> getDoubleList(@Nonnull DriverOption option) {
    return getCached(option.getPath(), getEffectiveOptions()::getDoubleList);
  }

  @Nonnull
  @Override
  public DriverExecutionProfile withDoubleList(
      @Nonnull DriverOption option, @Nonnull List<Double> value) {
    return with(option, value);
  }

  @Nonnull
  @Override
  public String getString(@Nonnull DriverOption option) {
    return getCached(option.getPath(), getEffectiveOptions()::getString);
  }

  @Nonnull
  @Override
  public DriverExecutionProfile withString(@Nonnull DriverOption option, @Nonnull String value) {
    return with(option, value);
  }

  @Nonnull
  @Override
  public List<String> getStringList(@Nonnull DriverOption option) {
    return getCached(option.getPath(), getEffectiveOptions()::getStringList);
  }

  @Nonnull
  @Override
  public DriverExecutionProfile withStringList(
      @Nonnull DriverOption option, @Nonnull List<String> value) {
    return with(option, value);
  }

  @Nonnull
  @Override
  public Map<String, String> getStringMap(@Nonnull DriverOption option) {
    Config subConfig = getCached(option.getPath(), getEffectiveOptions()::getConfig);
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    for (Map.Entry<String, ConfigValue> entry : subConfig.entrySet()) {
      if (entry.getValue().valueType().equals(ConfigValueType.STRING)) {
        builder.put(entry.getKey(), (String) entry.getValue().unwrapped());
      }
    }
    return builder.build();
  }

  @Nonnull
  @Override
  public DriverExecutionProfile withStringMap(
      @Nonnull DriverOption option, @Nonnull Map<String, String> map) {
    Base base = getBaseProfile();
    // Add the new option to any already derived options
    Config newAdded = getAddedOptions();
    for (String key : map.keySet()) {
      newAdded =
          newAdded.withValue(
              option.getPath() + "." + key, ConfigValueFactory.fromAnyRef(map.get(key)));
    }
    Derived derived = new Derived(base, newAdded);
    base.register(derived);
    return derived;
  }

  @Override
  public long getBytes(@Nonnull DriverOption option) {
    return getCached(option.getPath(), getEffectiveOptions()::getBytes);
  }

  @Nonnull
  @Override
  public DriverExecutionProfile withBytes(@Nonnull DriverOption option, long value) {
    return with(option, value);
  }

  @Nonnull
  @Override
  public List<Long> getBytesList(DriverOption option) {
    return getCached(option.getPath(), getEffectiveOptions()::getBytesList);
  }

  @Nonnull
  @Override
  public DriverExecutionProfile withBytesList(
      @Nonnull DriverOption option, @Nonnull List<Long> value) {
    return with(option, value);
  }

  @Nonnull
  @Override
  public Duration getDuration(@Nonnull DriverOption option) {
    return getCached(option.getPath(), getEffectiveOptions()::getDuration);
  }

  @Nonnull
  @Override
  public DriverExecutionProfile withDuration(
      @Nonnull DriverOption option, @Nonnull Duration value) {
    return with(option, value);
  }

  @Nonnull
  @Override
  public List<Duration> getDurationList(@Nonnull DriverOption option) {
    return getCached(option.getPath(), getEffectiveOptions()::getDurationList);
  }

  @Nonnull
  @Override
  public DriverExecutionProfile withDurationList(
      @Nonnull DriverOption option, @Nonnull List<Duration> value) {
    return with(option, value);
  }

  @Nonnull
  @Override
  public DriverExecutionProfile without(@Nonnull DriverOption option) {
    return with(option, null);
  }

  @Nonnull
  @Override
  public Object getComparisonKey(@Nonnull DriverOption option) {
    // This method has a default implementation in the interface, but here we can do it in one line:
    return getEffectiveOptions().getConfig(option.getPath());
  }

  @Nonnull
  @Override
  public SortedSet<Map.Entry<String, Object>> entrySet() {
    ImmutableSortedSet.Builder<Map.Entry<String, Object>> builder =
        ImmutableSortedSet.orderedBy(Map.Entry.comparingByKey());
    for (Map.Entry<String, ConfigValue> entry : getEffectiveOptions().entrySet()) {
      builder.add(new AbstractMap.SimpleEntry<>(entry.getKey(), entry.getValue().unwrapped()));
    }
    return builder.build();
  }

  private <T> T getCached(String path, Function<String, T> compute) {
    // compute's signature guarantees we get a T, and this is the only place where we mutate the
    // entry
    @SuppressWarnings("unchecked")
    T t = (T) cache.computeIfAbsent(path, compute);
    return t;
  }

  private DriverExecutionProfile with(@Nonnull DriverOption option, @Nullable Object value) {
    Base base = getBaseProfile();
    // Add the new option to any already derived options
    Config newAdded =
        getAddedOptions().withValue(option.getPath(), ConfigValueFactory.fromAnyRef(value));
    Derived derived = new Derived(base, newAdded);
    base.register(derived);
    return derived;
  }

  /** A profile that was loaded directly from the driver's configuration. */
  @ThreadSafe
  static class Base extends TypesafeDriverExecutionProfile {

    private final String name;
    private volatile Config options;
    private volatile Set<Derived> derivedProfiles;

    Base(String name, Config options) {
      this.name = name;
      this.options = options;
    }

    @Nonnull
    @Override
    public String getName() {
      return name;
    }

    @Override
    protected Base getBaseProfile() {
      return this;
    }

    @Override
    protected Config getAddedOptions() {
      return ConfigFactory.empty();
    }

    @Override
    protected Config getEffectiveOptions() {
      return options;
    }

    void refresh(Config newOptions) {
      this.options = newOptions;
      this.cache.clear();
      if (derivedProfiles != null) {
        for (Derived derivedProfile : derivedProfiles) {
          derivedProfile.refresh();
        }
      }
    }

    void register(Derived derivedProfile) {
      getDerivedProfiles().add(derivedProfile);
    }

    // Lazy init
    private Set<Derived> getDerivedProfiles() {
      Set<Derived> result = derivedProfiles;
      if (result == null) {
        synchronized (this) {
          result = derivedProfiles;
          if (result == null) {
            derivedProfiles =
                result = Collections.newSetFromMap(new MapMaker().weakKeys().makeMap());
          }
        }
      }
      return result;
    }
  }

  /**
   * A profile that was copied from another profile programmatically using {@code withXxx} methods.
   */
  @ThreadSafe
  static class Derived extends TypesafeDriverExecutionProfile {

    private final Base baseProfile;
    private final Config addedOptions;
    private volatile Config effectiveOptions;

    Derived(Base baseProfile, Config addedOptions) {
      this.baseProfile = baseProfile;
      this.addedOptions = addedOptions;
      refresh();
    }

    void refresh() {
      this.effectiveOptions = addedOptions.withFallback(baseProfile.getEffectiveOptions());
      this.cache.clear();
    }

    @Nonnull
    @Override
    public String getName() {
      return baseProfile.getName();
    }

    @Override
    protected Base getBaseProfile() {
      return baseProfile;
    }

    @Override
    protected Config getAddedOptions() {
      return addedOptions;
    }

    @Override
    protected Config getEffectiveOptions() {
      return effectiveOptions;
    }
  }
}
