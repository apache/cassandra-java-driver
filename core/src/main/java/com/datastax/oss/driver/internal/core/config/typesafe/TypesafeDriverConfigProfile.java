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
package com.datastax.oss.driver.internal.core.config.typesafe;

import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public abstract class TypesafeDriverConfigProfile implements DriverConfigProfile {

  /** The original profile in the driver's configuration that this profile was derived from. */
  protected abstract Base getBaseProfile();

  /** The extra options that were added with {@code withXxx} methods. */
  protected abstract Config getAddedOptions();

  /** The actual options that will be used to answer {@code getXxx} calls. */
  protected abstract Config getEffectiveOptions();

  protected final ConcurrentMap<String, Object> cache = new ConcurrentHashMap<>();

  @Override
  public boolean isDefined(DriverOption option) {
    return getEffectiveOptions().hasPath(option.getPath());
  }

  @Override
  public boolean getBoolean(DriverOption option) {
    return getCached(option.getPath(), getEffectiveOptions()::getBoolean);
  }

  @Override
  public DriverConfigProfile withBoolean(DriverOption option, boolean value) {
    return with(option, value);
  }

  @Override
  public int getInt(DriverOption option) {
    return getCached(option.getPath(), getEffectiveOptions()::getInt);
  }

  @Override
  public DriverConfigProfile withInt(DriverOption option, int value) {
    return with(option, value);
  }

  @Override
  public Duration getDuration(DriverOption option) {
    return getCached(option.getPath(), getEffectiveOptions()::getDuration);
  }

  @Override
  public DriverConfigProfile withDuration(DriverOption option, Duration value) {
    return with(option, value);
  }

  @Override
  public String getString(DriverOption option) {
    return getCached(option.getPath(), getEffectiveOptions()::getString);
  }

  @Override
  public DriverConfigProfile withString(DriverOption option, String value) {
    return with(option, value);
  }

  @Override
  public List<String> getStringList(DriverOption option) {
    return getCached(option.getPath(), getEffectiveOptions()::getStringList);
  }

  @Override
  public DriverConfigProfile withStringList(DriverOption option, List<String> value) {
    return with(option, value);
  }

  @Override
  public Map<String, String> getStringMap(DriverOption option) {
    Config subConfig = getCached(option.getPath(), getEffectiveOptions()::getConfig);
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    for (Map.Entry<String, ConfigValue> entry : subConfig.entrySet()) {
      if (entry.getValue().valueType().equals(ConfigValueType.STRING)) {
        builder.put(entry.getKey(), (String) entry.getValue().unwrapped());
      }
    }
    return builder.build();
  }

  @Override
  public DriverConfigProfile withStringMap(DriverOption option, Map<String, String> map) {
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
  public long getBytes(DriverOption option) {
    return getCached(option.getPath(), getEffectiveOptions()::getBytes);
  }

  @Override
  public DriverConfigProfile withBytes(DriverOption option, long value) {
    return with(option, value);
  }

  @Override
  public Object getComparisonKey(DriverOption option) {
    // No need to cache this, it's only used for policy initialization
    return getEffectiveOptions().getConfig(option.getPath());
  }

  @Override
  public SortedSet<Map.Entry<String, Object>> entrySet() {
    ImmutableSortedSet.Builder<Map.Entry<String, Object>> builder =
        ImmutableSortedSet.orderedBy(Comparator.comparing(Map.Entry::getKey));
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

  private DriverConfigProfile with(DriverOption option, Object value) {
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
  static class Base extends TypesafeDriverConfigProfile {

    private final String name;
    private volatile Config options;
    private volatile Set<Derived> derivedProfiles;

    Base(String name, Config options) {
      this.name = name;
      this.options = options;
    }

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
   * A profile that was copied from another profile programatically using {@code withXxx} methods.
   */
  @ThreadSafe
  static class Derived extends TypesafeDriverConfigProfile {

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
