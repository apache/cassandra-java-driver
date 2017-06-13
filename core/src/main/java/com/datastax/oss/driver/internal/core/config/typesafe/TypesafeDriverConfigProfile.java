/*
 * Copyright (C) 2017-2017 DataStax Inc.
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

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.config.DriverOption;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import java.time.Duration;
import java.util.List;

public class TypesafeDriverConfigProfile implements DriverConfigProfile {
  // The base options loaded from the driver's configuration
  private volatile Config base;
  // Any extras that were configured manually using withXxx methods
  private final Config extras;
  // The actual options returned by getXxx methods (which is a merge of the previous two)
  private volatile Config actual;

  public TypesafeDriverConfigProfile(Config base) {
    this(base, ConfigFactory.empty());
  }

  private TypesafeDriverConfigProfile(Config base, Config extras) {
    this.base = base;
    this.extras = extras;
    this.actual = extras.withFallback(base);
  }

  @Override
  public boolean isDefined(DriverOption option) {
    return actual.hasPath(option.getPath());
  }

  @Override
  public boolean getBoolean(DriverOption option) {
    return actual.getBoolean(option.getPath());
  }

  @Override
  public DriverConfigProfile withBoolean(DriverOption option, boolean value) {
    return with(option, value);
  }

  @Override
  public int getInt(DriverOption option) {
    return actual.getInt(option.getPath());
  }

  @Override
  public DriverConfigProfile withInt(DriverOption option, int value) {
    return with(option, value);
  }

  @Override
  public Duration getDuration(DriverOption option) {
    return actual.getDuration(option.getPath());
  }

  @Override
  public DriverConfigProfile withDuration(DriverOption option, Duration value) {
    return with(option, value);
  }

  @Override
  public String getString(DriverOption option) {
    return actual.getString(option.getPath());
  }

  @Override
  public DriverConfigProfile withString(DriverOption option, String value) {
    return with(option, value);
  }

  @Override
  public List<String> getStringList(DriverOption option) {
    return actual.getStringList(option.getPath());
  }

  @Override
  public DriverConfigProfile withStringList(DriverOption option, List<String> value) {
    return with(option, value);
  }

  @Override
  public long getBytes(DriverOption option) {
    return actual.getBytes(option.getPath());
  }

  @Override
  public DriverConfigProfile withBytes(DriverOption option, long value) {
    return with(option, value);
  }

  @Override
  public ConsistencyLevel getConsistencyLevel(DriverOption option) {
    String name = getString(option);
    return ConsistencyLevel.valueOf(name);
  }

  @Override
  public DriverConfigProfile withConsistencyLevel(DriverOption option, ConsistencyLevel value) {
    return with(option, value.toString());
  }

  private DriverConfigProfile with(DriverOption option, Object v) {
    return new TypesafeDriverConfigProfile(
        base, extras.withValue(option.getPath(), ConfigValueFactory.fromAnyRef(v)));
  }
}
