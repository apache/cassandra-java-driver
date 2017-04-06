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

import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.config.DriverOption;
import com.typesafe.config.Config;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class TypesafeDriverConfigProfile implements DriverConfigProfile {
  private volatile Config config;

  public TypesafeDriverConfigProfile(Config config) {
    this.config = config;
  }

  @Override
  public boolean isDefined(DriverOption option) {
    return config.hasPath(option.getPath());
  }

  @Override
  public boolean getBoolean(DriverOption option) {
    return config.getBoolean(option.getPath());
  }

  @Override
  public int getInt(DriverOption option) {
    return config.getInt(option.getPath());
  }

  @Override
  public Duration getDuration(DriverOption option) {
    return config.getDuration(option.getPath());
  }

  @Override
  public long getDuration(DriverOption option, TimeUnit targetUnit) {
    return config.getDuration(option.getPath(), targetUnit);
  }

  @Override
  public String getString(DriverOption option) {
    return config.getString(option.getPath());
  }

  @Override
  public List<String> getStringList(DriverOption option) {
    return config.getStringList(option.getPath());
  }

  @Override
  public long getBytes(DriverOption option) {
    return config.getBytes(option.getPath());
  }
}
