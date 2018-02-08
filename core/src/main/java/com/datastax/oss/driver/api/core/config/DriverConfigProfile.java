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
package com.datastax.oss.driver.api.core.config;

import java.time.Duration;
import java.util.List;

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
public interface DriverConfigProfile {
  boolean isDefined(DriverOption option);

  boolean getBoolean(DriverOption option);

  DriverConfigProfile withBoolean(DriverOption option, boolean value);

  int getInt(DriverOption option);

  DriverConfigProfile withInt(DriverOption option, int value);

  String getString(DriverOption option);

  DriverConfigProfile withString(DriverOption option, String value);

  List<String> getStringList(DriverOption option);

  DriverConfigProfile withStringList(DriverOption option, List<String> value);

  /** Returns a size in bytes. */
  long getBytes(DriverOption option);

  DriverConfigProfile withBytes(DriverOption option, long value);

  Duration getDuration(DriverOption option);

  DriverConfigProfile withDuration(DriverOption option, Duration value);
}
