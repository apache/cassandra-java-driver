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

import com.datastax.oss.driver.api.core.config.DriverOption;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** @see com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoaderBuilder */
@Deprecated
public interface DriverOptionConfigBuilder<SelfT extends DriverOptionConfigBuilder> {

  @Nonnull
  @CheckReturnValue
  default SelfT withBoolean(@Nonnull DriverOption option, boolean value) {
    return with(option, value);
  }

  @Nonnull
  @CheckReturnValue
  default SelfT withBooleanList(@Nonnull DriverOption option, @Nonnull List<Boolean> value) {
    return with(option, value);
  }

  @Nonnull
  @CheckReturnValue
  default SelfT withInt(@Nonnull DriverOption option, int value) {
    return with(option, value);
  }

  @Nonnull
  @CheckReturnValue
  default SelfT withIntList(@Nonnull DriverOption option, @Nonnull List<Integer> value) {
    return with(option, value);
  }

  @Nonnull
  @CheckReturnValue
  default SelfT withLong(@Nonnull DriverOption option, long value) {
    return with(option, value);
  }

  @Nonnull
  @CheckReturnValue
  default SelfT withLongList(@Nonnull DriverOption option, @Nonnull List<Long> value) {
    return with(option, value);
  }

  @Nonnull
  @CheckReturnValue
  default SelfT withDouble(@Nonnull DriverOption option, double value) {
    return with(option, value);
  }

  @Nonnull
  @CheckReturnValue
  default SelfT withDoubleList(@Nonnull DriverOption option, @Nonnull List<Double> value) {
    return with(option, value);
  }

  @Nonnull
  @CheckReturnValue
  default SelfT withString(@Nonnull DriverOption option, @Nonnull String value) {
    return with(option, value);
  }

  @Nonnull
  @CheckReturnValue
  default SelfT withStringList(@Nonnull DriverOption option, @Nonnull List<String> value) {
    return with(option, value);
  }

  @SuppressWarnings("unchecked")
  @Nonnull
  @CheckReturnValue
  default SelfT withStringMap(@Nonnull DriverOption option, @Nonnull Map<String, String> value) {
    SelfT v = (SelfT) this;
    for (String key : value.keySet()) {
      v = (SelfT) v.with(option.getPath() + "." + key, value.get(key));
    }
    return v;
  }

  /**
   * Specifies a size in bytes. This is separate from {@link #withLong(DriverOption, long)}, in case
   * implementations want to allow users to provide sizes in a more human-readable way, for example
   * "256 MB".
   */
  @Nonnull
  @CheckReturnValue
  default SelfT withBytes(@Nonnull DriverOption option, @Nonnull String value) {
    return with(option, value);
  }

  @Nonnull
  @CheckReturnValue
  default SelfT withBytes(@Nonnull DriverOption option, long value) {
    return with(option, value);
  }

  @Nonnull
  @CheckReturnValue
  default SelfT withBytesList(@Nonnull DriverOption option, @Nonnull List<Long> value) {
    return with(option, value);
  }

  @Nonnull
  @CheckReturnValue
  default SelfT withDuration(@Nonnull DriverOption option, @Nonnull Duration value) {
    return with(option, value);
  }

  @Nonnull
  @CheckReturnValue
  default SelfT withDurationList(@Nonnull DriverOption option, @Nonnull List<Duration> value) {
    return with(option, value);
  }

  @Nonnull
  @CheckReturnValue
  default SelfT withClass(@Nonnull DriverOption option, @Nonnull Class<?> value) {
    return with(option, value.getName());
  }

  /** Unsets an option. */
  @Nonnull
  @CheckReturnValue
  default SelfT without(@Nonnull DriverOption option) {
    return with(option, null);
  }

  @Nonnull
  @CheckReturnValue
  default SelfT with(@Nonnull DriverOption option, @Nullable Object value) {
    return with(option.getPath(), value);
  }

  /**
   * Provides a simple path to value mapping, all default methods invoke this method directly. It is
   * not recommended that it is used directly other than by these defaults.
   */
  @Nonnull
  @CheckReturnValue
  SelfT with(@Nonnull String path, @Nullable Object value);
}
