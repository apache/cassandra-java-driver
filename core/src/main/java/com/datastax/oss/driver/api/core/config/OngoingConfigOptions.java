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
package com.datastax.oss.driver.api.core.config;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

/** An object where config options can be set programmatically. */
public interface OngoingConfigOptions<SelfT extends OngoingConfigOptions<SelfT>> {

  @Nonnull
  SelfT withBoolean(@Nonnull DriverOption option, boolean value);

  @Nonnull
  SelfT withBooleanList(@Nonnull DriverOption option, @Nonnull List<Boolean> value);

  @Nonnull
  SelfT withInt(@Nonnull DriverOption option, int value);

  @Nonnull
  SelfT withIntList(@Nonnull DriverOption option, @Nonnull List<Integer> value);

  @Nonnull
  SelfT withLong(@Nonnull DriverOption option, long value);

  @Nonnull
  SelfT withLongList(@Nonnull DriverOption option, @Nonnull List<Long> value);

  @Nonnull
  SelfT withDouble(@Nonnull DriverOption option, double value);

  @Nonnull
  SelfT withDoubleList(@Nonnull DriverOption option, @Nonnull List<Double> value);

  @Nonnull
  SelfT withString(@Nonnull DriverOption option, @Nonnull String value);

  /**
   * Note that this is just a shortcut to call {@link #withString(DriverOption, String)} with {@code
   * value.getName()}.
   */
  @Nonnull
  default SelfT withClass(@Nonnull DriverOption option, @Nonnull Class<?> value) {
    return withString(option, value.getName());
  }

  /**
   * Note that this is just a shortcut to call {@link #withStringList(DriverOption, List)} with
   * class names obtained from {@link Class#getName()}.
   */
  @Nonnull
  default SelfT withClassList(@Nonnull DriverOption option, @Nonnull List<Class<?>> values) {
    return withStringList(option, values.stream().map(Class::getName).collect(Collectors.toList()));
  }

  @Nonnull
  SelfT withStringList(@Nonnull DriverOption option, @Nonnull List<String> value);

  @Nonnull
  SelfT withStringMap(@Nonnull DriverOption option, @Nonnull Map<String, String> value);

  @Nonnull
  SelfT withBytes(@Nonnull DriverOption option, long value);

  @Nonnull
  SelfT withBytesList(@Nonnull DriverOption option, @Nonnull List<Long> value);

  @Nonnull
  SelfT withDuration(@Nonnull DriverOption option, @Nonnull Duration value);

  @Nonnull
  SelfT withDurationList(@Nonnull DriverOption option, @Nonnull List<Duration> value);

  @Nonnull
  SelfT without(@Nonnull DriverOption option);
}
