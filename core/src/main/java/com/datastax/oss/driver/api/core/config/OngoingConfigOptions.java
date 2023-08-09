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

import edu.umd.cs.findbugs.annotations.NonNull;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** An object where config options can be set programmatically. */
public interface OngoingConfigOptions<SelfT extends OngoingConfigOptions<SelfT>> {

  @NonNull
  SelfT withBoolean(@NonNull DriverOption option, boolean value);

  @NonNull
  SelfT withBooleanList(@NonNull DriverOption option, @NonNull List<Boolean> value);

  @NonNull
  SelfT withInt(@NonNull DriverOption option, int value);

  @NonNull
  SelfT withIntList(@NonNull DriverOption option, @NonNull List<Integer> value);

  @NonNull
  SelfT withLong(@NonNull DriverOption option, long value);

  @NonNull
  SelfT withLongList(@NonNull DriverOption option, @NonNull List<Long> value);

  @NonNull
  SelfT withDouble(@NonNull DriverOption option, double value);

  @NonNull
  SelfT withDoubleList(@NonNull DriverOption option, @NonNull List<Double> value);

  @NonNull
  SelfT withString(@NonNull DriverOption option, @NonNull String value);

  /**
   * Note that this is just a shortcut to call {@link #withString(DriverOption, String)} with {@code
   * value.getName()}.
   */
  @NonNull
  default SelfT withClass(@NonNull DriverOption option, @NonNull Class<?> value) {
    return withString(option, value.getName());
  }

  /**
   * Note that this is just a shortcut to call {@link #withStringList(DriverOption, List)} with
   * class names obtained from {@link Class#getName()}.
   */
  @NonNull
  default SelfT withClassList(@NonNull DriverOption option, @NonNull List<Class<?>> values) {
    return withStringList(option, values.stream().map(Class::getName).collect(Collectors.toList()));
  }

  @NonNull
  SelfT withStringList(@NonNull DriverOption option, @NonNull List<String> value);

  @NonNull
  SelfT withStringMap(@NonNull DriverOption option, @NonNull Map<String, String> value);

  @NonNull
  SelfT withBytes(@NonNull DriverOption option, long value);

  @NonNull
  SelfT withBytesList(@NonNull DriverOption option, @NonNull List<Long> value);

  @NonNull
  SelfT withDuration(@NonNull DriverOption option, @NonNull Duration value);

  @NonNull
  SelfT withDurationList(@NonNull DriverOption option, @NonNull List<Duration> value);

  @NonNull
  SelfT without(@NonNull DriverOption option);
}
