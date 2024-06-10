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
package com.datastax.oss.driver.internal.core.type.codec.extras.time;

import com.datastax.oss.driver.api.core.type.codec.MappingCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.codec.TimestampCodec;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Objects;
import net.jcip.annotations.Immutable;

/**
 * {@link TypeCodec} that maps {@link LocalDateTime} to CQL {@code timestamp}, allowing the setting
 * and retrieval of {@code timestamp} columns as {@link LocalDateTime} instances.
 *
 * <p>This codec shares its logic with {@link TimestampCodec}. See the javadocs of this codec for
 * important remarks about implementation notes and accepted timestamp formats.
 */
@Immutable
public class LocalTimestampCodec extends MappingCodec<Instant, LocalDateTime> {

  private final ZoneId timeZone;

  /**
   * Creates a new {@code LocalTimestampCodec} that converts CQL timestamps into {@link
   * LocalDateTime} instances using the system's {@linkplain ZoneId#systemDefault() default time
   * zone} as their time zone. The supplied {@code timeZone} will also be used to parse CQL
   * timestamp literals that do not include any time zone information.
   */
  public LocalTimestampCodec() {
    this(ZoneId.systemDefault());
  }

  /**
   * Creates a new {@code LocalTimestampCodec} that converts CQL timestamps into {@link
   * LocalDateTime} instances using the given {@link ZoneId} as their time zone. The supplied {@code
   * timeZone} will also be used to parse CQL timestamp literals that do not include any time zone
   * information.
   */
  public LocalTimestampCodec(@NonNull ZoneId timeZone) {
    super(
        new TimestampCodec(Objects.requireNonNull(timeZone, "timeZone cannot be null")),
        GenericType.LOCAL_DATE_TIME);
    this.timeZone = timeZone;
  }

  @Override
  public boolean accepts(@NonNull Object value) {
    Objects.requireNonNull(value);
    return value instanceof LocalDateTime;
  }

  @Nullable
  @Override
  protected LocalDateTime innerToOuter(@Nullable Instant value) {
    return value == null ? null : LocalDateTime.ofInstant(value, timeZone);
  }

  @Nullable
  @Override
  protected Instant outerToInner(@Nullable LocalDateTime value) {
    return value == null ? null : value.atZone(timeZone).toInstant();
  }
}
