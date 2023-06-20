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
package com.datastax.oss.driver.internal.core.type.codec.extras.time;

import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.codec.ExtraTypeCodecs;
import com.datastax.oss.driver.api.core.type.codec.MappingCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Objects;
import net.jcip.annotations.Immutable;

/**
 * {@link TypeCodec} that maps {@link ZonedDateTime} to CQL {@code tuple<timestamp,varchar>},
 * providing a pattern for maintaining timezone information in Cassandra.
 *
 * <p>Since Cassandra's <code>timestamp</code> type does not store any time zone, by using a <code>
 * tuple&lt;timestamp,varchar&gt;</code> a timezone can be persisted in the <code>varchar
 * </code> field of such tuples, and so when the value is deserialized the original timezone is
 * preserved.
 *
 * <p>Note: if you want to retrieve CQL timestamps as {@link ZonedDateTime} instances but don't need
 * to persist the time zone to the database, you should rather use {@link ZonedTimestampCodec}.
 */
@Immutable
public class PersistentZonedTimestampCodec extends MappingCodec<TupleValue, ZonedDateTime> {

  private static final TupleType CQL_TYPE = DataTypes.tupleOf(DataTypes.TIMESTAMP, DataTypes.TEXT);

  public PersistentZonedTimestampCodec() {
    super(TypeCodecs.tupleOf(CQL_TYPE), GenericType.ZONED_DATE_TIME);
  }

  @Override
  public boolean accepts(@NonNull Object value) {
    Objects.requireNonNull(value);
    return value instanceof ZonedDateTime;
  }

  @NonNull
  @Override
  public TupleType getCqlType() {
    return CQL_TYPE;
  }

  @NonNull
  @Override
  public String format(@Nullable ZonedDateTime value) {
    if (value == null) {
      return "NULL";
    }
    // Use TIMESTAMP_UTC for a better-looking format
    return "("
        + ExtraTypeCodecs.TIMESTAMP_UTC.format(value.toInstant())
        + ","
        + TypeCodecs.TEXT.format(value.getZone().toString())
        + ")";
  }

  @Nullable
  @Override
  protected ZonedDateTime innerToOuter(@Nullable TupleValue value) {
    if (value == null) {
      return null;
    } else {
      Instant instant = Objects.requireNonNull(value.getInstant(0));
      ZoneId zoneId = ZoneId.of(Objects.requireNonNull(value.getString(1)));
      return ZonedDateTime.ofInstant(instant, zoneId);
    }
  }

  @Nullable
  @Override
  protected TupleValue outerToInner(@Nullable ZonedDateTime value) {
    if (value == null) {
      return null;
    } else {
      Instant instant = value.toInstant();
      String zoneId = value.getZone().toString();
      return getCqlType().newValue(instant, zoneId);
    }
  }
}
