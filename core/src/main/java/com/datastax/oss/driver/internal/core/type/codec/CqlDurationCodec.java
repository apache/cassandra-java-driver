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
package com.datastax.oss.driver.internal.core.type.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.util.VIntCoding;
import com.datastax.oss.driver.shaded.guava.common.io.ByteArrayDataOutput;
import com.datastax.oss.driver.shaded.guava.common.io.ByteStreams;
import com.datastax.oss.protocol.internal.util.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class CqlDurationCodec implements TypeCodec<CqlDuration> {
  @NonNull
  @Override
  public GenericType<CqlDuration> getJavaType() {
    return GenericType.CQL_DURATION;
  }

  @NonNull
  @Override
  public DataType getCqlType() {
    return DataTypes.DURATION;
  }

  @Override
  public boolean accepts(@NonNull Object value) {
    return value instanceof CqlDuration;
  }

  @Override
  public boolean accepts(@NonNull Class<?> javaClass) {
    return javaClass == CqlDuration.class;
  }

  @Nullable
  @Override
  public ByteBuffer encode(@Nullable CqlDuration value, @NonNull ProtocolVersion protocolVersion) {
    if (value == null) {
      return null;
    }
    long months = value.getMonths();
    long days = value.getDays();
    long nanoseconds = value.getNanoseconds();
    int size =
        VIntCoding.computeVIntSize(months)
            + VIntCoding.computeVIntSize(days)
            + VIntCoding.computeVIntSize(nanoseconds);
    ByteArrayDataOutput out = ByteStreams.newDataOutput(size);
    try {
      VIntCoding.writeVInt(months, out);
      VIntCoding.writeVInt(days, out);
      VIntCoding.writeVInt(nanoseconds, out);
    } catch (IOException e) {
      // cannot happen
      throw new AssertionError();
    }
    return ByteBuffer.wrap(out.toByteArray());
  }

  @Nullable
  @Override
  public CqlDuration decode(@Nullable ByteBuffer bytes, @NonNull ProtocolVersion protocolVersion) {
    if (bytes == null || bytes.remaining() == 0) {
      return null;
    } else {
      DataInput in = ByteStreams.newDataInput(Bytes.getArray(bytes));
      try {
        int months = (int) VIntCoding.readVInt(in);
        int days = (int) VIntCoding.readVInt(in);
        long nanoseconds = VIntCoding.readVInt(in);
        return CqlDuration.newInstance(months, days, nanoseconds);
      } catch (IOException e) {
        // cannot happen
        throw new AssertionError();
      }
    }
  }

  @NonNull
  @Override
  public String format(@Nullable CqlDuration value) {
    return (value == null) ? "NULL" : value.toString();
  }

  @Nullable
  @Override
  public CqlDuration parse(@Nullable String value) {
    return (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL"))
        ? null
        : CqlDuration.from(value);
  }
}
