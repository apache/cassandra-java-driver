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
package com.datastax.driver.extras.codecs.jdk8;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.IgnoreJDK6Requirement;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import java.nio.ByteBuffer;
import java.time.DateTimeException;

/** {@link TypeCodec} that maps {@link java.time.ZoneId} to CQL {@code varchar}. */
@IgnoreJDK6Requirement
@SuppressWarnings("Since15")
public class ZoneIdCodec extends TypeCodec<java.time.ZoneId> {

  public static final ZoneIdCodec instance = new ZoneIdCodec();

  private ZoneIdCodec() {
    super(DataType.varchar(), java.time.ZoneId.class);
  }

  @Override
  public ByteBuffer serialize(java.time.ZoneId value, ProtocolVersion protocolVersion) {
    if (value == null) {
      return null;
    }
    return varchar().serialize(value.toString(), protocolVersion);
  }

  @Override
  public java.time.ZoneId deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) {
    if (bytes == null || bytes.remaining() == 0) {
      return null;
    }
    return java.time.ZoneId.of(varchar().deserialize(bytes, protocolVersion));
  }

  @Override
  public String format(java.time.ZoneId value) {
    if (value == null) {
      return "NULL";
    }
    return varchar().format(value.toString());
  }

  @Override
  public java.time.ZoneId parse(String value) {
    String parsed = varchar().parse(value);
    if (parsed == null || parsed.isEmpty() || parsed.equalsIgnoreCase("NULL")) {
      return null;
    }
    try {
      return java.time.ZoneId.of(parsed);
    } catch (DateTimeException e) {
      String msg = String.format("Cannot parse zone-ID value from \"%s\"", value);
      throw new InvalidTypeException(msg);
    }
  }
}
