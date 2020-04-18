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
package com.datastax.oss.driver.internal.core.adminrequest;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.response.result.ColumnSpec;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import net.jcip.annotations.Immutable;

@Immutable
public class AdminRow {

  @VisibleForTesting
  static final TypeCodec<List<String>> LIST_OF_TEXT = TypeCodecs.listOf(TypeCodecs.TEXT);

  private static final TypeCodec<Set<String>> SET_OF_TEXT = TypeCodecs.setOf(TypeCodecs.TEXT);
  private static final TypeCodec<Map<String, String>> MAP_OF_STRING_TO_STRING =
      TypeCodecs.mapOf(TypeCodecs.TEXT, TypeCodecs.TEXT);

  private final Map<String, ColumnSpec> columnSpecs;
  private final List<ByteBuffer> data;
  private final ProtocolVersion protocolVersion;

  public AdminRow(
      Map<String, ColumnSpec> columnSpecs, List<ByteBuffer> data, ProtocolVersion protocolVersion) {
    this.columnSpecs = columnSpecs;
    this.data = data;
    this.protocolVersion = protocolVersion;
  }

  @Nullable
  public Boolean getBoolean(String columnName) {
    return get(columnName, TypeCodecs.BOOLEAN);
  }

  @Nullable
  public Integer getInteger(String columnName) {
    return get(columnName, TypeCodecs.INT);
  }

  public boolean isString(String columnName) {
    return columnSpecs.get(columnName).type.id == ProtocolConstants.DataType.VARCHAR;
  }

  @Nullable
  public String getString(String columnName) {
    return get(columnName, TypeCodecs.TEXT);
  }

  @Nullable
  public UUID getUuid(String columnName) {
    return get(columnName, TypeCodecs.UUID);
  }

  @Nullable
  public ByteBuffer getByteBuffer(String columnName) {
    return get(columnName, TypeCodecs.BLOB);
  }

  @Nullable
  public InetAddress getInetAddress(String columnName) {
    return get(columnName, TypeCodecs.INET);
  }

  @Nullable
  public List<String> getListOfString(String columnName) {
    return get(columnName, LIST_OF_TEXT);
  }

  @Nullable
  public Set<String> getSetOfString(String columnName) {
    return get(columnName, SET_OF_TEXT);
  }

  @Nullable
  public Map<String, String> getMapOfStringToString(String columnName) {
    return get(columnName, MAP_OF_STRING_TO_STRING);
  }

  public boolean isNull(String columnName) {
    if (!contains(columnName)) {
      return true;
    } else {
      int index = columnSpecs.get(columnName).index;
      return data.get(index) == null;
    }
  }

  public boolean contains(String columnName) {
    return columnSpecs.containsKey(columnName);
  }

  @Nullable
  public <T> T get(String columnName, TypeCodec<T> codec) {
    // Minimal checks here: this is for internal use, so the caller should know what they're
    // doing
    if (!contains(columnName)) {
      return null;
    } else {
      int index = columnSpecs.get(columnName).index;
      return codec.decode(data.get(index), protocolVersion);
    }
  }
}
