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
package com.datastax.oss.driver.api.core.type;

import com.datastax.oss.driver.api.core.detach.Detachable;
import com.datastax.oss.driver.internal.core.type.DefaultCustomType;
import com.datastax.oss.driver.internal.core.type.DefaultListType;
import com.datastax.oss.driver.internal.core.type.DefaultMapType;
import com.datastax.oss.driver.internal.core.type.DefaultSetType;
import com.datastax.oss.driver.internal.core.type.DefaultTupleType;
import com.datastax.oss.driver.internal.core.type.PrimitiveType;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Arrays;

/** Constants and factory methods to obtain data type instances. */
public class DataTypes {

  public static final DataType ASCII = new PrimitiveType(ProtocolConstants.DataType.ASCII);
  public static final DataType BIGINT = new PrimitiveType(ProtocolConstants.DataType.BIGINT);
  public static final DataType BLOB = new PrimitiveType(ProtocolConstants.DataType.BLOB);
  public static final DataType BOOLEAN = new PrimitiveType(ProtocolConstants.DataType.BOOLEAN);
  public static final DataType COUNTER = new PrimitiveType(ProtocolConstants.DataType.COUNTER);
  public static final DataType DECIMAL = new PrimitiveType(ProtocolConstants.DataType.DECIMAL);
  public static final DataType DOUBLE = new PrimitiveType(ProtocolConstants.DataType.DOUBLE);
  public static final DataType FLOAT = new PrimitiveType(ProtocolConstants.DataType.FLOAT);
  public static final DataType INT = new PrimitiveType(ProtocolConstants.DataType.INT);
  public static final DataType TIMESTAMP = new PrimitiveType(ProtocolConstants.DataType.TIMESTAMP);
  public static final DataType UUID = new PrimitiveType(ProtocolConstants.DataType.UUID);
  public static final DataType VARINT = new PrimitiveType(ProtocolConstants.DataType.VARINT);
  public static final DataType TIMEUUID = new PrimitiveType(ProtocolConstants.DataType.TIMEUUID);
  public static final DataType INET = new PrimitiveType(ProtocolConstants.DataType.INET);
  public static final DataType DATE = new PrimitiveType(ProtocolConstants.DataType.DATE);
  public static final DataType TEXT = new PrimitiveType(ProtocolConstants.DataType.VARCHAR);
  public static final DataType TIME = new PrimitiveType(ProtocolConstants.DataType.TIME);
  public static final DataType SMALLINT = new PrimitiveType(ProtocolConstants.DataType.SMALLINT);
  public static final DataType TINYINT = new PrimitiveType(ProtocolConstants.DataType.TINYINT);
  public static final DataType DURATION = new PrimitiveType(ProtocolConstants.DataType.DURATION);

  @NonNull
  public static DataType custom(@NonNull String className) {
    // In protocol v4, duration is implemented as a custom type
    if ("org.apache.cassandra.db.marshal.DurationType".equals(className)) {
      return DURATION;
    } else {
      return new DefaultCustomType(className);
    }
  }

  @NonNull
  public static ListType listOf(@NonNull DataType elementType) {
    return new DefaultListType(elementType, false);
  }

  @NonNull
  public static ListType listOf(@NonNull DataType elementType, boolean frozen) {
    return new DefaultListType(elementType, frozen);
  }

  @NonNull
  public static ListType frozenListOf(@NonNull DataType elementType) {
    return new DefaultListType(elementType, true);
  }

  @NonNull
  public static SetType setOf(@NonNull DataType elementType) {
    return new DefaultSetType(elementType, false);
  }

  @NonNull
  public static SetType setOf(@NonNull DataType elementType, boolean frozen) {
    return new DefaultSetType(elementType, frozen);
  }

  @NonNull
  public static SetType frozenSetOf(@NonNull DataType elementType) {
    return new DefaultSetType(elementType, true);
  }

  @NonNull
  public static MapType mapOf(@NonNull DataType keyType, @NonNull DataType valueType) {
    return new DefaultMapType(keyType, valueType, false);
  }

  @NonNull
  public static MapType mapOf(
      @NonNull DataType keyType, @NonNull DataType valueType, boolean frozen) {
    return new DefaultMapType(keyType, valueType, frozen);
  }

  @NonNull
  public static MapType frozenMapOf(@NonNull DataType keyType, @NonNull DataType valueType) {
    return new DefaultMapType(keyType, valueType, true);
  }

  /**
   * Builds a new, <em>detached</em> tuple type.
   *
   * @param componentTypes neither the individual types, nor the vararg array itself, can be {@code
   *     null}.
   * @see Detachable
   */
  @NonNull
  public static TupleType tupleOf(@NonNull DataType... componentTypes) {
    return new DefaultTupleType(ImmutableList.copyOf(Arrays.asList(componentTypes)));
  }
}
