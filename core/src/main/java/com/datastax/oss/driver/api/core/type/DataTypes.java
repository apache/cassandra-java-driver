/*
 * Copyright (C) 2017-2017 DataStax Inc.
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
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.google.common.collect.ImmutableList;
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

  public static DataType custom(String className) {
    // In protocol v4, duration is implemented as a custom type
    if ("org.apache.cassandra.db.marshal.DurationType".equals(className)) {
      return DURATION;
    } else {
      return new DefaultCustomType(className);
    }
  }

  public static ListType listOf(DataType elementType) {
    // frozen == true is only used in column definitions, it's unlikely that end users will need to
    // create such instances.
    return new DefaultListType(elementType, false);
  }

  public static SetType setOf(DataType elementType) {
    return new DefaultSetType(elementType, false);
  }

  public static MapType mapOf(DataType keyType, DataType valueType) {
    return new DefaultMapType(keyType, valueType, false);
  }

  /**
   * Builds a new, <em>detached</em> tuple type.
   *
   * @see Detachable
   */
  public static TupleType tupleOf(DataType... componentTypes) {
    return new DefaultTupleType(ImmutableList.copyOf(Arrays.asList(componentTypes)));
  }
}
