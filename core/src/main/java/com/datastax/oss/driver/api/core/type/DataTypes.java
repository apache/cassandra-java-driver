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
package com.datastax.oss.driver.api.core.type;

import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.detach.Detachable;
import com.datastax.oss.driver.internal.core.metadata.schema.parsing.DataTypeClassNameParser;
import com.datastax.oss.driver.internal.core.type.DefaultCustomType;
import com.datastax.oss.driver.internal.core.type.DefaultListType;
import com.datastax.oss.driver.internal.core.type.DefaultMapType;
import com.datastax.oss.driver.internal.core.type.DefaultSetType;
import com.datastax.oss.driver.internal.core.type.DefaultTupleType;
import com.datastax.oss.driver.internal.core.type.DefaultVectorType;
import com.datastax.oss.driver.internal.core.type.PrimitiveType;
import com.datastax.oss.driver.shaded.guava.common.base.Splitter;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nonnull;

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

  private static final DataTypeClassNameParser classNameParser = new DataTypeClassNameParser();
  private static final Splitter paramSplitter = Splitter.on(',').trimResults();

  @Nonnull
  public static DataType custom(@Nonnull String className) {

    // In protocol v4, duration is implemented as a custom type
    if (className.equals("org.apache.cassandra.db.marshal.DurationType")) return DURATION;

    /* Vector support is currently implemented as a custom type but is also parameterized */
    if (className.startsWith(DefaultVectorType.VECTOR_CLASS_NAME)) {
      List<String> params =
          paramSplitter.splitToList(
              className.substring(
                  DefaultVectorType.VECTOR_CLASS_NAME.length() + 1, className.length() - 1));
      DataType subType = classNameParser.parse(params.get(0), AttachmentPoint.NONE);
      int dimensions = Integer.parseInt(params.get(1));
      if (dimensions <= 0) {
        throw new IllegalArgumentException(
            String.format(
                "Request to create vector of size %d, size must be positive", dimensions));
      }
      return new DefaultVectorType(subType, dimensions);
    }
    return new DefaultCustomType(className);
  }

  @Nonnull
  public static ListType listOf(@Nonnull DataType elementType) {
    return new DefaultListType(elementType, false);
  }

  @Nonnull
  public static ListType listOf(@Nonnull DataType elementType, boolean frozen) {
    return new DefaultListType(elementType, frozen);
  }

  @Nonnull
  public static ListType frozenListOf(@Nonnull DataType elementType) {
    return new DefaultListType(elementType, true);
  }

  @Nonnull
  public static SetType setOf(@Nonnull DataType elementType) {
    return new DefaultSetType(elementType, false);
  }

  @Nonnull
  public static SetType setOf(@Nonnull DataType elementType, boolean frozen) {
    return new DefaultSetType(elementType, frozen);
  }

  @Nonnull
  public static SetType frozenSetOf(@Nonnull DataType elementType) {
    return new DefaultSetType(elementType, true);
  }

  @Nonnull
  public static MapType mapOf(@Nonnull DataType keyType, @Nonnull DataType valueType) {
    return new DefaultMapType(keyType, valueType, false);
  }

  @Nonnull
  public static MapType mapOf(
      @Nonnull DataType keyType, @Nonnull DataType valueType, boolean frozen) {
    return new DefaultMapType(keyType, valueType, frozen);
  }

  @Nonnull
  public static MapType frozenMapOf(@Nonnull DataType keyType, @Nonnull DataType valueType) {
    return new DefaultMapType(keyType, valueType, true);
  }

  /**
   * Builds a new, <em>detached</em> tuple type.
   *
   * @param componentTypes neither the individual types, nor the vararg array itself, can be {@code
   *     null}.
   * @see Detachable
   */
  @Nonnull
  public static TupleType tupleOf(@Nonnull DataType... componentTypes) {
    return new DefaultTupleType(ImmutableList.copyOf(Arrays.asList(componentTypes)));
  }

  public static VectorType vectorOf(DataType subtype, int dimensions) {
    return new DefaultVectorType(subtype, dimensions);
  }
}
