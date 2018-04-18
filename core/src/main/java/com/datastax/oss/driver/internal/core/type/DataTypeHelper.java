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
package com.datastax.oss.driver.internal.core.type;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.response.result.RawType;
import com.datastax.oss.protocol.internal.util.IntMap;
import java.util.List;
import java.util.Map;

public class DataTypeHelper {

  public static DataType fromProtocolSpec(RawType rawType, AttachmentPoint attachmentPoint) {
    DataType type = PRIMITIVE_TYPES_BY_CODE.get(rawType.id);
    if (type != null) {
      return type;
    } else {
      switch (rawType.id) {
        case ProtocolConstants.DataType.CUSTOM:
          RawType.RawCustom rawCustom = (RawType.RawCustom) rawType;
          return DataTypes.custom(rawCustom.className);
        case ProtocolConstants.DataType.LIST:
          RawType.RawList rawList = (RawType.RawList) rawType;
          return DataTypes.listOf(fromProtocolSpec(rawList.elementType, attachmentPoint));
        case ProtocolConstants.DataType.SET:
          RawType.RawSet rawSet = (RawType.RawSet) rawType;
          return DataTypes.setOf(fromProtocolSpec(rawSet.elementType, attachmentPoint));
        case ProtocolConstants.DataType.MAP:
          RawType.RawMap rawMap = (RawType.RawMap) rawType;
          return DataTypes.mapOf(
              fromProtocolSpec(rawMap.keyType, attachmentPoint),
              fromProtocolSpec(rawMap.valueType, attachmentPoint));
        case ProtocolConstants.DataType.TUPLE:
          RawType.RawTuple rawTuple = (RawType.RawTuple) rawType;
          List<RawType> rawFieldsList = rawTuple.fieldTypes;
          ImmutableList.Builder<DataType> fields = ImmutableList.builder();
          for (RawType rawField : rawFieldsList) {
            fields.add(fromProtocolSpec(rawField, attachmentPoint));
          }
          return new DefaultTupleType(fields.build(), attachmentPoint);
        case ProtocolConstants.DataType.UDT:
          RawType.RawUdt rawUdt = (RawType.RawUdt) rawType;
          ImmutableList.Builder<CqlIdentifier> fieldNames = ImmutableList.builder();
          ImmutableList.Builder<DataType> fieldTypes = ImmutableList.builder();
          for (Map.Entry<String, RawType> entry : rawUdt.fields.entrySet()) {
            fieldNames.add(CqlIdentifier.fromInternal(entry.getKey()));
            fieldTypes.add(fromProtocolSpec(entry.getValue(), attachmentPoint));
          }
          return new DefaultUserDefinedType(
              CqlIdentifier.fromInternal(rawUdt.keyspace),
              CqlIdentifier.fromInternal(rawUdt.typeName),
              false,
              fieldNames.build(),
              fieldTypes.build(),
              attachmentPoint);
        default:
          throw new IllegalArgumentException("Unsupported type: " + rawType.id);
      }
    }
  }

  private static IntMap<DataType> PRIMITIVE_TYPES_BY_CODE =
      sortByProtocolCode(
          DataTypes.ASCII,
          DataTypes.BIGINT,
          DataTypes.BLOB,
          DataTypes.BOOLEAN,
          DataTypes.COUNTER,
          DataTypes.DECIMAL,
          DataTypes.DOUBLE,
          DataTypes.FLOAT,
          DataTypes.INT,
          DataTypes.TIMESTAMP,
          DataTypes.UUID,
          DataTypes.VARINT,
          DataTypes.TIMEUUID,
          DataTypes.INET,
          DataTypes.DATE,
          DataTypes.TEXT,
          DataTypes.TIME,
          DataTypes.SMALLINT,
          DataTypes.TINYINT,
          DataTypes.DURATION);

  private static IntMap<DataType> sortByProtocolCode(DataType... types) {
    IntMap.Builder<DataType> builder = IntMap.builder();
    for (DataType type : types) {
      builder.put(type.getProtocolCode(), type);
    }
    return builder.build();
  }
}
