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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.metadata.schema.Describable;
import com.datastax.oss.driver.internal.core.metadata.schema.ScriptBuilder;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import java.util.List;

public interface UserDefinedType extends DataType, Describable {
  CqlIdentifier getKeyspace();

  CqlIdentifier getName();

  boolean isFrozen();

  List<CqlIdentifier> getFieldNames();

  int firstIndexOf(CqlIdentifier id);

  int firstIndexOf(String name);

  default boolean contains(CqlIdentifier id) {
    return firstIndexOf(id) >= 0;
  }

  default boolean contains(String name) {
    return firstIndexOf(name) >= 0;
  }

  List<DataType> getFieldTypes();

  UserDefinedType copy(boolean newFrozen);

  UdtValue newValue();

  AttachmentPoint getAttachmentPoint();

  @Override
  default String asCql(boolean includeFrozen, boolean pretty) {
    String template = (isFrozen() && includeFrozen) ? "frozen<%s.%s>" : "%s.%s";
    return String.format(template, getKeyspace().asCql(pretty), getName().asCql(pretty));
  }

  @Override
  default String describe(boolean pretty) {
    ScriptBuilder builder = new ScriptBuilder(pretty);

    builder
        .append("CREATE TYPE ")
        .append(getKeyspace())
        .append(".")
        .append(getName())
        .append(" (")
        .newLine()
        .increaseIndent();

    List<CqlIdentifier> fieldNames = getFieldNames();
    List<DataType> fieldTypes = getFieldTypes();
    int fieldCount = fieldNames.size();
    for (int i = 0; i < fieldCount; i++) {
      builder.append(fieldNames.get(i)).append(" ").append(fieldTypes.get(i).asCql(true, pretty));
      if (i < fieldCount - 1) {
        builder.append(",");
      }
      builder.newLine();
    }
    builder.decreaseIndent().append(");");
    return builder.build();
  }

  @Override
  default String describeWithChildren(boolean pretty) {
    // No children (if it uses other types, they're considered dependencies, not sub-elements)
    return describe(pretty);
  }

  default int getProtocolCode() {
    return ProtocolConstants.DataType.UDT;
  }
}
