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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.metadata.schema.Describable;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.internal.core.metadata.schema.ScriptBuilder;
import com.datastax.oss.driver.internal.core.util.Loggers;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface UserDefinedType extends DataType, Describable {

  @Nullable // because of ShallowUserDefinedType usage in the query builder
  CqlIdentifier getKeyspace();

  @Nonnull
  CqlIdentifier getName();

  boolean isFrozen();

  @Nonnull
  List<CqlIdentifier> getFieldNames();

  /**
   * @apiNote the default implementation only exists for backward compatibility. It wraps the result
   *     of {@link #firstIndexOf(CqlIdentifier)} in a singleton list, which is not entirely correct,
   *     as it will only return the first occurrence. Therefore it also logs a warning.
   *     <p>Implementors should always override this method (all built-in driver implementations
   *     do).
   */
  @Nonnull
  default List<Integer> allIndicesOf(@Nonnull CqlIdentifier id) {
    Loggers.USER_DEFINED_TYPE.warn(
        "{} should override allIndicesOf(CqlIdentifier), the default implementation is a "
            + "workaround for backward compatibility, it only returns the first occurrence",
        getClass().getName());
    return Collections.singletonList(firstIndexOf(id));
  }

  int firstIndexOf(@Nonnull CqlIdentifier id);

  /**
   * @apiNote the default implementation only exists for backward compatibility. It wraps the result
   *     of {@link #firstIndexOf(String)} in a singleton list, which is not entirely correct, as it
   *     will only return the first occurrence. Therefore it also logs a warning.
   *     <p>Implementors should always override this method (all built-in driver implementations
   *     do).
   */
  @Nonnull
  default List<Integer> allIndicesOf(@Nonnull String name) {
    Loggers.USER_DEFINED_TYPE.warn(
        "{} should override allIndicesOf(String), the default implementation is a "
            + "workaround for backward compatibility, it only returns the first occurrence",
        getClass().getName());
    return Collections.singletonList(firstIndexOf(name));
  }

  int firstIndexOf(@Nonnull String name);

  default boolean contains(@Nonnull CqlIdentifier id) {
    return firstIndexOf(id) >= 0;
  }

  default boolean contains(@Nonnull String name) {
    return firstIndexOf(name) >= 0;
  }

  @Nonnull
  List<DataType> getFieldTypes();

  @Nonnull
  UserDefinedType copy(boolean newFrozen);

  @Nonnull
  UdtValue newValue();

  /**
   * Creates a new instance with the specified values for the fields.
   *
   * <p>To encode the values, this method uses the {@link CodecRegistry} that this type is {@link
   * #getAttachmentPoint() attached} to; it looks for the best codec to handle the target CQL type
   * and actual runtime type of each value (see {@link CodecRegistry#codecFor(DataType, Object)}).
   *
   * @param fields the value of the fields. They must be in the same order as the fields in the
   *     type's definition. You can specify less values than there are fields (the remaining ones
   *     will be set to NULL), but not more (a runtime exception will be thrown). Individual values
   *     can be {@code null}, but the array itself can't.
   * @throws IllegalArgumentException if there are too many values.
   */
  @Nonnull
  UdtValue newValue(@Nonnull Object... fields);

  @Nonnull
  AttachmentPoint getAttachmentPoint();

  @Nonnull
  @Override
  default String asCql(boolean includeFrozen, boolean pretty) {
    if (getKeyspace() != null) {
      String template = (isFrozen() && includeFrozen) ? "frozen<%s.%s>" : "%s.%s";
      return String.format(template, getKeyspace().asCql(pretty), getName().asCql(pretty));
    } else {
      String template = (isFrozen() && includeFrozen) ? "frozen<%s>" : "%s";
      return String.format(template, getName().asCql(pretty));
    }
  }

  @Nonnull
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

  @Nonnull
  @Override
  default String describeWithChildren(boolean pretty) {
    // No children (if it uses other types, they're considered dependencies, not sub-elements)
    return describe(pretty);
  }

  @Override
  default int getProtocolCode() {
    return ProtocolConstants.DataType.UDT;
  }
}
