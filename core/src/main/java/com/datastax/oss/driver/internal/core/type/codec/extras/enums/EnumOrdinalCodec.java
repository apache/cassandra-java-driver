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
package com.datastax.oss.driver.internal.core.type.codec.extras.enums;

import com.datastax.oss.driver.api.core.type.codec.MappingCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Objects;
import net.jcip.annotations.Immutable;

/**
 * A codec that serializes {@link Enum} instances as CQL {@code int}s representing their ordinal
 * values as returned by {@link Enum#ordinal()}.
 *
 * <p><strong>Note that this codec relies on the enum constants declaration order; it is therefore
 * vital that this order remains immutable.</strong>
 *
 * @param <EnumT> The Enum class this codec serializes from and deserializes to.
 */
@Immutable
public class EnumOrdinalCodec<EnumT extends Enum<EnumT>> extends MappingCodec<Integer, EnumT> {

  private final EnumT[] enumConstants;

  public EnumOrdinalCodec(@NonNull Class<EnumT> enumClass) {
    super(
        TypeCodecs.INT,
        GenericType.of(Objects.requireNonNull(enumClass, "enumClass must not be null")));
    this.enumConstants = enumClass.getEnumConstants();
  }

  @Nullable
  @Override
  protected EnumT innerToOuter(@Nullable Integer value) {
    return value == null ? null : enumConstants[value];
  }

  @Nullable
  @Override
  protected Integer outerToInner(@Nullable EnumT value) {
    return value == null ? null : value.ordinal();
  }
}
