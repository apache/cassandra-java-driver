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
 * A codec that serializes {@link Enum} instances as CQL {@code varchar}s representing their
 * programmatic names as returned by {@link Enum#name()}.
 *
 * <p><strong>Note that this codec relies on the enum constant names; it is therefore vital that
 * enum names never change.</strong>
 *
 * @param <EnumT> The Enum class this codec serializes from and deserializes to.
 */
@Immutable
public class EnumNameCodec<EnumT extends Enum<EnumT>> extends MappingCodec<String, EnumT> {

  private final Class<EnumT> enumClass;

  public EnumNameCodec(@NonNull Class<EnumT> enumClass) {
    super(
        TypeCodecs.TEXT,
        GenericType.of(Objects.requireNonNull(enumClass, "enumClass must not be null")));
    this.enumClass = enumClass;
  }

  @Nullable
  @Override
  protected EnumT innerToOuter(@Nullable String value) {
    return value == null || value.isEmpty() ? null : Enum.valueOf(enumClass, value);
  }

  @Nullable
  @Override
  protected String outerToInner(@Nullable EnumT value) {
    return value == null ? null : value.name();
  }
}
