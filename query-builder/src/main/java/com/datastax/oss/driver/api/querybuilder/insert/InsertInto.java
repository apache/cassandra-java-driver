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
package com.datastax.oss.driver.api.querybuilder.insert;

import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.querybuilder.BindMarker;
import com.datastax.oss.driver.internal.querybuilder.insert.DefaultInsert;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * The beginning of an INSERT statement; at this point only the table is known, it might become a
 * JSON insert or a regular one, depending on which method is called next.
 */
public interface InsertInto extends OngoingValues {

  /** Makes this statement an INSERT JSON with the provided JSON string. */
  @NonNull
  JsonInsert json(@NonNull String json);

  /** Makes this statement an INSERT JSON with a bind marker, as in {@code INSERT JSON ?}. */
  @NonNull
  JsonInsert json(@NonNull BindMarker bindMarker);

  /**
   * Makes this statement an INSERT JSON with a custom type mapping, as in {@code INSERT JSON ?}.
   *
   * <p>This is an alternative to {@link #json(String)} for custom type mappings. The provided
   * registry should contain a codec that can format the value. Typically, this will be your
   * session's registry, which is accessible via {@code session.getContext().getCodecRegistry()}.
   *
   * @throws CodecNotFoundException if {@code codecRegistry} does not contain any codec that can
   *     handle {@code value}.
   * @see DriverContext#getCodecRegistry()
   * @see DefaultInsert#json(Object, CodecRegistry)
   */
  @NonNull
  default JsonInsert json(@Nullable Object value, @NonNull CodecRegistry codecRegistry) {
    try {
      return json(value, (value == null) ? null : codecRegistry.codecFor(value));
    } catch (CodecNotFoundException e) {
      throw new IllegalArgumentException(
          String.format(
              "Could not inline literal of type %s. "
                  + "This happens because the driver doesn't know how to map it to a CQL type. "
                  + "Try passing a TypeCodec or CodecRegistry to literal().",
              (value == null) ? null : value.getClass().getName()),
          e);
    }
  }

  /**
   * Makes this statement an INSERT JSON with a custom type mapping, as in {@code INSERT JSON ?}.
   * The value will be turned into a string with {@link TypeCodec#format(Object)}, and inlined in
   * the query.
   *
   * @see DefaultInsert#json(T, TypeCodec)
   */
  @NonNull
  <T> JsonInsert json(@Nullable T value, @Nullable TypeCodec<T> codec);
}
