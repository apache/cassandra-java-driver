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
package com.datastax.oss.driver.api.querybuilder.insert;

import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.querybuilder.BindMarker;
import edu.umd.cs.findbugs.annotations.NonNull;

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
   * Makes this statement an INSERT JSON with a custom type mapping. The provided {@code Object
   * value} will be mapped to a JSON string.
   *
   * <p>This is an alternative to {@link #json(String)} for custom type mappings. The provided
   * registry should contain a codec that can format the value. Typically, this will be your
   * session's registry, which is accessible via {@code session.getContext().getCodecRegistry()}.
   *
   * @throws CodecNotFoundException if {@code codecRegistry} does not contain any codec that can
   *     handle {@code value}.
   * @see DriverContext#getCodecRegistry()
   */
  @NonNull
  default JsonInsert json(@NonNull Object value, @NonNull CodecRegistry codecRegistry) {
    try {
      return json(value, codecRegistry.codecFor(value));
    } catch (CodecNotFoundException e) {
      throw new IllegalArgumentException(
          String.format(
              "Could not inline JSON literal of type %s. "
                  + "This happens because the provided CodecRegistry does not contain "
                  + "a codec for this type. Try registering your TypeCodec in the registry first, "
                  + "or use json(Object, TypeCodec).",
              value.getClass().getName()),
          e);
    }
  }

  /**
   * Makes this statement an INSERT JSON with a custom type mapping. The provided {@code Object
   * value} will be mapped to a JSON string. The value will be turned into a string with {@link
   * TypeCodec#format(Object)}, and inlined in the query.
   */
  @NonNull
  <T> JsonInsert json(@NonNull T value, @NonNull TypeCodec<T> codec);
}
