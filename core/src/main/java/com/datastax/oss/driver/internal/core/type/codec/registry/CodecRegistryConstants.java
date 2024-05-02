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
package com.datastax.oss.driver.internal.core.type.codec.registry;

import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

public class CodecRegistryConstants {

  /**
   * The driver's default primitive codecs (map all primitive CQL types to their "natural" Java
   * equivalent).
   *
   * <p>This is exposed in case you want to call {@link
   * DefaultCodecRegistry#DefaultCodecRegistry(String, int, BiFunction, int, BiConsumer,
   * TypeCodec[])} but only customize the caching options.
   */
  public static final TypeCodec<?>[] PRIMITIVE_CODECS =
      new TypeCodec<?>[] {
        // Must be declared before AsciiCodec so it gets chosen when CQL type not available
        TypeCodecs.TEXT,
        // Must be declared before TimeUUIDCodec so it gets chosen when CQL type not available
        TypeCodecs.UUID,
        TypeCodecs.TIMEUUID,
        TypeCodecs.TIMESTAMP,
        TypeCodecs.INT,
        TypeCodecs.BIGINT,
        TypeCodecs.BLOB,
        TypeCodecs.DOUBLE,
        TypeCodecs.FLOAT,
        TypeCodecs.DECIMAL,
        TypeCodecs.VARINT,
        TypeCodecs.INET,
        TypeCodecs.BOOLEAN,
        TypeCodecs.SMALLINT,
        TypeCodecs.TINYINT,
        TypeCodecs.DATE,
        TypeCodecs.TIME,
        TypeCodecs.DURATION,
        TypeCodecs.COUNTER,
        TypeCodecs.ASCII
      };
}
