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
package com.datastax.oss.driver.api.core.type.codec.registry;

import com.datastax.oss.driver.api.core.type.codec.TypeCodec;

/**
 * A codec registry that can be extended with new user codecs at runtime.
 *
 * <p>This interface only exists to preserve backward compatibility. In practice, the default {@link
 * CodecRegistry} implementation returned by the driver implements this interface, so it can safely
 * be cast.
 *
 * <p>However {@link CodecRegistry#DEFAULT} is immutable. It implements this interface, but {@link
 * #register(TypeCodec)} throws an {@link UnsupportedOperationException}.
 *
 * @since 4.3.0
 */
public interface MutableCodecRegistry extends CodecRegistry {

  /**
   * Adds the given codec to the registry.
   *
   * <p>This method will log a warning and ignore the codec if it collides with one already present
   * in the registry. Note that the driver's built-in implementation uses internal synchronization
   * to guarantee that two threads cannot register colliding codecs concurrently; registration is
   * not expected to happen in a very concurrent manner, so this should not pose a performance
   * issue.
   */
  void register(TypeCodec<?> codec);

  /** Invokes {@link #register(TypeCodec)} for every codec in the given list. */
  default void register(TypeCodec<?>... codecs) {
    for (TypeCodec<?> codec : codecs) {
      register(codec);
    }
  }

  /** Invokes {@link #register(TypeCodec)} for every codec in the given list. */
  default void register(Iterable<TypeCodec<?>> codecs) {
    for (TypeCodec<?> codec : codecs) {
      register(codec);
    }
  }
}
