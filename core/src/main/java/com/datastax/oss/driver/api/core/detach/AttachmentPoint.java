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
package com.datastax.oss.driver.api.core.detach;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.core.type.codec.registry.MutableCodecRegistry;
import edu.umd.cs.findbugs.annotations.NonNull;

/** @see Detachable */
public interface AttachmentPoint {
  AttachmentPoint NONE =
      new AttachmentPoint() {
        @NonNull
        @Override
        public ProtocolVersion getProtocolVersion() {
          return ProtocolVersion.DEFAULT;
        }

        @NonNull
        @Override
        public CodecRegistry getCodecRegistry() {
          return CodecRegistry.DEFAULT;
        }
      };

  @NonNull
  ProtocolVersion getProtocolVersion();

  /**
   * Note that the default registry implementation returned by the driver also implements {@link
   * MutableCodecRegistry}, which allows you to register new codecs at runtime. You can safely cast
   * the result of this method (as long as you didn't extend the driver context to plug a custom
   * registry implementation).
   */
  @NonNull
  CodecRegistry getCodecRegistry();
}
