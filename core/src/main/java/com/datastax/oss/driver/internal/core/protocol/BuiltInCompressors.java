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
package com.datastax.oss.driver.internal.core.protocol;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.protocol.internal.Compressor;
import io.netty.buffer.ByteBuf;
import java.util.Locale;

/**
 * Provides a single entry point to create compressor instances in the driver.
 *
 * <p>Note that this class also serves as a convenient target for GraalVM substitutions, see {@link
 * CompressorSubstitutions}.
 */
public class BuiltInCompressors {

  public static Compressor<ByteBuf> newInstance(String name, DriverContext context) {
    switch (name.toLowerCase(Locale.ROOT)) {
      case "lz4":
        return new Lz4Compressor(context);
      case "snappy":
        return new SnappyCompressor(context);
      case "none":
        return Compressor.none();
      default:
        throw new IllegalArgumentException(
            String.format(
                "Unsupported compression algorithm '%s' (from configuration option %s)",
                name, DefaultDriverOption.PROTOCOL_COMPRESSION.getPath()));
    }
  }
}
