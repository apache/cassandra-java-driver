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

import static com.datastax.oss.driver.internal.core.util.Dependency.LZ4;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.internal.core.util.GraalDependencyChecker;
import com.datastax.oss.protocol.internal.Compressor;
import com.oracle.svm.core.annotate.Substitute;
import com.oracle.svm.core.annotate.TargetClass;
import io.netty.buffer.ByteBuf;
import java.util.Locale;
import java.util.function.BooleanSupplier;

/**
 * Handles GraalVM substitutions for compressors: LZ4 is only supported if we can find the native
 * library in the classpath, and Snappy is never supported.
 *
 * <p>When a compressor is not supported, we delete its class, and modify {@link
 * BuiltInCompressors#newInstance(String, DriverContext)} to throw an error if the user attempts to
 * configure it.
 */
@SuppressWarnings("unused")
public class CompressorSubstitutions {

  @TargetClass(value = BuiltInCompressors.class, onlyWith = Lz4Present.class)
  public static final class BuiltInCompressorsLz4Only {
    @Substitute
    public static Compressor<ByteBuf> newInstance(String name, DriverContext context) {
      switch (name.toLowerCase(Locale.ROOT)) {
        case "lz4":
          return new Lz4Compressor(context);
        case "snappy":
          throw new UnsupportedOperationException(
              "Snappy compression is not supported for native images");
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

  @TargetClass(value = BuiltInCompressors.class, onlyWith = Lz4Missing.class)
  public static final class NoBuiltInCompressors {
    @Substitute
    public static Compressor<ByteBuf> newInstance(String name, DriverContext context) {
      switch (name.toLowerCase(Locale.ROOT)) {
        case "lz4":
          throw new UnsupportedOperationException(
              "This native image was not built with support for LZ4 compression");
        case "snappy":
          throw new UnsupportedOperationException(
              "Snappy compression is not supported for native images");
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

  public static class Lz4Present implements BooleanSupplier {
    @Override
    public boolean getAsBoolean() {
      return GraalDependencyChecker.isPresent(LZ4);
    }
  }

  public static class Lz4Missing extends Lz4Present {
    @Override
    public boolean getAsBoolean() {
      return !super.getAsBoolean();
    }
  }
}
