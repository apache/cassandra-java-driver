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
package com.datastax.oss.driver.internal.core.protocol;

import com.oracle.svm.core.annotate.Inject;
import com.oracle.svm.core.annotate.Substitute;
import com.oracle.svm.core.annotate.TargetClass;
import io.netty.buffer.ByteBuf;

/**
 * Snappy compression relies on the underlying native library and thus is not supported for native
 * images
 */
@TargetClass(className = "com.datastax.oss.driver.internal.core.protocol.SnappyCompressor")
final class SnappySubstitution {

  @Inject
  private final String EXCEPTION_MSG = "Snappy compression is not supported for native images";

  @Substitute
  protected ByteBuf compressHeap(ByteBuf input) {
    throw new UnsupportedOperationException(EXCEPTION_MSG);
  }

  @Substitute
  protected ByteBuf decompressDirect(ByteBuf input) {
    throw new UnsupportedOperationException(EXCEPTION_MSG);
  }

  @Substitute
  protected ByteBuf decompressHeap(ByteBuf input) {
    throw new UnsupportedOperationException(EXCEPTION_MSG);
  }

  @Substitute
  protected ByteBuf compressDirect(ByteBuf input) {
    throw new UnsupportedOperationException(EXCEPTION_MSG);
  }
}
