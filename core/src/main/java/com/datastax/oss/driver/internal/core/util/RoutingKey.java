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
package com.datastax.oss.driver.internal.core.util;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.ByteBuffer;

public class RoutingKey {

  /** Assembles multiple routing key components into a single buffer. */
  @NonNull
  public static ByteBuffer compose(@NonNull ByteBuffer... components) {
    if (components.length == 1) return components[0];

    int totalLength = 0;
    for (ByteBuffer bb : components) totalLength += 2 + bb.remaining() + 1;

    ByteBuffer out = ByteBuffer.allocate(totalLength);
    for (ByteBuffer buffer : components) {
      ByteBuffer bb = buffer.duplicate();
      putShortLength(out, bb.remaining());
      out.put(bb);
      out.put((byte) 0);
    }
    out.flip();
    return out;
  }

  private static void putShortLength(ByteBuffer bb, int length) {
    bb.put((byte) ((length >> 8) & 0xFF));
    bb.put((byte) (length & 0xFF));
  }
}
