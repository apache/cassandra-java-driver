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
package com.datastax.oss.driver.api.core.data;

import com.datastax.oss.protocol.internal.util.Bytes;
import java.nio.ByteBuffer;

/**
 * A set of static utility methods to work with byte buffers (associated with CQL type {@code
 * blob}).
 */
public class ByteUtils {

  // Implementation note: this is just a gateway to the internal `Bytes` class in native-protocol.
  // The difference is that this one is part of the public API.

  /**
   * Converts a blob to its CQL hex string representation.
   *
   * <p>A CQL blob string representation consists of the hexadecimal representation of the blob
   * bytes prefixed by "0x".
   *
   * @param bytes the blob/bytes to convert to a string.
   * @return the CQL string representation of {@code bytes}. If {@code bytes} is {@code null}, this
   *     method returns {@code null}.
   */
  public static String toHexString(ByteBuffer bytes) {
    return Bytes.toHexString(bytes);
  }

  /**
   * Converts a blob to its CQL hex string representation.
   *
   * <p>A CQL blob string representation consists of the hexadecimal representation of the blob
   * bytes prefixed by "0x".
   *
   * @param byteArray the blob/bytes array to convert to a string.
   * @return the CQL string representation of {@code bytes}. If {@code bytes} is {@code null}, this
   *     method returns {@code null}.
   */
  public static String toHexString(byte[] byteArray) {
    return Bytes.toHexString(byteArray);
  }

  /**
   * Parses a hex string representing a CQL blob.
   *
   * <p>The input should be a valid representation of a CQL blob, i.e. it must start by "0x"
   * followed by the hexadecimal representation of the blob bytes.
   *
   * @param str the CQL blob string representation to parse.
   * @return the bytes corresponding to {@code str}. If {@code str} is {@code null}, this method
   *     returns {@code null}.
   * @throws IllegalArgumentException if {@code str} is not a valid CQL blob string.
   */
  public static ByteBuffer fromHexString(String str) {
    return Bytes.fromHexString(str);
  }

  /**
   * Extracts the content of the provided {@code ByteBuffer} as a byte array.
   *
   * <p>This method works with any type of {@code ByteBuffer} (direct and non-direct ones), but when
   * the buffer is backed by an array, it will try to avoid copy when possible. As a consequence,
   * changes to the returned byte array may or may not reflect into the initial buffer.
   *
   * @param bytes the buffer whose contents to extract.
   * @return a byte array with the contents of {@code bytes}. That array may be the array backing
   *     {@code bytes} if this can avoid a copy.
   */
  public static byte[] getArray(ByteBuffer bytes) {
    return Bytes.getArray(bytes);
  }

  private ByteUtils() {}
}
