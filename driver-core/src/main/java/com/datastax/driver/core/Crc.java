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
package com.datastax.driver.core;

import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.FastThreadLocal;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Copied and adapted from the server-side version. */
class Crc {

  private static final Logger logger = LoggerFactory.getLogger(Crc.class);

  private static final FastThreadLocal<CRC32> crc32 =
      new FastThreadLocal<CRC32>() {
        @Override
        protected CRC32 initialValue() {
          return new CRC32();
        }
      };

  private static final byte[] initialBytes =
      new byte[] {(byte) 0xFA, (byte) 0x2D, (byte) 0x55, (byte) 0xCA};

  private static final CrcUpdater CRC_UPDATER = selectCrcUpdater();

  static int computeCrc32(ByteBuf buffer) {
    CRC32 crc = newCrc32();
    CRC_UPDATER.update(crc, buffer);
    return (int) crc.getValue();
  }

  private static CRC32 newCrc32() {
    CRC32 crc = crc32.get();
    crc.reset();
    crc.update(initialBytes);
    return crc;
  }

  private static final int CRC24_INIT = 0x875060;
  /**
   * Polynomial chosen from https://users.ece.cmu.edu/~koopman/crc/index.html, by Philip Koopman
   *
   * <p>This webpage claims a copyright to Philip Koopman, which he licenses under the Creative
   * Commons Attribution 4.0 International License (https://creativecommons.org/licenses/by/4.0)
   *
   * <p>It is unclear if this copyright can extend to a 'fact' such as this specific number,
   * particularly as we do not use Koopman's notation to represent the polynomial, but we anyway
   * attribute his work and link the terms of his license since they are not incompatible with our
   * usage and we greatly appreciate his work.
   *
   * <p>This polynomial provides hamming distance of 8 for messages up to length 105 bits; we only
   * support 8-64 bits at present, with an expected range of 40-48.
   */
  private static final int CRC24_POLY = 0x1974F0B;

  /**
   * NOTE: the order of bytes must reach the wire in the same order the CRC is computed, with the
   * CRC immediately following in a trailer. Since we read in least significant byte order, if you
   * write to a buffer using putInt or putLong, the byte order will be reversed and you will lose
   * the guarantee of protection from burst corruptions of 24 bits in length.
   *
   * <p>Make sure either to write byte-by-byte to the wire, or to use Integer/Long.reverseBytes if
   * you write to a BIG_ENDIAN buffer.
   *
   * <p>See http://users.ece.cmu.edu/~koopman/pubs/ray06_crcalgorithms.pdf
   *
   * <p>Complain to the ethernet spec writers, for having inverse bit to byte significance order.
   *
   * <p>Note we use the most naive algorithm here. We support at most 8 bytes, and typically supply
   * 5 or fewer, so any efficiency of a table approach is swallowed by the time to hit L3, even for
   * a tiny (4bit) table.
   *
   * @param bytes an up to 8-byte register containing bytes to compute the CRC over the bytes AND
   *     bits will be read least-significant to most significant.
   * @param len the number of bytes, greater than 0 and fewer than 9, to be read from bytes
   * @return the least-significant bit AND byte order crc24 using the CRC24_POLY polynomial
   */
  static int computeCrc24(long bytes, int len) {
    int crc = CRC24_INIT;
    while (len-- > 0) {
      crc ^= (bytes & 0xff) << 16;
      bytes >>= 8;

      for (int i = 0; i < 8; i++) {
        crc <<= 1;
        if ((crc & 0x1000000) != 0) crc ^= CRC24_POLY;
      }
    }
    return crc;
  }

  private static CrcUpdater selectCrcUpdater() {
    try {
      CRC32.class.getDeclaredMethod("update", ByteBuffer.class);
      return new Java8CrcUpdater();
    } catch (Exception e) {
      logger.warn(
          "It looks like you are running Java 7 or below. "
              + "CRC checks (used in protocol {} and above) will require a memory copy, which can "
              + "negatively impact performance. Consider using a more modern VM.",
          ProtocolVersion.V5,
          e);
      return new Java6CrcUpdater();
    }
  }

  private interface CrcUpdater {
    void update(CRC32 crc, ByteBuf buffer);
  }

  private static class Java6CrcUpdater implements CrcUpdater {
    @Override
    public void update(CRC32 crc, ByteBuf buffer) {
      if (buffer.hasArray()) {
        crc.update(buffer.array(), buffer.arrayOffset(), buffer.readableBytes());
      } else {
        byte[] bytes = new byte[buffer.readableBytes()];
        buffer.getBytes(buffer.readerIndex(), bytes);
        crc.update(bytes);
      }
    }
  }

  @IgnoreJDK6Requirement
  private static class Java8CrcUpdater implements CrcUpdater {
    @Override
    public void update(CRC32 crc, ByteBuf buffer) {
      crc.update(buffer.internalNioBuffer(buffer.readerIndex(), buffer.readableBytes()));
    }
  }
}
