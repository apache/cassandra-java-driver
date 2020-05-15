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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.datastax.driver.core.ProtocolOptions.Compression;
import com.datastax.driver.core.SegmentCodec.Header;
import com.datastax.driver.core.exceptions.CrcMismatchException;
import com.google.common.base.Strings;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.testng.annotations.Test;

public class SegmentCodecTest {

  public static final SegmentCodec CODEC_NO_COMPRESSION =
      new SegmentCodec(UnpooledByteBufAllocator.DEFAULT, Compression.NONE);
  public static final SegmentCodec CODEC_LZ4 =
      new SegmentCodec(UnpooledByteBufAllocator.DEFAULT, Compression.LZ4);

  @Test(groups = "unit")
  public void should_encode_uncompressed_header() {
    ByteBuf header = CODEC_NO_COMPRESSION.encodeHeader(5, -1, true);

    byte byte0 = header.getByte(2);
    byte byte1 = header.getByte(1);
    byte byte2 = header.getByte(0);

    assertThat(bits(byte0) + bits(byte1) + bits(byte2))
        .isEqualTo(
            "000000" // padding (6 bits)
                + "1" // selfContainedFlag
                + "00000000000000101" // length (17 bits)
            );
  }

  @Test(groups = "unit")
  public void should_encode_compressed_header() {
    ByteBuf header = CODEC_LZ4.encodeHeader(5, 12, true);

    byte byte0 = header.getByte(4);
    byte byte1 = header.getByte(3);
    byte byte2 = header.getByte(2);
    byte byte3 = header.getByte(1);
    byte byte4 = header.getByte(0);

    assertThat(bits(byte0) + bits(byte1) + bits(byte2) + bits(byte3) + bits(byte4))
        .isEqualTo(
            "00000" // padding (5 bits)
                + "1" // selfContainedFlag
                + "00000000000001100" // uncompressed length (17 bits)
                + "00000000000000101" // compressed length (17 bits)
            );
  }

  /**
   * Checks that we correctly use 8 bytes when we left-shift the uncompressed length, to avoid
   * overflows.
   */
  @Test(groups = "unit")
  public void should_encode_compressed_header_when_aligned_uncompressed_length_overflows() {
    ByteBuf header = CODEC_LZ4.encodeHeader(5, Segment.MAX_PAYLOAD_LENGTH, true);

    byte byte0 = header.getByte(4);
    byte byte1 = header.getByte(3);
    byte byte2 = header.getByte(2);
    byte byte3 = header.getByte(1);
    byte byte4 = header.getByte(0);

    assertThat(bits(byte0) + bits(byte1) + bits(byte2) + bits(byte3) + bits(byte4))
        .isEqualTo(
            "00000" // padding (5 bits)
                + "1" // selfContainedFlag
                + "11111111111111111" // uncompressed length (17 bits)
                + "00000000000000101" // compressed length (17 bits)
            );
  }

  @Test(groups = "unit")
  public void should_decode_uncompressed_payload() {
    // Assembling the test data manually would have little value because it would be very similar to
    // our production code. So simply use that production code, assuming it's correct.
    ByteBuf buffer = CODEC_NO_COMPRESSION.encodeHeader(5, -1, true);
    Header header = CODEC_NO_COMPRESSION.decodeHeader(buffer);
    assertThat(header.payloadLength).isEqualTo(5);
    assertThat(header.uncompressedPayloadLength).isEqualTo(-1);
    assertThat(header.isSelfContained).isTrue();
  }

  @Test(groups = "unit")
  public void should_decode_compressed_payload() {
    ByteBuf buffer = CODEC_LZ4.encodeHeader(5, 12, true);
    Header header = CODEC_LZ4.decodeHeader(buffer);
    assertThat(header.payloadLength).isEqualTo(5);
    assertThat(header.uncompressedPayloadLength).isEqualTo(12);
    assertThat(header.isSelfContained).isTrue();
  }

  @Test(groups = "unit")
  public void should_fail_to_decode_if_corrupted() {
    ByteBuf buffer = CODEC_NO_COMPRESSION.encodeHeader(5, -1, true);

    // Flip a random byte
    for (int bitOffset = 0; bitOffset < 47; bitOffset++) {
      int byteOffset = bitOffset / 8;
      int shift = bitOffset % 8;

      ByteBuf slice = buffer.slice(buffer.readerIndex() + byteOffset, 1);
      slice.markReaderIndex();
      byte byteToCorrupt = slice.readByte();
      slice.resetReaderIndex();
      slice.writerIndex(slice.readerIndex());
      slice.writeByte((byteToCorrupt & 0xFF) ^ (1 << shift));

      try {
        CODEC_NO_COMPRESSION.decodeHeader(buffer.duplicate());
        fail("Expected CrcMismatchException");
      } catch (CrcMismatchException e) {
        // expected
      }
    }
  }

  private static String bits(byte b) {
    return Strings.padStart(Integer.toBinaryString(b & 0xFF), 8, '0');
  }
}
