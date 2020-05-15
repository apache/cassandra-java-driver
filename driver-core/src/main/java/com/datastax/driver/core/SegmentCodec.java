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

import com.datastax.driver.core.exceptions.CrcMismatchException;
import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.io.IOException;
import java.util.List;

class SegmentCodec {

  private static final int COMPRESSED_HEADER_LENGTH = 5;
  private static final int UNCOMPRESSED_HEADER_LENGTH = 3;
  static final int CRC24_LENGTH = 3;
  static final int CRC32_LENGTH = 4;

  private final ByteBufAllocator allocator;
  private final boolean compress;
  private final FrameCompressor compressor;

  SegmentCodec(ByteBufAllocator allocator, ProtocolOptions.Compression compression) {
    this.allocator = allocator;
    this.compress = compression != ProtocolOptions.Compression.NONE;
    this.compressor = compression.compressor();
  }

  /** The length of the segment header, excluding the 3-byte trailing CRC. */
  int headerLength() {
    return compress ? COMPRESSED_HEADER_LENGTH : UNCOMPRESSED_HEADER_LENGTH;
  }

  void encode(Segment segment, List<Object> out) throws IOException {
    ByteBuf uncompressedPayload = segment.getPayload();
    int uncompressedPayloadLength = uncompressedPayload.readableBytes();
    assert uncompressedPayloadLength <= Segment.MAX_PAYLOAD_LENGTH;
    ByteBuf encodedPayload;
    if (compress) {
      uncompressedPayload.markReaderIndex();
      ByteBuf compressedPayload = compressor.compress(uncompressedPayload);
      if (compressedPayload.readableBytes() >= uncompressedPayloadLength) {
        // Skip compression if it's not worth it
        uncompressedPayload.resetReaderIndex();
        encodedPayload = uncompressedPayload;
        compressedPayload.release();
        // By convention, this is how we signal this to the server:
        uncompressedPayloadLength = 0;
      } else {
        encodedPayload = compressedPayload;
        uncompressedPayload.release();
      }
    } else {
      encodedPayload = uncompressedPayload;
    }
    int payloadLength = encodedPayload.readableBytes();

    ByteBuf header =
        encodeHeader(payloadLength, uncompressedPayloadLength, segment.isSelfContained());

    int payloadCrc = Crc.computeCrc32(encodedPayload);
    ByteBuf trailer = allocator.ioBuffer(CRC32_LENGTH);
    for (int i = 0; i < CRC32_LENGTH; i++) {
      trailer.writeByte(payloadCrc & 0xFF);
      payloadCrc >>= 8;
    }

    out.add(header);
    out.add(encodedPayload);
    out.add(trailer);
  }

  @VisibleForTesting
  ByteBuf encodeHeader(int payloadLength, int uncompressedLength, boolean isSelfContained) {
    assert payloadLength <= Segment.MAX_PAYLOAD_LENGTH;
    assert !compress || uncompressedLength <= Segment.MAX_PAYLOAD_LENGTH;

    int headerLength = headerLength();

    long headerData = payloadLength;
    int flagOffset = 17;
    if (compress) {
      headerData |= (long) uncompressedLength << 17;
      flagOffset += 17;
    }
    if (isSelfContained) {
      headerData |= 1L << flagOffset;
    }

    int headerCrc = Crc.computeCrc24(headerData, headerLength);

    ByteBuf header = allocator.ioBuffer(headerLength + CRC24_LENGTH);
    // Write both data and CRC in little-endian order
    for (int i = 0; i < headerLength; i++) {
      int shift = i * 8;
      header.writeByte((int) (headerData >> shift & 0xFF));
    }
    for (int i = 0; i < CRC24_LENGTH; i++) {
      int shift = i * 8;
      header.writeByte(headerCrc >> shift & 0xFF);
    }
    return header;
  }

  /**
   * Decodes a segment header and checks its CRC. It is assumed that the caller has already checked
   * that there are enough bytes.
   */
  Header decodeHeader(ByteBuf buffer) throws CrcMismatchException {
    int headerLength = headerLength();
    assert buffer.readableBytes() >= headerLength + CRC24_LENGTH;

    // Read header data (little endian):
    long headerData = 0;
    for (int i = 0; i < headerLength; i++) {
      headerData |= (buffer.readByte() & 0xFFL) << 8 * i;
    }

    // Read CRC (little endian) and check it:
    int expectedHeaderCrc = 0;
    for (int i = 0; i < CRC24_LENGTH; i++) {
      expectedHeaderCrc |= (buffer.readByte() & 0xFF) << 8 * i;
    }
    int actualHeaderCrc = Crc.computeCrc24(headerData, headerLength);
    if (actualHeaderCrc != expectedHeaderCrc) {
      throw new CrcMismatchException(
          String.format(
              "CRC mismatch on header %s. Received %s, computed %s.",
              Long.toHexString(headerData),
              Integer.toHexString(expectedHeaderCrc),
              Integer.toHexString(actualHeaderCrc)));
    }

    int payloadLength = (int) headerData & Segment.MAX_PAYLOAD_LENGTH;
    headerData >>= 17;
    int uncompressedPayloadLength;
    if (compress) {
      uncompressedPayloadLength = (int) headerData & Segment.MAX_PAYLOAD_LENGTH;
      headerData >>= 17;
    } else {
      uncompressedPayloadLength = -1;
    }
    boolean isSelfContained = (headerData & 1) == 1;
    return new Header(payloadLength, uncompressedPayloadLength, isSelfContained);
  }

  /**
   * Decodes the rest of a segment from a previously decoded header, and checks the payload's CRC.
   * It is assumed that the caller has already checked that there are enough bytes.
   */
  Segment decode(Header header, ByteBuf buffer) throws CrcMismatchException, IOException {
    assert buffer.readableBytes() == header.payloadLength + CRC32_LENGTH;

    // Extract payload:
    ByteBuf encodedPayload = buffer.readSlice(header.payloadLength);
    encodedPayload.retain();

    // Read and check CRC:
    int expectedPayloadCrc = 0;
    for (int i = 0; i < CRC32_LENGTH; i++) {
      expectedPayloadCrc |= (buffer.readByte() & 0xFF) << 8 * i;
    }
    buffer.release(); // done with this (we retained the payload independently)
    int actualPayloadCrc = Crc.computeCrc32(encodedPayload);
    if (actualPayloadCrc != expectedPayloadCrc) {
      encodedPayload.release();
      throw new CrcMismatchException(
          String.format(
              "CRC mismatch on payload. Received %s, computed %s.",
              Integer.toHexString(expectedPayloadCrc), Integer.toHexString(actualPayloadCrc)));
    }

    // Decompress payload if needed:
    ByteBuf payload;
    if (compress && header.uncompressedPayloadLength > 0) {
      payload = compressor.decompress(encodedPayload, header.uncompressedPayloadLength);
      encodedPayload.release();
    } else {
      payload = encodedPayload;
    }

    return new Segment(payload, header.isSelfContained);
  }

  /** Temporary holder for header data. During decoding, it is convenient to store it separately. */
  static class Header {
    final int payloadLength;
    final int uncompressedPayloadLength;
    final boolean isSelfContained;

    public Header(int payloadLength, int uncompressedPayloadLength, boolean isSelfContained) {
      this.payloadLength = payloadLength;
      this.uncompressedPayloadLength = uncompressedPayloadLength;
      this.isSelfContained = isSelfContained;
    }
  }
}
