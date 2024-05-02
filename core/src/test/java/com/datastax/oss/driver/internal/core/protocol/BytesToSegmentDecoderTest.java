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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.datastax.oss.driver.api.core.connection.CrcMismatchException;
import com.datastax.oss.protocol.internal.Compressor;
import com.datastax.oss.protocol.internal.Segment;
import com.datastax.oss.protocol.internal.SegmentCodec;
import com.google.common.base.Strings;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class BytesToSegmentDecoderTest {

  // Hard-coded test data, the values were generated with our encoding methods.
  // We're not really testing the decoding itself here, only that our subclass calls the
  // LengthFieldBasedFrameDecoder parent constructor with the right parameters.
  private static final ByteBuf REGULAR_HEADER = byteBuf("04000201f9f2");
  private static final ByteBuf REGULAR_PAYLOAD = byteBuf("00000001");
  private static final ByteBuf REGULAR_TRAILER = byteBuf("1fd6022d");
  private static final ByteBuf REGULAR_WRONG_HEADER = byteBuf("04000202f9f2");
  private static final ByteBuf REGULAR_WRONG_TRAILER = byteBuf("1fd6022e");

  private static final ByteBuf MAX_HEADER = byteBuf("ffff03254047");
  private static final ByteBuf MAX_PAYLOAD =
      byteBuf(Strings.repeat("01", Segment.MAX_PAYLOAD_LENGTH));
  private static final ByteBuf MAX_TRAILER = byteBuf("a05c2f13");

  private static final ByteBuf LZ4_HEADER = byteBuf("120020000491c94f");
  private static final ByteBuf LZ4_PAYLOAD_UNCOMPRESSED =
      byteBuf("00000001000000010000000100000001");
  private static final ByteBuf LZ4_PAYLOAD_COMPRESSED =
      byteBuf("f00100000001000000010000000100000001");
  private static final ByteBuf LZ4_TRAILER = byteBuf("2bd67f90");

  private static final Compressor<ByteBuf> LZ4_COMPRESSOR = new Lz4Compressor("test");

  private EmbeddedChannel channel;

  @Before
  public void setup() {
    channel = new EmbeddedChannel();
  }

  @Test
  public void should_decode_regular_segment() {
    channel.pipeline().addLast(newDecoder(Compressor.none()));
    channel.writeInbound(Unpooled.wrappedBuffer(REGULAR_HEADER, REGULAR_PAYLOAD, REGULAR_TRAILER));
    Segment<ByteBuf> segment = channel.readInbound();
    assertThat(segment.isSelfContained).isTrue();
    assertThat(segment.payload).isEqualTo(REGULAR_PAYLOAD);
  }

  @Test
  public void should_decode_max_length_segment() {
    channel.pipeline().addLast(newDecoder(Compressor.none()));
    channel.writeInbound(Unpooled.wrappedBuffer(MAX_HEADER, MAX_PAYLOAD, MAX_TRAILER));
    Segment<ByteBuf> segment = channel.readInbound();
    assertThat(segment.isSelfContained).isTrue();
    assertThat(segment.payload).isEqualTo(MAX_PAYLOAD);
  }

  @Test
  public void should_decode_segment_from_multiple_incoming_chunks() {
    channel.pipeline().addLast(newDecoder(Compressor.none()));
    // Send the header in two slices, to cover the case where the length can't be read the first
    // time:
    ByteBuf headerStart = REGULAR_HEADER.slice(0, 3);
    ByteBuf headerEnd = REGULAR_HEADER.slice(3, 3);
    channel.writeInbound(headerStart);
    channel.writeInbound(headerEnd);
    channel.writeInbound(REGULAR_PAYLOAD.duplicate());
    channel.writeInbound(REGULAR_TRAILER.duplicate());
    Segment<ByteBuf> segment = channel.readInbound();
    assertThat(segment.isSelfContained).isTrue();
    assertThat(segment.payload).isEqualTo(REGULAR_PAYLOAD);
  }

  @Test
  public void should_decode_compressed_segment() {
    channel.pipeline().addLast(newDecoder(LZ4_COMPRESSOR));
    // We need a contiguous buffer for this one, because of how our decompressor operates
    ByteBuf buffer = Unpooled.wrappedBuffer(LZ4_HEADER, LZ4_PAYLOAD_COMPRESSED, LZ4_TRAILER).copy();
    channel.writeInbound(buffer);
    Segment<ByteBuf> segment = channel.readInbound();
    assertThat(segment.isSelfContained).isTrue();
    assertThat(segment.payload).isEqualTo(LZ4_PAYLOAD_UNCOMPRESSED);
  }

  @Test
  public void should_surface_header_crc_mismatch() {
    try {
      channel.pipeline().addLast(newDecoder(Compressor.none()));
      channel.writeInbound(
          Unpooled.wrappedBuffer(REGULAR_WRONG_HEADER, REGULAR_PAYLOAD, REGULAR_TRAILER));
      fail("Expected a " + DecoderException.class.getSimpleName());
    } catch (DecoderException exception) {
      assertThat(exception).hasCauseInstanceOf(CrcMismatchException.class);
    }
  }

  @Test
  public void should_surface_trailer_crc_mismatch() {
    try {
      channel.pipeline().addLast(newDecoder(Compressor.none()));
      channel.writeInbound(
          Unpooled.wrappedBuffer(REGULAR_HEADER, REGULAR_PAYLOAD, REGULAR_WRONG_TRAILER));
      fail("Expected a " + DecoderException.class.getSimpleName());
    } catch (DecoderException exception) {
      assertThat(exception).hasCauseInstanceOf(CrcMismatchException.class);
    }
  }

  private BytesToSegmentDecoder newDecoder(Compressor<ByteBuf> compressor) {
    return new BytesToSegmentDecoder(
        new SegmentCodec<>(
            new ByteBufPrimitiveCodec(UnpooledByteBufAllocator.DEFAULT), compressor));
  }

  private static ByteBuf byteBuf(String hex) {
    return Unpooled.unreleasableBuffer(
        Unpooled.wrappedBuffer(ByteBufUtil.decodeHexDump(hex)).asReadOnly());
  }
}
