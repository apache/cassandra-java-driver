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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.datastax.oss.driver.api.core.connection.FrameTooLongException;
import com.datastax.oss.driver.internal.core.channel.ChannelHandlerTestBase;
import com.datastax.oss.protocol.internal.Compressor;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.FrameCodec;
import com.datastax.oss.protocol.internal.ProtocolV5ClientCodecs;
import com.datastax.oss.protocol.internal.request.Query;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.codec.EncoderException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class FrameEncoderTest extends ChannelHandlerTestBase {
  static final int MAX_FRAME_LENGTH = 128;

  private TestByteBufPrimitiveCodec byteBufPrimitiveCodec;
  private FrameCodec<ByteBuf> frameCodec;
  private FrameEncoder encoder;

  @Before
  @Override
  public void setup() {
    super.setup();

    byteBufPrimitiveCodec = new TestByteBufPrimitiveCodec(channel.alloc());
    frameCodec =
        new FrameCodec<>(byteBufPrimitiveCodec, Compressor.none(), new ProtocolV5ClientCodecs());
    encoder = new FrameEncoder(frameCodec, MAX_FRAME_LENGTH);

    channel.pipeline().addFirst(encoder);
  }

  @After
  public void after() {
    // check any bytebufs allocated by FrameCodec have been released
    for (ByteBuf byteBuf : byteBufPrimitiveCodec.allocated) {
      assertThat(byteBuf.refCnt()).isEqualTo(0);
    }
  }

  @Test
  public void should_encode_valid_frame() {
    Query shortQuery = new Query("select * from system.local");
    Frame frame = Frame.forRequest(5, 1, false, Collections.emptyMap(), shortQuery);
    channel.writeOutbound(frame);
    // get outbound bytebuf and release it
    readOutboundT(ByteBuf.class).release();
  }

  @Test
  public void should_fail_frame_too_long() {
    Query longQuery = new Query(RandomStringUtils.randomAlphabetic(MAX_FRAME_LENGTH));
    Frame frame = Frame.forRequest(5, 1, false, Collections.emptyMap(), longQuery);
    try {
      channel.writeOutbound(frame);
      fail("Should not be able to write long frame");
    } catch (Exception e) {
      assertThat(e)
          .isInstanceOf(EncoderException.class)
          .getCause()
          .isInstanceOf(FrameTooLongException.class)
          .hasMessageContaining("Outgoing frame length exceeds 128");
    }

    // check nothing was written to the channel
    assertNoOutboundFrame();
  }

  private static class TestByteBufPrimitiveCodec extends ByteBufPrimitiveCodec {

    private List<ByteBuf> allocated = new ArrayList<>();

    public TestByteBufPrimitiveCodec(ByteBufAllocator allocator) {
      super(allocator);
    }

    @Override
    public ByteBuf allocate(int size) {
      ByteBuf buf = super.allocate(size);
      allocated.add(buf);
      return buf;
    }

    @Override
    public void release(ByteBuf toRelease) {
      super.release(toRelease);
    }
  }
}
