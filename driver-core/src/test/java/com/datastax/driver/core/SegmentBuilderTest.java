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
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class SegmentBuilderTest {

  private static final Message.ProtocolEncoder REQUEST_ENCODER =
      new Message.ProtocolEncoder(ProtocolVersion.V5);

  // The constant names denote the total encoded size, including the frame header
  private static final Message.Request _38B_REQUEST = new Requests.Query("SELECT * FROM table");
  private static final Message.Request _51B_REQUEST =
      new Requests.Query("SELECT * FROM table WHERE id = 1");
  private static final Message.Request _1KB_REQUEST =
      new Requests.Query(
          "SELECT * FROM table WHERE id = ?",
          new Requests.QueryProtocolOptions(
              Message.Request.Type.QUERY,
              ConsistencyLevel.ONE,
              new ByteBuffer[] {ByteBuffer.allocate(967)},
              Collections.<String, ByteBuffer>emptyMap(),
              false,
              -1,
              null,
              ConsistencyLevel.SERIAL,
              Long.MIN_VALUE,
              Integer.MIN_VALUE),
          false);

  private static final EmbeddedChannel MOCK_CHANNEL = new EmbeddedChannel();
  private static final ChannelHandlerContext CONTEXT = Mockito.mock(ChannelHandlerContext.class);

  @BeforeClass(groups = "unit")
  public static void setup() {
    // This is the only method called by our test implementation
    when(CONTEXT.newPromise())
        .thenAnswer(
            new Answer<ChannelPromise>() {
              @Override
              public ChannelPromise answer(InvocationOnMock invocation) {
                return MOCK_CHANNEL.newPromise();
              }
            });
  }

  @Test(groups = "unit")
  public void should_concatenate_frames_when_under_limit() {
    TestSegmentBuilder builder = new TestSegmentBuilder(CONTEXT, 100);

    ChannelPromise requestPromise1 = newPromise();
    builder.addRequest(_38B_REQUEST, requestPromise1);
    ChannelPromise requestPromise2 = newPromise();
    builder.addRequest(_51B_REQUEST, requestPromise2);
    // Nothing produced yet since we would still have room for more frames
    assertThat(builder.segments).isEmpty();

    builder.flush();
    assertThat(builder.segments).hasSize(1);
    assertThat(builder.segmentPromises).hasSize(1);
    Segment segment = builder.segments.get(0);
    assertThat(segment.getPayload().readableBytes()).isEqualTo(38 + 51);
    assertThat(segment.isSelfContained()).isTrue();
    ChannelPromise segmentPromise = builder.segmentPromises.get(0);
    assertForwards(segmentPromise, requestPromise1, requestPromise2);
  }

  @Test(groups = "unit")
  public void should_start_new_segment_when_over_limit() {
    TestSegmentBuilder builder = new TestSegmentBuilder(CONTEXT, 100);

    ChannelPromise requestPromise1 = newPromise();
    builder.addRequest(_38B_REQUEST, requestPromise1);
    ChannelPromise requestPromise2 = newPromise();
    builder.addRequest(_51B_REQUEST, requestPromise2);
    ChannelPromise requestPromise3 = newPromise();
    builder.addRequest(_38B_REQUEST, requestPromise3);
    // Adding the 3rd frame brings the total size over 100, so a first segment should be emitted
    // with the first two messages:
    assertThat(builder.segments).hasSize(1);

    ChannelPromise requestPromise4 = newPromise();
    builder.addRequest(_38B_REQUEST, requestPromise4);
    builder.flush();
    assertThat(builder.segments).hasSize(2);

    Segment segment1 = builder.segments.get(0);
    assertThat(segment1.getPayload().readableBytes()).isEqualTo(38 + 51);
    assertThat(segment1.isSelfContained()).isTrue();
    ChannelPromise segmentPromise1 = builder.segmentPromises.get(0);
    assertForwards(segmentPromise1, requestPromise1, requestPromise2);
    Segment segment2 = builder.segments.get(1);
    assertThat(segment2.getPayload().readableBytes()).isEqualTo(38 + 38);
    assertThat(segment2.isSelfContained()).isTrue();
    ChannelPromise segmentPromise2 = builder.segmentPromises.get(1);
    assertForwards(segmentPromise2, requestPromise3, requestPromise4);
  }

  @Test(groups = "unit")
  public void should_start_new_segment_when_at_limit() {
    TestSegmentBuilder builder = new TestSegmentBuilder(CONTEXT, 38 + 51);

    ChannelPromise requestPromise1 = newPromise();
    builder.addRequest(_38B_REQUEST, requestPromise1);
    ChannelPromise requestPromise2 = newPromise();
    builder.addRequest(_51B_REQUEST, requestPromise2);
    ChannelPromise requestPromise3 = newPromise();
    builder.addRequest(_38B_REQUEST, requestPromise3);
    assertThat(builder.segments).hasSize(1);

    ChannelPromise requestPromise4 = newPromise();
    builder.addRequest(_51B_REQUEST, requestPromise4);
    builder.flush();
    assertThat(builder.segments).hasSize(2);

    Segment segment1 = builder.segments.get(0);
    assertThat(segment1.getPayload().readableBytes()).isEqualTo(38 + 51);
    assertThat(segment1.isSelfContained()).isTrue();
    ChannelPromise segmentPromise1 = builder.segmentPromises.get(0);
    assertForwards(segmentPromise1, requestPromise1, requestPromise2);
    Segment segment2 = builder.segments.get(1);
    assertThat(segment2.getPayload().readableBytes()).isEqualTo(38 + 51);
    assertThat(segment2.isSelfContained()).isTrue();
    ChannelPromise segmentPromise2 = builder.segmentPromises.get(1);
    assertForwards(segmentPromise2, requestPromise3, requestPromise4);
  }

  @Test(groups = "unit")
  public void should_split_large_frame() {
    TestSegmentBuilder builder = new TestSegmentBuilder(CONTEXT, 100);

    ChannelPromise parentPromise = newPromise();
    builder.addRequest(_1KB_REQUEST, parentPromise);

    assertThat(builder.segments).hasSize(11);
    assertThat(builder.segmentPromises).hasSize(11);
    for (int i = 0; i < 11; i++) {
      Segment slice = builder.segments.get(i);
      assertThat(slice.getPayload().readableBytes()).isEqualTo(i == 10 ? 24 : 100);
      assertThat(slice.isSelfContained()).isFalse();
    }
  }

  @Test(groups = "unit")
  public void should_succeed_parent_write_if_all_slices_successful() {
    TestSegmentBuilder builder = new TestSegmentBuilder(CONTEXT, 100);

    ChannelPromise parentPromise = newPromise();
    builder.addRequest(_1KB_REQUEST, parentPromise);

    assertThat(builder.segments).hasSize(11);
    assertThat(builder.segmentPromises).hasSize(11);

    for (int i = 0; i < 11; i++) {
      assertThat(parentPromise.isDone()).isFalse();
      builder.segmentPromises.get(i).setSuccess();
    }

    assertThat(parentPromise.isDone()).isTrue();
  }

  @Test(groups = "unit")
  public void should_fail_parent_write_if_any_slice_fails() {
    TestSegmentBuilder builder = new TestSegmentBuilder(CONTEXT, 100);

    ChannelPromise parentPromise = newPromise();
    builder.addRequest(_1KB_REQUEST, parentPromise);

    assertThat(builder.segments).hasSize(11);

    // Complete a few slices successfully
    for (int i = 0; i < 5; i++) {
      builder.segmentPromises.get(i).setSuccess();
    }
    assertThat(parentPromise.isDone()).isFalse();

    // Fail a slice, the parent should fail immediately
    Exception mockException = new Exception("test");
    builder.segmentPromises.get(5).setFailure(mockException);
    assertThat(parentPromise.isDone()).isTrue();
    assertThat(parentPromise.cause()).isEqualTo(mockException);

    // The remaining slices should have been cancelled
    for (int i = 6; i < 11; i++) {
      assertThat(builder.segmentPromises.get(i).isCancelled()).isTrue();
    }
  }

  @Test(groups = "unit")
  public void should_split_large_frame_when_exact_multiple() {
    TestSegmentBuilder builder = new TestSegmentBuilder(CONTEXT, 256);

    ChannelPromise parentPromise = newPromise();
    builder.addRequest(_1KB_REQUEST, parentPromise);

    assertThat(builder.segments).hasSize(4);
    assertThat(builder.segmentPromises).hasSize(4);
    for (int i = 0; i < 4; i++) {
      Segment slice = builder.segments.get(i);
      assertThat(slice.getPayload().readableBytes()).isEqualTo(256);
      assertThat(slice.isSelfContained()).isFalse();
    }
  }

  @Test(groups = "unit")
  public void should_mix_small_frames_and_large_frames() {
    TestSegmentBuilder builder = new TestSegmentBuilder(CONTEXT, 100);

    ChannelPromise requestPromise1 = newPromise();
    builder.addRequest(_38B_REQUEST, requestPromise1);
    ChannelPromise requestPromise2 = newPromise();
    builder.addRequest(_51B_REQUEST, requestPromise2);

    // Large frame: process immediately, does not impact accumulated small frames
    ChannelPromise requestPromise3 = newPromise();
    builder.addRequest(_1KB_REQUEST, requestPromise3);
    assertThat(builder.segments).hasSize(11);

    // Another small frames bring us above the limit
    ChannelPromise requestPromise4 = newPromise();
    builder.addRequest(_38B_REQUEST, requestPromise4);
    assertThat(builder.segments).hasSize(12);

    // One last frame and finish
    ChannelPromise requestPromise5 = newPromise();
    builder.addRequest(_38B_REQUEST, requestPromise5);
    builder.flush();
    assertThat(builder.segments).hasSize(13);
    assertThat(builder.segmentPromises).hasSize(13);

    for (int i = 0; i < 11; i++) {
      Segment slice = builder.segments.get(i);
      assertThat(slice.getPayload().readableBytes()).isEqualTo(i == 10 ? 24 : 100);
      assertThat(slice.isSelfContained()).isFalse();
    }

    Segment smallMessages1 = builder.segments.get(11);
    assertThat(smallMessages1.getPayload().readableBytes()).isEqualTo(38 + 51);
    assertThat(smallMessages1.isSelfContained()).isTrue();
    ChannelPromise segmentPromise1 = builder.segmentPromises.get(11);
    assertForwards(segmentPromise1, requestPromise1, requestPromise2);
    Segment smallMessages2 = builder.segments.get(12);
    assertThat(smallMessages2.getPayload().readableBytes()).isEqualTo(38 + 38);
    assertThat(smallMessages2.isSelfContained()).isTrue();
    ChannelPromise segmentPromise2 = builder.segmentPromises.get(12);
    assertForwards(segmentPromise2, requestPromise4, requestPromise5);
  }

  private static ChannelPromise newPromise() {
    return MOCK_CHANNEL.newPromise();
  }

  private void assertForwards(ChannelPromise segmentPromise, ChannelPromise... requestPromises) {
    for (ChannelPromise requestPromise : requestPromises) {
      assertThat(requestPromise.isDone()).isFalse();
    }
    segmentPromise.setSuccess();
    for (ChannelPromise requestPromise : requestPromises) {
      assertThat(requestPromise.isSuccess()).isTrue();
    }
  }

  // Test implementation that simply stores segments and promises in the order they were produced.
  static class TestSegmentBuilder extends SegmentBuilder {

    List<Segment> segments = new ArrayList<Segment>();
    List<ChannelPromise> segmentPromises = new ArrayList<ChannelPromise>();

    TestSegmentBuilder(ChannelHandlerContext context, int maxPayloadLength) {
      super(context, ByteBufAllocator.DEFAULT, REQUEST_ENCODER, maxPayloadLength);
    }

    @Override
    protected void processSegment(Segment segment, ChannelPromise segmentPromise) {
      segments.add(segment);
      segmentPromises.add(segmentPromise);
    }
  }
}
