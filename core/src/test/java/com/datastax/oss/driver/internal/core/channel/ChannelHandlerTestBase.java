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
package com.datastax.oss.driver.internal.core.channel;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.Message;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.Collections;
import org.junit.Before;

/**
 * Infrastructure for channel handler test.
 *
 * <p>It relies on an embedded channel where the tested handler is installed. Then the test can
 * simulate incoming/outgoing messages, and check that the handler propagates the adequate messages
 * upstream/downstream.
 */
public class ChannelHandlerTestBase {
  protected EmbeddedChannel channel;

  @Before
  public void setup() {
    channel = new EmbeddedChannel();
  }

  /** Reads a request frame that we expect the tested handler to have sent inbound. */
  protected Frame readInboundFrame() {
    channel.runPendingTasks();
    Object o = channel.readInbound();
    assertThat(o).isInstanceOf(Frame.class);
    return ((Frame) o);
  }

  /** Reads a request frame that we expect the tested handler to have sent outbound. */
  protected Frame readOutboundFrame() {
    channel.runPendingTasks();
    Object o = channel.readOutbound();
    assertThat(o).isInstanceOf(Frame.class);
    return ((Frame) o);
  }

  protected void assertNoOutboundFrame() {
    channel.runPendingTasks();
    Object o = channel.readOutbound();
    assertThat(o).isNull();
  }

  /** Writes a response frame for the tested handler to read. */
  protected void writeInboundFrame(Frame responseFrame) {
    channel.writeInbound(responseFrame);
  }

  /** Writes a response frame that matches the given request, with the given response message. */
  protected void writeInboundFrame(Frame requestFrame, Message response) {
    channel.writeInbound(buildInboundFrame(requestFrame, response));
  }

  /** Builds a response frame matching a request frame. */
  protected Frame buildInboundFrame(Frame requestFrame, Message response) {
    return Frame.forResponse(
        requestFrame.protocolVersion,
        requestFrame.streamId,
        null,
        requestFrame.customPayload,
        Collections.emptyList(),
        response);
  }
}
