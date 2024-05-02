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
package com.datastax.oss.driver.internal.core.channel;

import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.connection.BusyConnectionException;
import com.datastax.oss.driver.internal.core.util.ProtocolUtils;
import com.datastax.oss.driver.internal.core.util.concurrent.UncaughtExceptions;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.response.Error;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import net.jcip.annotations.NotThreadSafe;

/** Common infrastructure to send a native protocol request from a channel handler. */
@NotThreadSafe // must be confined to the channel's event loop
abstract class ChannelHandlerRequest implements ResponseCallback {

  final Channel channel;
  final ChannelHandlerContext ctx;
  final InFlightHandler inFlightHandler;
  private final long timeoutMillis;

  private ScheduledFuture<?> timeoutFuture;

  ChannelHandlerRequest(ChannelHandlerContext ctx, long timeoutMillis) {
    this.ctx = ctx;
    this.channel = ctx.channel();
    this.inFlightHandler = ctx.pipeline().get(InFlightHandler.class);
    assert inFlightHandler != null;
    this.timeoutMillis = timeoutMillis;
  }

  abstract String describe();

  abstract Message getRequest();

  abstract void onResponse(Message response);

  /** either message or cause can be null */
  abstract void fail(String message, Throwable cause);

  void fail(Throwable cause) {
    fail(null, cause);
  }

  void send() {
    assert channel.eventLoop().inEventLoop();
    if (!inFlightHandler.preAcquireId()) {
      fail(
          new BusyConnectionException(
              String.format(
                  "%s has reached its maximum number of simultaneous requests", channel)));
    } else {
      DriverChannel.RequestMessage message =
          new DriverChannel.RequestMessage(getRequest(), false, Frame.NO_PAYLOAD, this);
      ChannelFuture writeFuture = channel.writeAndFlush(message);
      writeFuture.addListener(this::writeListener);
    }
  }

  private void writeListener(Future<? super Void> writeFuture) {
    if (writeFuture.isSuccess()) {
      timeoutFuture =
          channel.eventLoop().schedule(this::onTimeout, timeoutMillis, TimeUnit.MILLISECONDS);
    } else {
      String message =
          String.format("%s: failed to send request (%s)", describe(), writeFuture.cause());
      fail(message, writeFuture.cause());
    }
  }

  @Override
  public final void onResponse(Frame responseFrame) {
    timeoutFuture.cancel(true);
    onResponse(responseFrame.message);
  }

  @Override
  public final void onFailure(Throwable error) {
    // timeoutFuture may not have been assigned if write failed.
    if (timeoutFuture != null) {
      timeoutFuture.cancel(true);
    }
    String message = String.format("%s: unexpected failure (%s)", describe(), error);
    fail(message, error);
  }

  private void onTimeout() {
    fail(new DriverTimeoutException(describe() + ": timed out after " + timeoutMillis + " ms"));
    if (!channel.closeFuture().isDone()) {
      // Cancel the response callback
      channel.writeAndFlush(this).addListener(UncaughtExceptions::log);
    }
  }

  void failOnUnexpected(Message response) {
    if (response instanceof Error) {
      Error error = (Error) response;
      fail(
          new IllegalArgumentException(
              String.format(
                  "%s: server replied with unexpected error code [%s]: %s",
                  describe(), ProtocolUtils.errorCodeString(error.code), error.message)));
    } else {
      fail(
          new IllegalArgumentException(
              String.format(
                  "%s: server replied with unexpected response type (opcode=%s)",
                  describe(), ProtocolUtils.opcodeString(response.opcode))));
    }
  }
}
