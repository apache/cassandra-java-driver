/*
 * Copyright (C) 2017-2017 DataStax Inc.
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
package com.datastax.oss.driver.internal.core.cql;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.channel.ResponseCallback;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.Message;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.concurrent.Promise;
import java.util.concurrent.CompletableFuture;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.never;

/**
 * The simulated behavior of the connection pool for a given node in a {@link
 * RequestHandlerTestHarness}.
 *
 * <p>This only covers a single attempt, if the node is to be tried multiple times there will be
 * multiple instances of this class.
 */
public class PoolBehavior {

  final Node node;
  final DriverChannel channel;
  private final Promise<Void> writePromise;
  private final CompletableFuture<ResponseCallback> callbackFuture = new CompletableFuture<>();

  public PoolBehavior(Node node, boolean createChannel) {
    this.node = node;
    if (!createChannel) {
      this.channel = null;
      this.writePromise = null;
    } else {
      this.channel = Mockito.mock(DriverChannel.class);
      this.writePromise = GlobalEventExecutor.INSTANCE.newPromise();
      Mockito.when(
              channel.write(
                  any(Message.class), anyBoolean(), anyMap(), any(ResponseCallback.class)))
          .thenAnswer(
              invocation -> {
                callbackFuture.complete(invocation.getArgument(3));
                return writePromise;
              });
      ChannelFuture closeFuture = Mockito.mock(ChannelFuture.class);
      Mockito.when(channel.closeFuture()).thenReturn(closeFuture);
    }
  }

  public void verifyWrite() {
    Mockito.verify(channel)
        .write(any(Message.class), anyBoolean(), anyMap(), any(ResponseCallback.class));
  }

  public void verifyNoWrite() {
    Mockito.verify(channel, never())
        .write(any(Message.class), anyBoolean(), anyMap(), any(ResponseCallback.class));
  }

  public void setWriteSuccess() {
    writePromise.setSuccess(null);
  }

  public void setWriteFailure(Throwable cause) {
    writePromise.setFailure(cause);
  }

  public void setResponseSuccess(Frame responseFrame) {
    callbackFuture.thenAccept(callback -> callback.onResponse(responseFrame));
  }

  public void setResponseFailure(Throwable cause) {
    callbackFuture.thenAccept(callback -> callback.onFailure(cause));
  }

  /** Mocks a follow-up request on the same channel. */
  public void mockFollowupRequest(Class<? extends Message> expectedMessage, Frame responseFrame) {
    Promise<Void> writePromise2 = GlobalEventExecutor.INSTANCE.newPromise();
    CompletableFuture<ResponseCallback> callbackFuture2 = new CompletableFuture<>();
    Mockito.when(
            channel.write(
                any(expectedMessage), anyBoolean(), anyMap(), any(ResponseCallback.class)))
        .thenAnswer(
            invocation -> {
              callbackFuture2.complete(invocation.getArgument(3));
              return writePromise2;
            });
    writePromise2.setSuccess(null);
    callbackFuture2.thenAccept(callback -> callback.onResponse(responseFrame));
  }

  public void verifyCancellation() {
    Mockito.verify(channel).cancel(any(ResponseCallback.class));
  }
}
