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

import static com.datastax.oss.driver.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.driver.shaded.guava.common.collect.ListMultimap;
import com.datastax.oss.driver.shaded.guava.common.collect.MultimapBuilder;
import com.datastax.oss.driver.shaded.guava.common.collect.Sets;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.internal.util.MockUtil;
import org.mockito.stubbing.OngoingStubbing;

/**
 * Helper class to set up and verify a sequence of invocations on a ChannelFactory mock.
 *
 * <p>Use the builder at the beginning of the test to stub expected calls. Then call the verify
 * methods throughout the test to check that each call has been performed.
 *
 * <p>This class handles asynchronous calls to the thread factory, but it must be used from a single
 * thread (see {@link #waitForCalls(Node, int)}).
 */
public class MockChannelFactoryHelper {

  private static final int CONNECT_TIMEOUT_MILLIS = 500;

  public static Builder builder(ChannelFactory channelFactory) {
    return new Builder(channelFactory);
  }

  private final ChannelFactory channelFactory;
  private final InOrder inOrder;
  // If waitForCalls sees more invocations than expected, the difference is stored here
  private final Map<Node, Integer> previous = new HashMap<>();

  public MockChannelFactoryHelper(ChannelFactory channelFactory) {
    this.channelFactory = channelFactory;
    this.inOrder = inOrder(channelFactory);
  }

  public void waitForCall(Node node) {
    waitForCalls(node, 1);
  }

  /**
   * Waits for a given number of calls to {@code ChannelFactory.connect()}.
   *
   * <p>Because we test asynchronous, non-blocking code, there might already be more calls than
   * expected when this method is called. If so, the extra calls are stored and stored and will be
   * taken into account next time.
   */
  public void waitForCalls(Node node, int expected) {
    int fromLastTime = previous.getOrDefault(node, 0);
    if (fromLastTime >= expected) {
      previous.put(node, fromLastTime - expected);
      return;
    }
    expected -= fromLastTime;

    // Because we test asynchronous, non-blocking code, there might have been already more
    // invocations than expected. Use `atLeast` and a captor to find out.
    ArgumentCaptor<DriverChannelOptions> optionsCaptor =
        ArgumentCaptor.forClass(DriverChannelOptions.class);
    inOrder
        .verify(channelFactory, timeout(CONNECT_TIMEOUT_MILLIS).atLeast(expected))
        .connect(eq(node), optionsCaptor.capture());
    int actual = optionsCaptor.getAllValues().size();

    int extras = actual - expected;
    if (extras > 0) {
      previous.compute(node, (k, v) -> (v == null) ? extras : v + extras);
    }
  }

  public void verifyNoMoreCalls() {
    inOrder
        .verify(channelFactory, timeout(CONNECT_TIMEOUT_MILLIS).times(0))
        .connect(any(Node.class), any(DriverChannelOptions.class));

    Set<Integer> counts = Sets.newHashSet(previous.values());
    if (!counts.isEmpty()) {
      assertThat(counts).containsExactly(0);
    }
  }

  public static class Builder {
    private final ChannelFactory channelFactory;
    private final ListMultimap<Node, Object> invocations =
        MultimapBuilder.hashKeys().arrayListValues().build();

    public Builder(ChannelFactory channelFactory) {
      assertThat(MockUtil.isMock(channelFactory)).as("expected a mock").isTrue();
      verifyZeroInteractions(channelFactory);
      this.channelFactory = channelFactory;
    }

    public Builder success(Node node, DriverChannel channel) {
      invocations.put(node, channel);
      return this;
    }

    public Builder failure(Node node, String error) {
      invocations.put(node, new Exception(error));
      return this;
    }

    public Builder failure(Node node, Throwable error) {
      invocations.put(node, error);
      return this;
    }

    public Builder pending(Node node, CompletableFuture<DriverChannel> future) {
      invocations.put(node, future);
      return this;
    }

    public MockChannelFactoryHelper build() {
      stub();
      return new MockChannelFactoryHelper(channelFactory);
    }

    private void stub() {
      for (Node node : invocations.keySet()) {
        Deque<CompletionStage<DriverChannel>> results = new ArrayDeque<>();
        for (Object object : invocations.get(node)) {
          if (object instanceof DriverChannel) {
            results.add(CompletableFuture.completedFuture(((DriverChannel) object)));
          } else if (object instanceof Throwable) {
            results.add(CompletableFutures.failedFuture(((Throwable) object)));
          } else if (object instanceof CompletableFuture) {
            @SuppressWarnings("unchecked")
            CompletionStage<DriverChannel> future = (CompletionStage<DriverChannel>) object;
            results.add(future);
          } else {
            fail("unexpected type: " + object.getClass());
          }
        }
        if (results.size() > 0) {
          CompletionStage<DriverChannel> first = results.poll();
          OngoingStubbing<CompletionStage<DriverChannel>> ongoingStubbing =
              when(channelFactory.connect(eq(node), any(DriverChannelOptions.class)))
                  .thenReturn(first);
          for (CompletionStage<DriverChannel> result : results) {
            ongoingStubbing.thenReturn(result);
          }
        }
      }
    }
  }
}
