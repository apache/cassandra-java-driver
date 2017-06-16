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
package com.datastax.oss.driver.internal.core.session;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.pool.ChannelPool;
import com.datastax.oss.driver.internal.core.pool.ChannelPoolFactory;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.mockito.internal.util.MockUtil;
import org.mockito.stubbing.OngoingStubbing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.timeout;

public class MockChannelPoolFactoryHelper {

  public static MockChannelPoolFactoryHelper.Builder builder(
      ChannelPoolFactory channelPoolFactory) {
    return new MockChannelPoolFactoryHelper.Builder(channelPoolFactory);
  }

  private final ChannelPoolFactory channelPoolFactory;
  private final InOrder inOrder;
  // If waitForCalls sees more invocations than expected, the difference is stored here
  private final Map<Params, Integer> previous = new HashMap<>();

  private MockChannelPoolFactoryHelper(ChannelPoolFactory channelPoolFactory) {
    this.channelPoolFactory = channelPoolFactory;
    this.inOrder = Mockito.inOrder(channelPoolFactory);
  }

  public void waitForCall(Node node, CqlIdentifier keyspace, NodeDistance distance) {
    waitForCalls(node, keyspace, distance, 1);
  }

  /**
   * Waits for a given number of calls to {@code ChannelPoolFactory.init()}.
   *
   * <p>Because we test asynchronous, non-blocking code, there might already be more calls than
   * expected when this method is called. If so, the extra calls are stored and stored and will be
   * taken into account next time.
   */
  public void waitForCalls(Node node, CqlIdentifier keyspace, NodeDistance distance, int expected) {
    Params params = new Params(node, keyspace, distance);
    int fromLastTime = previous.getOrDefault(params, 0);
    if (fromLastTime >= expected) {
      previous.put(params, fromLastTime - expected);
      return;
    }
    expected -= fromLastTime;

    // Because we test asynchronous, non-blocking code, there might have been already more
    // invocations than expected. Use `atLeast` and a captor to find out.
    ArgumentCaptor<InternalDriverContext> contextCaptor =
        ArgumentCaptor.forClass(InternalDriverContext.class);
    inOrder
        .verify(channelPoolFactory, timeout(100).atLeast(expected))
        .init(eq(node), eq(keyspace), eq(distance), contextCaptor.capture(), eq("test"));
    int actual = contextCaptor.getAllValues().size();

    int extras = actual - expected;
    if (extras > 0) {
      previous.compute(params, (k, v) -> (v == null) ? extras : v + extras);
    }
  }

  public static class Builder {
    private final ChannelPoolFactory channelPoolFactory;
    private final ListMultimap<Params, Object> invocations =
        MultimapBuilder.hashKeys().arrayListValues().build();

    private Builder(ChannelPoolFactory channelPoolFactory) {
      assertThat(MockUtil.isMock(channelPoolFactory)).isTrue().as("expected a mock");
      Mockito.verifyZeroInteractions(channelPoolFactory);
      this.channelPoolFactory = channelPoolFactory;
    }

    public Builder success(
        Node node, CqlIdentifier keyspaceName, NodeDistance distance, ChannelPool pool) {
      invocations.put(new Params(node, keyspaceName, distance), pool);
      return this;
    }

    public Builder failure(
        Node node, CqlIdentifier keyspaceName, NodeDistance distance, String error) {
      invocations.put(new Params(node, keyspaceName, distance), new Exception(error));
      return this;
    }

    public Builder failure(
        Node node, CqlIdentifier keyspaceName, NodeDistance distance, Throwable error) {
      invocations.put(new Params(node, keyspaceName, distance), error);
      return this;
    }

    public Builder pending(
        Node node,
        CqlIdentifier keyspaceName,
        NodeDistance distance,
        CompletionStage<ChannelPool> future) {
      invocations.put(new Params(node, keyspaceName, distance), future);
      return this;
    }

    public MockChannelPoolFactoryHelper build() {
      stub();
      return new MockChannelPoolFactoryHelper(channelPoolFactory);
    }

    private void stub() {
      for (Params params : invocations.keySet()) {
        LinkedList<CompletionStage<ChannelPool>> results = new LinkedList<>();
        for (Object object : invocations.get(params)) {
          if (object instanceof ChannelPool) {
            results.add(CompletableFuture.completedFuture(((ChannelPool) object)));
          } else if (object instanceof Throwable) {
            results.add(CompletableFutures.failedFuture(((Throwable) object)));
          } else if (object instanceof CompletableFuture) {
            @SuppressWarnings("unchecked")
            CompletionStage<ChannelPool> future = (CompletionStage<ChannelPool>) object;
            results.add(future);
          } else {
            fail("unexpected type: " + object.getClass());
          }
        }
        if (results.size() > 0) {
          CompletionStage<ChannelPool> first = results.poll();
          OngoingStubbing<CompletionStage<ChannelPool>> ongoingStubbing =
              Mockito.when(
                      channelPoolFactory.init(
                          eq(params.node),
                          eq(params.keyspace),
                          eq(params.distance),
                          any(InternalDriverContext.class),
                          eq("test")))
                  .thenReturn(first);
          for (CompletionStage<ChannelPool> result : results) {
            ongoingStubbing.thenReturn(result);
          }
        }
      }
    }
  }

  private static class Params {
    private final Node node;
    private final CqlIdentifier keyspace;
    private final NodeDistance distance;

    private Params(Node node, CqlIdentifier keyspace, NodeDistance distance) {
      this.node = node;
      this.keyspace = keyspace;
      this.distance = distance;
    }

    @Override
    public boolean equals(Object other) {
      if (other == this) {
        return true;
      } else if (other instanceof Params) {
        Params that = (Params) other;
        return Objects.equals(this.node, that.node)
            && Objects.equals(this.keyspace, that.keyspace)
            && Objects.equals(this.distance, that.distance);
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Objects.hash(node, keyspace, distance);
    }
  }
}
