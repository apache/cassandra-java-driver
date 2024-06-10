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

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Before;
import org.junit.Test;

public class SliceWriteListenerTest {

  private final EmbeddedChannel channel = new EmbeddedChannel();

  private ChannelPromise framePromise, slicePromise1, slicePromise2, slicePromise3;

  @Before
  public void setup() {
    framePromise = channel.newPromise();
    slicePromise1 = channel.newPromise();
    slicePromise2 = channel.newPromise();
    slicePromise3 = channel.newPromise();

    ByteBufSegmentBuilder.SliceWriteListener listener =
        new ByteBufSegmentBuilder.SliceWriteListener(
            framePromise, ImmutableList.of(slicePromise1, slicePromise2, slicePromise3));
    slicePromise1.addListener(listener);
    slicePromise2.addListener(listener);
    slicePromise3.addListener(listener);

    assertThat(framePromise.isDone()).isFalse();
  }

  @Test
  public void should_succeed_frame_if_all_slices_succeed() {
    slicePromise1.setSuccess();
    assertThat(framePromise.isDone()).isFalse();
    slicePromise2.setSuccess();
    assertThat(framePromise.isDone()).isFalse();
    slicePromise3.setSuccess();

    assertThat(framePromise.isSuccess()).isTrue();
  }

  @Test
  public void should_fail_frame_and_cancel_remaining_slices_if_one_slice_fails() {
    slicePromise1.setSuccess();
    assertThat(framePromise.isDone()).isFalse();
    Exception failure = new Exception("test");
    slicePromise2.setFailure(failure);

    assertThat(framePromise.isDone()).isTrue();
    assertThat(framePromise.isSuccess()).isFalse();
    assertThat(framePromise.cause()).isEqualTo(failure);

    assertThat(slicePromise3.isCancelled()).isTrue();
  }
}
