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
package com.datastax.dse.driver.internal.core.util.concurrent;

import static com.datastax.oss.driver.Assertions.assertThatStage;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CompletionStage;
import org.junit.Test;

public class BoundedConcurrentQueueTest {

  @Test
  public void should_dequeue_null_when_empty() {
    BoundedConcurrentQueue<Integer> queue = new BoundedConcurrentQueue<>(4);
    assertThat(queue.peek()).isNull();
    assertThat(queue.poll()).isNull();
  }

  @Test
  public void should_enqueue_and_dequeue_while_not_full() {
    BoundedConcurrentQueue<Integer> queue = new BoundedConcurrentQueue<>(4);

    assertThatStage(queue.offer(1)).isSuccess(e -> assertThat(e).isEqualTo(1));
    assertThat(queue.peek()).isEqualTo(1);
    assertThat(queue.poll()).isEqualTo(1);

    assertThatStage(queue.offer(2)).isSuccess(e -> assertThat(e).isEqualTo(2));
    assertThatStage(queue.offer(3)).isSuccess(e -> assertThat(e).isEqualTo(3));
    assertThatStage(queue.offer(4)).isSuccess(e -> assertThat(e).isEqualTo(4));

    assertThat(queue.peek()).isEqualTo(2);
    assertThat(queue.poll()).isEqualTo(2);
    assertThat(queue.peek()).isEqualTo(3);
    assertThat(queue.poll()).isEqualTo(3);
    assertThat(queue.peek()).isEqualTo(4);
    assertThat(queue.poll()).isEqualTo(4);
    assertThat(queue.poll()).isNull();
  }

  @Test
  public void should_delay_insertion_when_full_until_space_available() {
    BoundedConcurrentQueue<Integer> queue = new BoundedConcurrentQueue<>(4);

    assertThatStage(queue.offer(1)).isSuccess(e -> assertThat(e).isEqualTo(1));
    assertThatStage(queue.offer(2)).isSuccess(e -> assertThat(e).isEqualTo(2));
    assertThatStage(queue.offer(3)).isSuccess(e -> assertThat(e).isEqualTo(3));
    assertThatStage(queue.offer(4)).isSuccess(e -> assertThat(e).isEqualTo(4));

    CompletionStage<Integer> enqueue5 = queue.offer(5);
    assertThat(enqueue5).isNotDone();

    assertThat(queue.poll()).isEqualTo(1);
    assertThatStage(enqueue5).isSuccess(e -> assertThat(e).isEqualTo(5));
  }

  @Test(expected = IllegalStateException.class)
  public void should_fail_to_insert_when_other_insert_already_pending() {
    BoundedConcurrentQueue<Integer> queue = new BoundedConcurrentQueue<>(1);
    assertThatStage(queue.offer(1)).isSuccess();
    assertThatStage(queue.offer(2)).isNotDone();
    queue.offer(3);
  }
}
