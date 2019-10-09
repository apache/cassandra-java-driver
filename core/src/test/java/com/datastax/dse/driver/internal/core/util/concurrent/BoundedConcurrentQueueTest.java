/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
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
