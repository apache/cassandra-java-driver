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
package com.datastax.oss.driver.internal.core.cql;

import static java.util.stream.StreamSupport.stream;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.internal.core.MockPagingIterable;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.util.ArrayList;
import java.util.List;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class PagingIterableSpliteratorTest {

  @Test
  @UseDataProvider("splitsWithEstimatedSize")
  public void should_split_with_estimated_size(
      int size, int chunkSize, List<Integer> expectedLeft, List<Integer> expectedRight) {
    // given
    PagingIterableSpliterator.Builder<Integer> builder =
        PagingIterableSpliterator.builder(iterableOfSize(size))
            .withEstimatedSize(size)
            .withChunkSize(chunkSize);
    // when
    PagingIterableSpliterator<Integer> right = builder.build();
    Spliterator<Integer> left = right.trySplit();
    // then
    assertThat(right.characteristics())
        .isEqualTo(
            Spliterator.ORDERED
                | Spliterator.IMMUTABLE
                | Spliterator.NONNULL
                | Spliterator.SIZED
                | Spliterator.SUBSIZED);
    assertThat(right.estimateSize()).isEqualTo(expectedRight.size());
    assertThat(right.getExactSizeIfKnown()).isEqualTo(expectedRight.size());
    TestConsumer rightConsumer = new TestConsumer();
    right.forEachRemaining(rightConsumer);
    assertThat(rightConsumer.items).containsExactlyElementsOf(expectedRight);
    if (expectedLeft.isEmpty()) {
      assertThat(left).isNull();
    } else {
      assertThat(left.characteristics())
          .isEqualTo(
              Spliterator.ORDERED
                  | Spliterator.IMMUTABLE
                  | Spliterator.NONNULL
                  | Spliterator.SIZED
                  | Spliterator.SUBSIZED);
      assertThat(left.estimateSize()).isEqualTo(expectedLeft.size());
      assertThat(left.getExactSizeIfKnown()).isEqualTo(expectedLeft.size());
      TestConsumer leftConsumer = new TestConsumer();
      left.forEachRemaining(leftConsumer);
      assertThat(leftConsumer.items).containsExactlyElementsOf(expectedLeft);
    }
  }

  @DataProvider
  public static Iterable<?> splitsWithEstimatedSize() {
    List<List<Object>> arguments = new ArrayList<>();
    arguments.add(Lists.newArrayList(0, 1, ImmutableList.of(), ImmutableList.of()));
    arguments.add(Lists.newArrayList(1, 1, ImmutableList.of(), ImmutableList.of(0)));
    arguments.add(Lists.newArrayList(1, 2, ImmutableList.of(), ImmutableList.of(0)));
    arguments.add(Lists.newArrayList(2, 1, ImmutableList.of(0), ImmutableList.of(1)));
    arguments.add(
        Lists.newArrayList(
            10, 1, ImmutableList.of(0), ImmutableList.of(1, 2, 3, 4, 5, 6, 7, 8, 9)));
    arguments.add(
        Lists.newArrayList(
            10, 5, ImmutableList.of(0, 1, 2, 3, 4), ImmutableList.of(5, 6, 7, 8, 9)));
    arguments.add(
        Lists.newArrayList(
            10, 9, ImmutableList.of(0, 1, 2, 3, 4, 5, 6, 7, 8), ImmutableList.of(9)));
    arguments.add(
        Lists.newArrayList(
            10, 10, ImmutableList.of(), ImmutableList.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)));
    return arguments;
  }

  @Test
  @UseDataProvider("splitsWithUnknownSize")
  public void should_split_with_unknown_size(
      int size, int chunkSize, List<Integer> expectedLeft, List<Integer> expectedRight) {
    // given
    PagingIterableSpliterator.Builder<Integer> builder =
        PagingIterableSpliterator.builder(iterableOfSize(size)).withChunkSize(chunkSize);
    // when
    PagingIterableSpliterator<Integer> right = builder.build();
    Spliterator<Integer> left = right.trySplit();
    // then
    assertThat(right.characteristics())
        .isEqualTo(Spliterator.ORDERED | Spliterator.IMMUTABLE | Spliterator.NONNULL);
    assertThat(right.estimateSize()).isEqualTo(Long.MAX_VALUE);
    assertThat(right.getExactSizeIfKnown()).isEqualTo(-1);
    TestConsumer rightConsumer = new TestConsumer();
    right.forEachRemaining(rightConsumer);
    assertThat(rightConsumer.items).containsExactlyElementsOf(expectedRight);
    if (expectedLeft.isEmpty()) {
      assertThat(left).isNull();
    } else {
      // left side will also be SIZED and SUBSIZED
      assertThat(left.characteristics())
          .isEqualTo(
              Spliterator.ORDERED
                  | Spliterator.IMMUTABLE
                  | Spliterator.NONNULL
                  | Spliterator.SIZED
                  | Spliterator.SUBSIZED);
      assertThat(left.estimateSize()).isEqualTo(expectedLeft.size());
      assertThat(left.getExactSizeIfKnown()).isEqualTo(expectedLeft.size());
      TestConsumer leftConsumer = new TestConsumer();
      left.forEachRemaining(leftConsumer);
      assertThat(leftConsumer.items).containsExactlyElementsOf(expectedLeft);
    }
  }

  @DataProvider
  public static Iterable<?> splitsWithUnknownSize() {
    List<List<Object>> arguments = new ArrayList<>();
    arguments.add(Lists.newArrayList(0, 1, ImmutableList.of(), ImmutableList.of()));
    arguments.add(Lists.newArrayList(1, 1, ImmutableList.of(0), ImmutableList.of()));
    arguments.add(Lists.newArrayList(1, 2, ImmutableList.of(0), ImmutableList.of()));
    arguments.add(Lists.newArrayList(2, 1, ImmutableList.of(0), ImmutableList.of(1)));
    arguments.add(
        Lists.newArrayList(
            10, 1, ImmutableList.of(0), ImmutableList.of(1, 2, 3, 4, 5, 6, 7, 8, 9)));
    arguments.add(
        Lists.newArrayList(
            10, 5, ImmutableList.of(0, 1, 2, 3, 4), ImmutableList.of(5, 6, 7, 8, 9)));
    arguments.add(
        Lists.newArrayList(
            10, 9, ImmutableList.of(0, 1, 2, 3, 4, 5, 6, 7, 8), ImmutableList.of(9)));
    arguments.add(
        Lists.newArrayList(
            10, 10, ImmutableList.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), ImmutableList.of()));
    return arguments;
  }

  @Test
  public void should_consume_with_tryAdvance() {
    // given
    PagingIterableSpliterator<Integer> spliterator =
        new PagingIterableSpliterator<>(iterableOfSize(10));
    TestConsumer action = new TestConsumer();
    // when
    for (int i = 0; i < 20; i++) {
      spliterator.tryAdvance(action);
    }
    // then
    assertThat(action.items).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
  }

  @Test
  public void should_consume_with_forEachRemaining() {
    // given
    PagingIterableSpliterator<Integer> spliterator =
        new PagingIterableSpliterator<>(iterableOfSize(10));
    TestConsumer action = new TestConsumer();
    // when
    spliterator.forEachRemaining(action);
    // then
    assertThat(action.items).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
  }

  @Test
  @UseDataProvider("streams")
  public void should_consume_stream(int size, int chunkSize, boolean parallel) {
    // given
    PagingIterableSpliterator<Integer> spliterator =
        PagingIterableSpliterator.builder(iterableOfSize(size))
            .withEstimatedSize(size)
            .withChunkSize(chunkSize)
            .build();
    // when
    long count = stream(spliterator, parallel).count();
    // then
    assertThat(count).isEqualTo(size);
  }

  @DataProvider
  public static Iterable<?> streams() {
    List<List<Object>> arguments = new ArrayList<>();
    arguments.add(Lists.newArrayList(10_000, 5_000, false));
    arguments.add(Lists.newArrayList(10_000, 1_000, false));
    arguments.add(Lists.newArrayList(10_000, 9_999, false));
    arguments.add(Lists.newArrayList(10_000, 1, false));
    arguments.add(Lists.newArrayList(10_000, 5_000, true));
    arguments.add(Lists.newArrayList(10_000, 1_000, true));
    arguments.add(Lists.newArrayList(10_000, 9_999, true));
    arguments.add(Lists.newArrayList(10_000, 1, true));
    return arguments;
  }

  private static MockPagingIterable<Integer> iterableOfSize(int size) {
    return new MockPagingIterable<>(
        IntStream.range(0, size).boxed().collect(Collectors.toList()).iterator());
  }

  private static class TestConsumer implements Consumer<Integer> {

    private final List<Integer> items = new ArrayList<>();

    @Override
    public void accept(Integer integer) {
      items.add(integer);
    }
  }
}
