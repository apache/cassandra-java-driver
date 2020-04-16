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
package com.datastax.oss.driver.api.core.paging;

import static com.datastax.oss.driver.Assertions.assertThat;

import com.datastax.oss.driver.api.core.paging.OffsetPager.Page;
import com.datastax.oss.driver.internal.core.MockAsyncPagingIterable;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.util.concurrent.CompletionStage;
import org.junit.Test;

public class OffsetPagerAsyncTest extends OffsetPagerTestBase {

  @Override
  protected Page<String> getActualPage(
      OffsetPager pager, OffsetPagerTestFixture fixture, int fetchSize) {
    CompletionStage<Page<String>> pageFuture =
        pager.getPage(fixture.getAsyncIterable(fetchSize), fixture.getRequestedPage());
    return CompletableFutures.getCompleted(pageFuture);
  }

  /**
   * Covers the corner case where the server sends back an empty frame at the end of the result set.
   */
  @Test
  @UseDataProvider("fetchSizes")
  public void should_return_last_page_when_result_finishes_with_empty_frame(int fetchSize) {
    MockAsyncPagingIterable<String> iterable =
        new MockAsyncPagingIterable<>(ImmutableList.of("a", "b", "c"), fetchSize, true);
    OffsetPager pager = new OffsetPager(3);
    Page<String> page = CompletableFutures.getCompleted(pager.getPage(iterable, 1));

    assertThat(page.getElements()).containsExactly("a", "b", "c");
    assertThat(page.getPageNumber()).isEqualTo(1);
    assertThat(page.isLast()).isTrue();
  }
}
