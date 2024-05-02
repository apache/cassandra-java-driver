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
package com.datastax.oss.driver.api.core.paging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.internal.core.MockAsyncPagingIterable;
import com.datastax.oss.driver.internal.core.MockPagingIterable;
import com.datastax.oss.driver.shaded.guava.common.base.Splitter;
import java.util.List;

public class OffsetPagerTestFixture {

  private static final Splitter SPEC_SPLITTER = Splitter.on('|').trimResults();
  private static final Splitter LIST_SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

  private final List<String> inputElements;
  private final int requestedPage;
  private final int pageSize;
  private final int expectedPageNumber;
  private final List<String> expectedElements;
  private final boolean expectedIsLast;

  public OffsetPagerTestFixture(String spec) {
    List<String> components = SPEC_SPLITTER.splitToList(spec);
    int size = components.size();
    if (size != 3 && size != 6) {
      fail("Invalid fixture spec, expected 3 or 5 components");
    }

    this.inputElements = LIST_SPLITTER.splitToList(components.get(0));
    this.requestedPage = Integer.parseInt(components.get(1));
    this.pageSize = Integer.parseInt(components.get(2));
    if (size == 3) {
      this.expectedPageNumber = -1;
      this.expectedElements = null;
      this.expectedIsLast = false;
    } else {
      this.expectedPageNumber = Integer.parseInt(components.get(3));
      this.expectedElements = LIST_SPLITTER.splitToList(components.get(4));
      this.expectedIsLast = Boolean.parseBoolean(components.get(5));
    }
  }

  public PagingIterable<String> getSyncIterable() {
    return new MockPagingIterable<>(inputElements.iterator());
  }

  public MockAsyncPagingIterable<String> getAsyncIterable(int fetchSize) {
    return new MockAsyncPagingIterable<>(inputElements, fetchSize, false);
  }

  public int getRequestedPage() {
    return requestedPage;
  }

  public int getPageSize() {
    return pageSize;
  }

  public void assertMatches(OffsetPager.Page<String> actualPage) {
    assertThat(actualPage.getPageNumber()).isEqualTo(expectedPageNumber);
    assertThat(actualPage.getElements()).isEqualTo(expectedElements);
    assertThat(actualPage.isLast()).isEqualTo(expectedIsLast);
  }
}
