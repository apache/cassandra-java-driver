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

import com.datastax.oss.driver.TestDataProviders;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public abstract class OffsetPagerTestBase {

  /**
   * The fetch size only matters for the async implementation. For sync this will essentially run
   * the same fixture 4 times, but that's not a problem because tests are fast.
   */
  @DataProvider
  public static Object[][] fetchSizes() {
    return TestDataProviders.fromList(1, 2, 3, 100);
  }

  @DataProvider
  public static Object[][] scenarios() {
    Object[][] fixtures =
        TestDataProviders.fromList(
            // ------- inputs -------- | ------ expected -------
            // iterable  | page | size | page | contents | last?
            "a,b,c,d,e,f | 1    | 3    | 1    | a,b,c    | false",
            "a,b,c,d,e,f | 2    | 3    | 2    | d,e,f    | true",
            "a,b,c,d,e,f | 2    | 4    | 2    | e,f      | true",
            "a,b,c,d,e,f | 2    | 5    | 2    | f        | true",
            "a,b,c       | 1    | 3    | 1    | a,b,c    | true",
            "a,b         | 1    | 3    | 1    | a,b      | true",
            "a           | 1    | 3    | 1    | a        | true",
            // Empty iterator => return one empty page
            "            | 1    | 3    | 1    |          | true",
            // Past the end => return last page
            "a,b,c,d,e,f | 3    | 3    | 2    | d,e,f    | true",
            "a,b,c,d,e   | 3    | 3    | 2    | d,e      | true");
    return TestDataProviders.combine(fixtures, fetchSizes());
  }

  @Test
  @UseDataProvider("scenarios")
  public void should_return_existing_page(String fixtureSpec, int fetchSize) {
    OffsetPagerTestFixture fixture = new OffsetPagerTestFixture(fixtureSpec);
    OffsetPager pager = new OffsetPager(fixture.getPageSize());
    OffsetPager.Page<String> actualPage = getActualPage(pager, fixture, fetchSize);
    fixture.assertMatches(actualPage);
  }

  protected abstract OffsetPager.Page<String> getActualPage(
      OffsetPager pager, OffsetPagerTestFixture fixture, int fetchSize);
}
