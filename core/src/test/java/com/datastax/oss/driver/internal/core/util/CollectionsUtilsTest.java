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
package com.datastax.oss.driver.internal.core.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class CollectionsUtilsTest {
  @Test
  @UseDataProvider("listsProvider")
  public void should_combine_two_lists_by_index(
      List<Integer> firstList, List<Integer> secondList, Map<Integer, Integer> expected) {

    // when
    Map<Integer, Integer> result =
        CollectionsUtils.combineListsIntoOrderedMap(firstList, secondList);

    // then
    assertThat(result).isEqualTo(expected);
  }

  @Test
  public void should_throw_if_lists_have_not_matching_size() {
    // given
    List<Integer> list1 = ImmutableList.of(1);
    List<Integer> list2 = ImmutableList.of(1, 2);

    // when
    assertThatThrownBy(() -> CollectionsUtils.combineListsIntoOrderedMap(list1, list2))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageMatching("Cannot combine lists with not matching sizes");
  }

  @DataProvider
  public static Object[][] listsProvider() {

    return new Object[][] {
      {ImmutableList.of(1), ImmutableList.of(1), ImmutableMap.of(1, 1)},
      {ImmutableList.of(1, 10, 5), ImmutableList.of(1, 10, 5), ImmutableMap.of(1, 1, 10, 10, 5, 5)},
      {ImmutableList.of(1, 1), ImmutableList.of(1, 2), ImmutableMap.of(1, 2)}
    };
  }
}
