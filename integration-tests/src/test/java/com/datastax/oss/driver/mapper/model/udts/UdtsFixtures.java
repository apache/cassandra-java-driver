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
package com.datastax.oss.driver.mapper.model.udts;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.data.GettableByName;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.mapper.model.EntityFixture;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class UdtsFixtures {

  private static final GenericType<Map<String, List<UdtValue>>> MAP1_GENERIC_TYPE =
      new GenericType<Map<String, List<UdtValue>>>() {};
  private static final GenericType<Map<UdtValue, Set<List<UdtValue>>>> MAP2_GENERIC_TYPE =
      new GenericType<Map<UdtValue, Set<List<UdtValue>>>>() {};
  private static final GenericType<Map<UdtValue, Map<String, Set<UdtValue>>>> MAP3_GENERIC_TYPE =
      new GenericType<Map<UdtValue, Map<String, Set<UdtValue>>>>() {};

  public static List<String> createStatements() {
    return ImmutableList.of(
        "CREATE TYPE type1(s text)",
        "CREATE TYPE type2(i int)",
        "CREATE TABLE container(id uuid PRIMARY KEY, "
            + "list frozen<list<type1>>, "
            + "map1 frozen<map<text, list<type1>>>, "
            + "map2 frozen<map<type1, set<list<type2>>>>,"
            + "map3 frozen<map<type1, map<text, set<type2>>>>"
            + ")");
  }

  public static EntityFixture<Container> SAMPLE_CONTAINER =
      new EntityFixture<Container>(
          new Container(
              UUID.randomUUID(),
              ImmutableList.of(new Type1("a"), new Type1("b")),
              ImmutableMap.of(
                  "cd",
                  ImmutableList.of(new Type1("c"), new Type1("d")),
                  "ef",
                  ImmutableList.of(new Type1("e"), new Type1("f"))),
              ImmutableMap.of(
                  new Type1("12"),
                  ImmutableSet.of(ImmutableList.of(new Type2(1)), ImmutableList.of(new Type2(2)))),
              ImmutableMap.of(
                  new Type1("12"),
                  ImmutableMap.of("12", ImmutableSet.of(new Type2(1), new Type2(2)))))) {

        @Override
        public void assertMatches(GettableByName data) {
          assertThat(data.getUuid("id")).isEqualTo(entity.getId());

          List<UdtValue> list = data.getList("list", UdtValue.class);
          assertThat(list).extracting(udt -> udt.getString("s")).containsOnly("a", "b");

          Map<String, List<UdtValue>> map1 = data.get("map1", MAP1_GENERIC_TYPE);
          assertThat(map1).containsOnlyKeys("cd", "ef");
          assertThat(map1.get("cd")).extracting(udt -> udt.getString("s")).containsOnly("c", "d");
          assertThat(map1.get("ef")).extracting(udt -> udt.getString("s")).containsOnly("e", "f");

          Map<UdtValue, Set<List<UdtValue>>> map2 = data.get("map2", MAP2_GENERIC_TYPE);
          assertThat(map2).hasSize(1);
          UdtValue key2 = map2.keySet().iterator().next();
          assertThat(key2.getString("s")).isEqualTo("12");
          Set<List<UdtValue>> set2 = map2.get(key2);
          assertThat(set2)
              .extracting(
                  l -> {
                    assertThat(l).hasSize(1);
                    return l.get(0).getInt("i");
                  })
              .containsOnly(1, 2);

          Map<UdtValue, Map<String, Set<UdtValue>>> map3 = data.get("map3", MAP3_GENERIC_TYPE);
          assertThat(map3).hasSize(1);
          UdtValue key3 = map3.keySet().iterator().next();
          assertThat(key3.getString("s")).isEqualTo("12");
          Map<String, Set<UdtValue>> map33 = map3.get(key3);
          assertThat(map33).hasSize(1);
          Set<UdtValue> set3 = map33.get("12");
          assertThat(set3).extracting(v -> v.getInt("i")).containsOnly(1, 2);
        }
      };
}
