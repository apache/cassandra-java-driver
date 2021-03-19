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
package com.datastax.oss.driver.api.querybuilder.update;

import static com.datastax.oss.driver.api.querybuilder.Assertions.assertThat;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.update;

import com.datastax.oss.driver.api.querybuilder.Literal;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import org.junit.Test;

public class UpdateFluentAssignmentTest {

  @Test
  public void should_generate_simple_column_assignment() {
    assertThat(update("foo").setColumn("v", bindMarker()).whereColumn("k").isEqualTo(bindMarker()))
        .hasCql("UPDATE foo SET v=? WHERE k=?");
    assertThat(
            update("ks", "foo")
                .setColumn("v", bindMarker())
                .whereColumn("k")
                .isEqualTo(bindMarker()))
        .hasCql("UPDATE ks.foo SET v=? WHERE k=?");
  }

  @Test
  public void should_generate_field_assignment() {
    assertThat(
            update("foo")
                .setField("address", "street", bindMarker())
                .whereColumn("k")
                .isEqualTo(bindMarker()))
        .hasCql("UPDATE foo SET address.street=? WHERE k=?");
  }

  @Test
  public void should_generate_map_value_assignment() {
    assertThat(
            update("foo")
                .setMapValue("features", literal("color"), bindMarker())
                .whereColumn("k")
                .isEqualTo(bindMarker()))
        .hasCql("UPDATE foo SET features['color']=? WHERE k=?");
  }

  @Test
  public void should_generate_list_value_assignment() {
    assertThat(
            update("foo")
                .setListValue("features", literal(1), bindMarker())
                .whereColumn("k")
                .isEqualTo(bindMarker()))
        .hasCql("UPDATE foo SET features[1]=? WHERE k=?");
  }

  @Test
  public void should_generate_counter_operations() {
    assertThat(update("foo").increment("c").whereColumn("k").isEqualTo(bindMarker()))
        .hasCql("UPDATE foo SET c=c+1 WHERE k=?");
    assertThat(update("foo").increment("c", literal(2)).whereColumn("k").isEqualTo(bindMarker()))
        .hasCql("UPDATE foo SET c=c+2 WHERE k=?");
    assertThat(update("foo").increment("c", bindMarker()).whereColumn("k").isEqualTo(bindMarker()))
        .hasCql("UPDATE foo SET c=c+? WHERE k=?");

    assertThat(update("foo").decrement("c").whereColumn("k").isEqualTo(bindMarker()))
        .hasCql("UPDATE foo SET c=c-1 WHERE k=?");
    assertThat(update("foo").decrement("c", literal(2)).whereColumn("k").isEqualTo(bindMarker()))
        .hasCql("UPDATE foo SET c=c-2 WHERE k=?");
    assertThat(update("foo").decrement("c", bindMarker()).whereColumn("k").isEqualTo(bindMarker()))
        .hasCql("UPDATE foo SET c=c-? WHERE k=?");
  }

  @Test
  public void should_generate_list_operations() {
    Literal listLiteral = literal(ImmutableList.of(1, 2, 3));

    assertThat(update("foo").append("l", bindMarker()).whereColumn("k").isEqualTo(bindMarker()))
        .hasCql("UPDATE foo SET l=l+? WHERE k=?");
    assertThat(update("foo").append("l", listLiteral).whereColumn("k").isEqualTo(bindMarker()))
        .hasCql("UPDATE foo SET l=l+[1,2,3] WHERE k=?");
    assertThat(
            update("foo")
                .appendListElement("l", bindMarker())
                .whereColumn("k")
                .isEqualTo(bindMarker()))
        .hasCql("UPDATE foo SET l=l+[?] WHERE k=?");

    assertThat(update("foo").prepend("l", bindMarker()).whereColumn("k").isEqualTo(bindMarker()))
        .hasCql("UPDATE foo SET l=?+l WHERE k=?");
    assertThat(update("foo").prepend("l", listLiteral).whereColumn("k").isEqualTo(bindMarker()))
        .hasCql("UPDATE foo SET l=[1,2,3]+l WHERE k=?");
    assertThat(
            update("foo")
                .prependListElement("l", bindMarker())
                .whereColumn("k")
                .isEqualTo(bindMarker()))
        .hasCql("UPDATE foo SET l=[?]+l WHERE k=?");

    assertThat(update("foo").remove("l", bindMarker()).whereColumn("k").isEqualTo(bindMarker()))
        .hasCql("UPDATE foo SET l=l-? WHERE k=?");
    assertThat(update("foo").remove("l", listLiteral).whereColumn("k").isEqualTo(bindMarker()))
        .hasCql("UPDATE foo SET l=l-[1,2,3] WHERE k=?");
    assertThat(
            update("foo")
                .removeListElement("l", bindMarker())
                .whereColumn("k")
                .isEqualTo(bindMarker()))
        .hasCql("UPDATE foo SET l=l-[?] WHERE k=?");
  }

  @Test
  public void should_generate_set_operations() {
    Literal setLiteral = literal(ImmutableSet.of(1, 2, 3));

    assertThat(update("foo").append("s", bindMarker()).whereColumn("k").isEqualTo(bindMarker()))
        .hasCql("UPDATE foo SET s=s+? WHERE k=?");
    assertThat(update("foo").append("s", setLiteral).whereColumn("k").isEqualTo(bindMarker()))
        .hasCql("UPDATE foo SET s=s+{1,2,3} WHERE k=?");
    assertThat(
            update("foo")
                .appendSetElement("s", bindMarker())
                .whereColumn("k")
                .isEqualTo(bindMarker()))
        .hasCql("UPDATE foo SET s=s+{?} WHERE k=?");

    assertThat(update("foo").prepend("s", bindMarker()).whereColumn("k").isEqualTo(bindMarker()))
        .hasCql("UPDATE foo SET s=?+s WHERE k=?");
    assertThat(update("foo").prepend("s", setLiteral).whereColumn("k").isEqualTo(bindMarker()))
        .hasCql("UPDATE foo SET s={1,2,3}+s WHERE k=?");
    assertThat(
            update("foo")
                .prependSetElement("s", bindMarker())
                .whereColumn("k")
                .isEqualTo(bindMarker()))
        .hasCql("UPDATE foo SET s={?}+s WHERE k=?");

    assertThat(update("foo").remove("s", bindMarker()).whereColumn("k").isEqualTo(bindMarker()))
        .hasCql("UPDATE foo SET s=s-? WHERE k=?");
    assertThat(update("foo").remove("s", setLiteral).whereColumn("k").isEqualTo(bindMarker()))
        .hasCql("UPDATE foo SET s=s-{1,2,3} WHERE k=?");
    assertThat(
            update("foo")
                .removeSetElement("s", bindMarker())
                .whereColumn("k")
                .isEqualTo(bindMarker()))
        .hasCql("UPDATE foo SET s=s-{?} WHERE k=?");
  }

  @Test
  public void should_generate_map_operations() {
    Literal mapLiteral = literal(ImmutableMap.of(1, "foo", 2, "bar"));

    assertThat(update("foo").append("m", bindMarker()).whereColumn("k").isEqualTo(bindMarker()))
        .hasCql("UPDATE foo SET m=m+? WHERE k=?");
    assertThat(update("foo").append("m", mapLiteral).whereColumn("k").isEqualTo(bindMarker()))
        .hasCql("UPDATE foo SET m=m+{1:'foo',2:'bar'} WHERE k=?");
    assertThat(
            update("foo")
                .appendMapEntry("m", literal(1), literal("foo"))
                .whereColumn("k")
                .isEqualTo(bindMarker()))
        .hasCql("UPDATE foo SET m=m+{1:'foo'} WHERE k=?");

    assertThat(update("foo").prepend("m", bindMarker()).whereColumn("k").isEqualTo(bindMarker()))
        .hasCql("UPDATE foo SET m=?+m WHERE k=?");
    assertThat(update("foo").prepend("m", mapLiteral).whereColumn("k").isEqualTo(bindMarker()))
        .hasCql("UPDATE foo SET m={1:'foo',2:'bar'}+m WHERE k=?");
    assertThat(
            update("foo")
                .prependMapEntry("m", literal(1), literal("foo"))
                .whereColumn("k")
                .isEqualTo(bindMarker()))
        .hasCql("UPDATE foo SET m={1:'foo'}+m WHERE k=?");

    assertThat(update("foo").remove("m", bindMarker()).whereColumn("k").isEqualTo(bindMarker()))
        .hasCql("UPDATE foo SET m=m-? WHERE k=?");
    assertThat(update("foo").remove("m", mapLiteral).whereColumn("k").isEqualTo(bindMarker()))
        .hasCql("UPDATE foo SET m=m-{1:'foo',2:'bar'} WHERE k=?");
    assertThat(
            update("foo")
                .removeMapEntry("m", literal(1), literal("foo"))
                .whereColumn("k")
                .isEqualTo(bindMarker()))
        .hasCql("UPDATE foo SET m=m-{1:'foo'} WHERE k=?");
  }
}
