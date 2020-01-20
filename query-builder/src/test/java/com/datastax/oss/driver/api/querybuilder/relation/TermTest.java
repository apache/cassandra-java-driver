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
package com.datastax.oss.driver.api.querybuilder.relation;

import static com.datastax.oss.driver.api.querybuilder.Assertions.assertThat;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.add;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.currentDate;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.currentTime;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.currentTimeUuid;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.currentTimestamp;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.function;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.maxTimeUuid;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.minTimeUuid;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.multiply;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.negate;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.now;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.raw;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.remainder;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.subtract;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.toDate;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.toTimestamp;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.toUnixTimestamp;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.typeHint;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException;
import com.datastax.oss.driver.api.querybuilder.CharsetCodec;
import com.datastax.oss.driver.internal.core.type.UserDefinedTypeBuilder;
import com.datastax.oss.driver.shaded.guava.common.base.Charsets;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import java.util.Date;
import org.junit.Test;

public class TermTest {

  @Test
  public void should_generate_arithmetic_terms() {
    assertThat(add(raw("a"), raw("b"))).hasCql("a+b");
    assertThat(add(add(raw("a"), raw("b")), add(raw("c"), raw("d")))).hasCql("a+b+c+d");
    assertThat(subtract(add(raw("a"), raw("b")), add(raw("c"), raw("d")))).hasCql("a+b-(c+d)");
    assertThat(subtract(add(raw("a"), raw("b")), subtract(raw("c"), raw("d")))).hasCql("a+b-(c-d)");
    assertThat(negate(add(raw("a"), raw("b")))).hasCql("-(a+b)");
    assertThat(negate(subtract(raw("a"), raw("b")))).hasCql("-(a-b)");
    assertThat(multiply(add(raw("a"), raw("b")), add(raw("c"), raw("d")))).hasCql("(a+b)*(c+d)");
    assertThat(remainder(multiply(raw("a"), raw("b")), multiply(raw("c"), raw("d"))))
        .hasCql("a*b%(c*d)");
    assertThat(remainder(multiply(raw("a"), raw("b")), remainder(raw("c"), raw("d"))))
        .hasCql("a*b%(c%d)");
  }

  @Test
  public void should_generate_function_terms() {
    assertThat(function("f")).hasCql("f()");
    assertThat(function("f", raw("a"), raw("b"))).hasCql("f(a,b)");
    assertThat(function("ks", "f", raw("a"), raw("b"))).hasCql("ks.f(a,b)");
    assertThat(now()).hasCql("now()");
    assertThat(currentTimestamp()).hasCql("currenttimestamp()");
    assertThat(currentDate()).hasCql("currentdate()");
    assertThat(currentTime()).hasCql("currenttime()");
    assertThat(currentTimeUuid()).hasCql("currenttimeuuid()");
    assertThat(minTimeUuid(raw("a"))).hasCql("mintimeuuid(a)");
    assertThat(maxTimeUuid(raw("a"))).hasCql("maxtimeuuid(a)");
    assertThat(toDate(raw("a"))).hasCql("todate(a)");
    assertThat(toTimestamp(raw("a"))).hasCql("totimestamp(a)");
    assertThat(toUnixTimestamp(raw("a"))).hasCql("tounixtimestamp(a)");
  }

  @Test
  public void should_generate_type_hint_terms() {
    assertThat(typeHint(raw("1"), DataTypes.BIGINT)).hasCql("(bigint)1");
  }

  @Test
  public void should_generate_literal_terms() {
    assertThat(literal(1)).hasCql("1");
    assertThat(literal("foo")).hasCql("'foo'");
    assertThat(literal(ImmutableList.of(1, 2, 3))).hasCql("[1,2,3]");

    TupleType tupleType = DataTypes.tupleOf(DataTypes.INT, DataTypes.TEXT);
    TupleValue tupleValue = tupleType.newValue().setInt(0, 1).setString(1, "foo");
    assertThat(literal(tupleValue)).hasCql("(1,'foo')");

    UserDefinedType udtType =
        new UserDefinedTypeBuilder(CqlIdentifier.fromCql("ks"), CqlIdentifier.fromCql("user"))
            .withField(CqlIdentifier.fromCql("first_name"), DataTypes.TEXT)
            .withField(CqlIdentifier.fromCql("last_name"), DataTypes.TEXT)
            .build();
    UdtValue udtValue =
        udtType.newValue().setString("first_name", "Jane").setString("last_name", "Doe");
    assertThat(literal(udtValue)).hasCql("{first_name:'Jane',last_name:'Doe'}");
    assertThat(literal(null)).hasCql("NULL");

    assertThat(literal(Charsets.UTF_8, new CharsetCodec())).hasCql("'UTF-8'");
    assertThat(literal(Charsets.UTF_8, CharsetCodec.TEST_REGISTRY)).hasCql("'UTF-8'");
  }

  @Test
  public void should_fail_when_no_codec_for_literal() {
    assertThatThrownBy(() -> literal(new Date(1234)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Could not inline literal of type java.util.Date. "
                + "This happens because the driver doesn't know how to map it to a CQL type. "
                + "Try passing a TypeCodec or CodecRegistry to literal().")
        .hasCauseInstanceOf(CodecNotFoundException.class);
  }
}
