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
import static com.datastax.oss.driver.api.querybuilder.QueryBuilderDsl.add;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilderDsl.currentDate;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilderDsl.currentTime;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilderDsl.currentTimeUuid;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilderDsl.currentTimestamp;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilderDsl.function;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilderDsl.literal;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilderDsl.maxTimeUuid;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilderDsl.minTimeUuid;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilderDsl.multiply;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilderDsl.negate;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilderDsl.now;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilderDsl.raw;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilderDsl.remainder;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilderDsl.subtract;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilderDsl.toDate;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilderDsl.toTimestamp;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilderDsl.toUnixTimestamp;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilderDsl.typeHint;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException;
import com.datastax.oss.driver.api.querybuilder.CharsetCodec;
import com.datastax.oss.driver.internal.core.type.UserDefinedTypeBuilder;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
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

  @Test(expected = CodecNotFoundException.class)
  public void should_fail_when_no_codec_for_literal() {
    literal(Charsets.UTF_8);
  }
}
