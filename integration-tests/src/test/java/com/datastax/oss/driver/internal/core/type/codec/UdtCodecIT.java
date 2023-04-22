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
package com.datastax.oss.driver.internal.core.type.codec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import java.util.Objects;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
public class UdtCodecIT {

  private CcmRule ccmRule = CcmRule.getInstance();

  private SessionRule<CqlSession> sessionRule = SessionRule.builder(ccmRule).build();

  @Rule public TestRule chain = RuleChain.outerRule(ccmRule).around(sessionRule);

  @Test
  public void should_decoding_udt_be_backward_compatible() {
    CqlSession session = sessionRule.session();
    session.execute("CREATE TYPE test_type_1 (a text, b int)");
    session.execute("CREATE TABLE test_table_1 (e int primary key, f frozen<test_type_1>)");
    // insert a row using version 1 of the UDT schema
    session.execute("INSERT INTO test_table_1(e, f) VALUES(1, {a: 'a', b: 1})");
    UserDefinedType udt =
        session
            .getMetadata()
            .getKeyspace(sessionRule.keyspace())
            .flatMap(ks -> ks.getUserDefinedType("test_type_1"))
            .orElseThrow(IllegalStateException::new);
    TypeCodec<?> oldCodec = session.getContext().getCodecRegistry().codecFor(udt);
    // update UDT schema
    session.execute("ALTER TYPE test_type_1 add i text");
    // insert a row using version 2 of the UDT schema
    session.execute("INSERT INTO test_table_1(e, f) VALUES(2, {a: 'b', b: 2, i: 'b'})");
    Row row =
        Objects.requireNonNull(session.execute("SELECT f FROM test_table_1 WHERE e = ?", 2).one());
    // Try to read new row with old codec. Using row.getUdtValue() would not cause any issues,
    // because new codec will be automatically registered (using all 3 attributes).
    // If application leverages generic row.get(String, Codec) method, data reading with old codec
    // should
    // be backward-compatible.
    UdtValue value = Objects.requireNonNull((UdtValue) row.get("f", oldCodec));
    assertThat(value.getString("a")).isEqualTo("b");
    assertThat(value.getInt("b")).isEqualTo(2);
    assertThatThrownBy(() -> value.getString("i")).hasMessage("i is not a field in this UDT");
  }
}
