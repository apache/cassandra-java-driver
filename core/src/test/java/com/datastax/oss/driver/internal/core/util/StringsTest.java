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

import com.datastax.oss.driver.TestDataProviders;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.util.Locale;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class StringsTest {

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "locales")
  public void should_report_cql_keyword(Locale locale) {
    Locale def = Locale.getDefault();
    try {
      Locale.setDefault(locale);

      assertThat(Strings.isReservedCqlKeyword(null)).isFalse();
      assertThat(Strings.isReservedCqlKeyword("NOT A RESERVED KEYWORD")).isFalse();

      assertThat(Strings.isReservedCqlKeyword("add")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("allow")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("alter")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("and")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("apply")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("asc")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("authorize")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("batch")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("begin")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("by")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("columnfamily")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("create")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("default")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("delete")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("desc")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("describe")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("drop")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("entries")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("execute")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("from")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("full")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("grant")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("if")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("in")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("index")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("infinity")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("insert")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("into")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("is")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("keyspace")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("limit")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("materialized")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("mbean")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("mbeans")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("modify")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("nan")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("norecursive")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("not")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("null")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("of")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("on")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("or")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("order")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("primary")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("rename")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("replace")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("revoke")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("schema")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("select")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("set")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("table")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("to")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("token")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("truncate")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("unlogged")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("unset")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("update")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("use")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("using")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("view")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("where")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("with")).isTrue();

      assertThat(Strings.isReservedCqlKeyword("ALLOW")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("ALTER")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("AND")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("APPLY")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("ASC")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("AUTHORIZE")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("BATCH")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("BEGIN")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("BY")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("COLUMNFAMILY")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("CREATE")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("DEFAULT")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("DELETE")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("DESC")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("DESCRIBE")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("DROP")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("ENTRIES")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("EXECUTE")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("FROM")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("FULL")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("GRANT")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("IF")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("IN")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("INDEX")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("INFINITY")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("INSERT")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("INTO")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("IS")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("KEYSPACE")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("LIMIT")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("MATERIALIZED")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("MBEAN")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("MBEANS")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("MODIFY")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("NAN")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("NORECURSIVE")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("NOT")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("NULL")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("OF")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("ON")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("OR")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("ORDER")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("PRIMARY")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("RENAME")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("REPLACE")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("REVOKE")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("SCHEMA")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("SELECT")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("SET")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("TABLE")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("TO")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("TOKEN")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("TRUNCATE")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("UNLOGGED")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("UNSET")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("UPDATE")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("USE")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("USING")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("VIEW")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("WHERE")).isTrue();
      assertThat(Strings.isReservedCqlKeyword("WITH")).isTrue();
    } finally {
      Locale.setDefault(def);
    }
  }
}
