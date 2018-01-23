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
package com.datastax.oss.driver.api.core.type;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.internal.core.type.UserDefinedTypeBuilder;
import org.junit.Test;

public class UserDefinedTypeTest {

  private static final UserDefinedType ADDRESS_TYPE =
      new UserDefinedTypeBuilder(
              CqlIdentifier.fromInternal("test"), CqlIdentifier.fromInternal("address"))
          // Not actually used in this test, but UDTs must have fields:
          .withField(CqlIdentifier.fromInternal("street"), DataTypes.TEXT)
          .frozen()
          .build();
  private static final UserDefinedType ACCOUNT_TYPE =
      new UserDefinedTypeBuilder(
              CqlIdentifier.fromInternal("test"), CqlIdentifier.fromInternal("account"))
          .withField(CqlIdentifier.fromInternal("ID"), DataTypes.TEXT) // case-sensitive
          .withField(CqlIdentifier.fromInternal("name"), DataTypes.TEXT)
          .withField(CqlIdentifier.fromInternal("address"), ADDRESS_TYPE)
          .withField(
              CqlIdentifier.fromInternal("frozen_list"), DataTypes.frozenListOf(DataTypes.TEXT))
          .withField(
              CqlIdentifier.fromInternal("list_of_map"),
              DataTypes.listOf(DataTypes.frozenMapOf(DataTypes.TEXT, DataTypes.INT)))
          .build();

  @Test
  public void should_describe_as_cql() {
    assertThat(ACCOUNT_TYPE.describe(false))
        .isEqualTo(
            "CREATE TYPE \"test\".\"account\" ( \"ID\" text, \"name\" text, \"address\" frozen<\"test\".\"address\">, \"frozen_list\" frozen<list<text>>, \"list_of_map\" list<frozen<map<text, int>>> );");
  }

  @Test
  public void should_describe_as_pretty_cql() {
    assertThat(ACCOUNT_TYPE.describe(true))
        .isEqualTo(
            "CREATE TYPE test.account (\n"
                + "    \"ID\" text,\n"
                + "    name text,\n"
                + "    address frozen<test.address>,\n"
                + "    frozen_list frozen<list<text>>,\n"
                + "    list_of_map list<frozen<map<text, int>>>\n"
                + ");");
  }

  @Test
  public void should_evaluate_equality() {
    assertThat(ACCOUNT_TYPE.newValue()).isEqualTo(ACCOUNT_TYPE.newValue());
  }
}
