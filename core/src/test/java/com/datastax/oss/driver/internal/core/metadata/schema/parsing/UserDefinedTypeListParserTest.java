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
package com.datastax.oss.driver.internal.core.metadata.schema.parsing;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import java.util.Map;
import org.junit.Test;

public class UserDefinedTypeListParserTest extends SchemaParserTestBase {

  private static final AdminRow PERSON_ROW_2_2 =
      mockTypeRow(
          "ks",
          "person",
          ImmutableList.of("first_name", "last_name", "address"),
          ImmutableList.of(
              "org.apache.cassandra.db.marshal.UTF8Type",
              "org.apache.cassandra.db.marshal.UTF8Type",
              "org.apache.cassandra.db.marshal.UserType("
                  + "ks,61646472657373," // address
                  + "737472656574:org.apache.cassandra.db.marshal.UTF8Type," // street
                  + "7a6970636f6465:org.apache.cassandra.db.marshal.Int32Type)")); // zipcode

  private static final AdminRow PERSON_ROW_3_0 =
      mockTypeRow(
          "ks",
          "person",
          ImmutableList.of("first_name", "last_name", "address"),
          ImmutableList.of("text", "text", "address"));

  private static final AdminRow ADDRESS_ROW_3_0 =
      mockTypeRow(
          "ks", "address", ImmutableList.of("street", "zipcode"), ImmutableList.of("text", "int"));

  @Test
  public void should_parse_modern_table() {
    UserDefinedTypeParser parser = new UserDefinedTypeParser(new DataTypeCqlNameParser(), context);
    Map<CqlIdentifier, UserDefinedType> types =
        parser.parse(KEYSPACE_ID, PERSON_ROW_3_0, ADDRESS_ROW_3_0);

    assertThat(types).hasSize(2);
    UserDefinedType personType = types.get(CqlIdentifier.fromInternal("person"));
    UserDefinedType addressType = types.get(CqlIdentifier.fromInternal("address"));

    assertThat(personType.getKeyspace().asInternal()).isEqualTo("ks");
    assertThat(personType.getName().asInternal()).isEqualTo("person");
    assertThat(personType.getFieldNames())
        .containsExactly(
            CqlIdentifier.fromInternal("first_name"),
            CqlIdentifier.fromInternal("last_name"),
            CqlIdentifier.fromInternal("address"));
    assertThat(personType.getFieldTypes().get(0)).isEqualTo(DataTypes.TEXT);
    assertThat(personType.getFieldTypes().get(1)).isEqualTo(DataTypes.TEXT);
    assertThat(personType.getFieldTypes().get(2)).isSameAs(addressType);
  }

  @Test
  public void should_parse_legacy_table() {
    UserDefinedTypeParser parser =
        new UserDefinedTypeParser(new DataTypeClassNameParser(), context);
    // no need to add a column for the address type, because in 2.2 UDTs are always fully redefined
    // in column and field types (instead of referencing an existing type)
    Map<CqlIdentifier, UserDefinedType> types = parser.parse(KEYSPACE_ID, PERSON_ROW_2_2);

    assertThat(types).hasSize(1);
    UserDefinedType personType = types.get(CqlIdentifier.fromInternal("person"));

    assertThat(personType.getKeyspace().asInternal()).isEqualTo("ks");
    assertThat(personType.getName().asInternal()).isEqualTo("person");
    assertThat(personType.getFieldNames())
        .containsExactly(
            CqlIdentifier.fromInternal("first_name"),
            CqlIdentifier.fromInternal("last_name"),
            CqlIdentifier.fromInternal("address"));
    assertThat(personType.getFieldTypes().get(0)).isEqualTo(DataTypes.TEXT);
    assertThat(personType.getFieldTypes().get(1)).isEqualTo(DataTypes.TEXT);
    UserDefinedType addressType = ((UserDefinedType) personType.getFieldTypes().get(2));
    assertThat(addressType.getKeyspace().asInternal()).isEqualTo("ks");
    assertThat(addressType.getName().asInternal()).isEqualTo("address");
    assertThat(addressType.getFieldNames())
        .containsExactly(
            CqlIdentifier.fromInternal("street"), CqlIdentifier.fromInternal("zipcode"));
  }

  @Test
  public void should_parse_empty_list() {
    UserDefinedTypeParser parser = new UserDefinedTypeParser(new DataTypeCqlNameParser(), context);
    assertThat(parser.parse(KEYSPACE_ID /* no types*/)).isEmpty();
  }

  @Test
  public void should_parse_singleton_list() {
    UserDefinedTypeParser parser = new UserDefinedTypeParser(new DataTypeCqlNameParser(), context);
    Map<CqlIdentifier, UserDefinedType> types =
        parser.parse(
            KEYSPACE_ID, mockTypeRow("ks", "t", ImmutableList.of("i"), ImmutableList.of("int")));

    assertThat(types).hasSize(1);
    UserDefinedType type = types.get(CqlIdentifier.fromInternal("t"));
    assertThat(type.getKeyspace().asInternal()).isEqualTo("ks");
    assertThat(type.getName().asInternal()).isEqualTo("t");
    assertThat(type.getFieldNames()).containsExactly(CqlIdentifier.fromInternal("i"));
    assertThat(type.getFieldTypes()).containsExactly(DataTypes.INT);
  }

  @Test
  public void should_resolve_list_dependency() {
    UserDefinedTypeParser parser = new UserDefinedTypeParser(new DataTypeCqlNameParser(), context);
    Map<CqlIdentifier, UserDefinedType> types =
        parser.parse(
            KEYSPACE_ID,
            mockTypeRow(
                "ks", "a", ImmutableList.of("bs"), ImmutableList.of("frozen<list<frozen<b>>>")),
            mockTypeRow("ks", "b", ImmutableList.of("i"), ImmutableList.of("int")));

    assertThat(types).hasSize(2);
    UserDefinedType aType = types.get(CqlIdentifier.fromInternal("a"));
    UserDefinedType bType = types.get(CqlIdentifier.fromInternal("b"));
    assertThat(((ListType) aType.getFieldTypes().get(0)).getElementType()).isEqualTo(bType);
  }

  @Test
  public void should_resolve_set_dependency() {
    UserDefinedTypeParser parser = new UserDefinedTypeParser(new DataTypeCqlNameParser(), context);
    Map<CqlIdentifier, UserDefinedType> types =
        parser.parse(
            KEYSPACE_ID,
            mockTypeRow(
                "ks", "a", ImmutableList.of("bs"), ImmutableList.of("frozen<set<frozen<b>>>")),
            mockTypeRow("ks", "b", ImmutableList.of("i"), ImmutableList.of("int")));

    assertThat(types).hasSize(2);
    UserDefinedType aType = types.get(CqlIdentifier.fromInternal("a"));
    UserDefinedType bType = types.get(CqlIdentifier.fromInternal("b"));
    assertThat(((SetType) aType.getFieldTypes().get(0)).getElementType()).isEqualTo(bType);
  }

  @Test
  public void should_resolve_map_dependency() {
    UserDefinedTypeParser parser = new UserDefinedTypeParser(new DataTypeCqlNameParser(), context);
    Map<CqlIdentifier, UserDefinedType> types =
        parser.parse(
            KEYSPACE_ID,
            mockTypeRow(
                "ks",
                "a1",
                ImmutableList.of("bs"),
                ImmutableList.of("frozen<map<int, frozen<b>>>")),
            mockTypeRow(
                "ks",
                "a2",
                ImmutableList.of("bs"),
                ImmutableList.of("frozen<map<frozen<b>, int>>")),
            mockTypeRow("ks", "b", ImmutableList.of("i"), ImmutableList.of("int")));

    assertThat(types).hasSize(3);
    UserDefinedType a1Type = types.get(CqlIdentifier.fromInternal("a1"));
    UserDefinedType a2Type = types.get(CqlIdentifier.fromInternal("a2"));
    UserDefinedType bType = types.get(CqlIdentifier.fromInternal("b"));
    assertThat(((MapType) a1Type.getFieldTypes().get(0)).getValueType()).isEqualTo(bType);
    assertThat(((MapType) a2Type.getFieldTypes().get(0)).getKeyType()).isEqualTo(bType);
  }

  @Test
  public void should_resolve_tuple_dependency() {
    UserDefinedTypeParser parser = new UserDefinedTypeParser(new DataTypeCqlNameParser(), context);
    Map<CqlIdentifier, UserDefinedType> types =
        parser.parse(
            KEYSPACE_ID,
            mockTypeRow(
                "ks",
                "a",
                ImmutableList.of("b"),
                ImmutableList.of("frozen<tuple<int, frozen<b>>>")),
            mockTypeRow("ks", "b", ImmutableList.of("i"), ImmutableList.of("int")));

    assertThat(types).hasSize(2);
    UserDefinedType aType = types.get(CqlIdentifier.fromInternal("a"));
    UserDefinedType bType = types.get(CqlIdentifier.fromInternal("b"));
    assertThat(((TupleType) aType.getFieldTypes().get(0)).getComponentTypes().get(1))
        .isEqualTo(bType);
  }

  @Test
  public void should_resolve_nested_dependency() {
    UserDefinedTypeParser parser = new UserDefinedTypeParser(new DataTypeCqlNameParser(), context);
    Map<CqlIdentifier, UserDefinedType> types =
        parser.parse(
            KEYSPACE_ID,
            mockTypeRow(
                "ks",
                "a",
                ImmutableList.of("bs"),
                ImmutableList.of("frozen<tuple<int, frozen<list<frozen<b>>>>>")),
            mockTypeRow("ks", "b", ImmutableList.of("i"), ImmutableList.of("int")));

    assertThat(types).hasSize(2);
    UserDefinedType aType = types.get(CqlIdentifier.fromInternal("a"));
    UserDefinedType bType = types.get(CqlIdentifier.fromInternal("b"));
    TupleType tupleType = (TupleType) aType.getFieldTypes().get(0);
    ListType listType = (ListType) tupleType.getComponentTypes().get(1);
    assertThat(listType.getElementType()).isEqualTo(bType);
  }
}
