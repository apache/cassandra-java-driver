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

import static com.datastax.oss.driver.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.schema.ShallowUserDefinedType;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class DataTypeCqlNameParserTest {

  private static final CqlIdentifier KEYSPACE_ID = CqlIdentifier.fromInternal("ks");

  @Mock private InternalDriverContext context;
  private DataTypeCqlNameParser parser;

  @Before
  public void setUp() throws Exception {
    parser = new DataTypeCqlNameParser();
  }

  @Test
  public void should_parse_native_types() {
    for (Map.Entry<String, DataType> entry :
        DataTypeCqlNameParser.NATIVE_TYPES_BY_NAME.entrySet()) {

      String className = entry.getKey();
      DataType expectedType = entry.getValue();

      assertThat(parse(className)).isEqualTo(expectedType);
    }
  }

  @Test
  public void should_parse_collection_types() {
    assertThat(parse("list<text>")).isEqualTo(DataTypes.listOf(DataTypes.TEXT));
    assertThat(parse("frozen<list<text>>")).isEqualTo(DataTypes.frozenListOf(DataTypes.TEXT));
    assertThat(parse("set<text>")).isEqualTo(DataTypes.setOf(DataTypes.TEXT));
    assertThat(parse("map<text,text>")).isEqualTo(DataTypes.mapOf(DataTypes.TEXT, DataTypes.TEXT));
    assertThat(parse("map<text,frozen<map<int,int>>>"))
        .isEqualTo(
            DataTypes.mapOf(DataTypes.TEXT, DataTypes.frozenMapOf(DataTypes.INT, DataTypes.INT)));
  }

  @Test
  public void should_parse_top_level_user_type_as_shallow() {
    UserDefinedType addressType = (UserDefinedType) parse("address");
    assertThat(addressType).isInstanceOf(ShallowUserDefinedType.class);
    assertThat(addressType.getKeyspace()).isEqualTo(KEYSPACE_ID);
    assertThat(addressType.getName().asInternal()).isEqualTo("address");
    assertThat(addressType.isFrozen()).isFalse();

    UserDefinedType frozenAddressType = (UserDefinedType) parse("frozen<address>");
    assertThat(frozenAddressType).isInstanceOf(ShallowUserDefinedType.class);
    assertThat(frozenAddressType.getKeyspace()).isEqualTo(KEYSPACE_ID);
    assertThat(frozenAddressType.getName().asInternal()).isEqualTo("address");
    assertThat(frozenAddressType.isFrozen()).isTrue();
  }

  @Test
  public void should_reuse_existing_user_type_when_not_top_level() {
    UserDefinedType addressType = mock(UserDefinedType.class);
    UserDefinedType frozenAddressType = mock(UserDefinedType.class);
    when(addressType.copy(false)).thenReturn(addressType);
    when(addressType.copy(true)).thenReturn(frozenAddressType);

    ImmutableMap<CqlIdentifier, UserDefinedType> existingTypes =
        ImmutableMap.of(CqlIdentifier.fromInternal("address"), addressType);

    ListType listOfAddress = (ListType) parse("list<address>", existingTypes);
    assertThat(listOfAddress.getElementType()).isEqualTo(addressType);

    ListType listOfFrozenAddress = (ListType) parse("list<frozen<address>>", existingTypes);
    assertThat(listOfFrozenAddress.getElementType()).isEqualTo(frozenAddressType);
  }

  @Test
  public void should_parse_tuple() {
    TupleType tupleType = (TupleType) parse("tuple<int,text,float>");

    assertThat(tupleType.getComponentTypes().size()).isEqualTo(3);
    assertThat(tupleType.getComponentTypes().get(0)).isEqualTo(DataTypes.INT);
    assertThat(tupleType.getComponentTypes().get(1)).isEqualTo(DataTypes.TEXT);
    assertThat(tupleType.getComponentTypes().get(2)).isEqualTo(DataTypes.FLOAT);
  }

  @Test
  public void should_parse_udt_named_like_collection_type() {
    // Those are all valid UDT names!
    assertThat(parse("tuple")).isInstanceOf(UserDefinedType.class);
    assertThat(parse("list")).isInstanceOf(UserDefinedType.class);
    assertThat(parse("map")).isInstanceOf(UserDefinedType.class);
    assertThat(parse("frozen")).isInstanceOf(UserDefinedType.class);

    MapType mapType = (MapType) parse("map<list,frozen>");
    assertThat(mapType.getKeyType()).isInstanceOf(UserDefinedType.class);
    assertThat(mapType.getValueType()).isInstanceOf(UserDefinedType.class);
  }

  private DataType parse(String toParse) {
    return parse(toParse, null);
  }

  private DataType parse(String toParse, Map<CqlIdentifier, UserDefinedType> existingTypes) {
    return parser.parse(KEYSPACE_ID, toParse, existingTypes, context);
  }
}
