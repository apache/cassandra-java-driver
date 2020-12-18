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
package com.datastax.oss.driver.internal.core.metadata.schema.parsing;

import static com.datastax.oss.driver.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.datastax.oss.driver.TestDataProviders;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.util.Locale;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(DataProviderRunner.class)
public class DataTypeClassNameParserTest {

  private static final CqlIdentifier KEYSPACE_ID = CqlIdentifier.fromInternal("ks");

  @Mock private InternalDriverContext context;
  private DataTypeClassNameParser parser;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    parser = new DataTypeClassNameParser();
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "locales")
  public void should_parse_native_types(Locale locale) {
    Locale def = Locale.getDefault();
    try {
      Locale.setDefault(locale);
      for (Map.Entry<String, DataType> entry :
          DataTypeClassNameParser.NATIVE_TYPES_BY_CLASS_NAME.entrySet()) {

        String className = entry.getKey();
        DataType expectedType = entry.getValue();

        assertThat(parse(className)).isEqualTo(expectedType);
      }
    } finally {
      Locale.setDefault(def);
    }
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "locales")
  public void should_parse_collection_types(Locale locale) {
    Locale def = Locale.getDefault();
    try {
      Locale.setDefault(locale);
      assertThat(
              parse(
                  "org.apache.cassandra.db.marshal.ListType("
                      + "org.apache.cassandra.db.marshal.UTF8Type)"))
          .isEqualTo(DataTypes.listOf(DataTypes.TEXT));

      assertThat(
              parse(
                  "org.apache.cassandra.db.marshal.FrozenType("
                      + ("org.apache.cassandra.db.marshal.ListType("
                          + "org.apache.cassandra.db.marshal.UTF8Type))")))
          .isEqualTo(DataTypes.frozenListOf(DataTypes.TEXT));

      assertThat(
              parse(
                  "org.apache.cassandra.db.marshal.SetType("
                      + "org.apache.cassandra.db.marshal.UTF8Type)"))
          .isEqualTo(DataTypes.setOf(DataTypes.TEXT));

      assertThat(
              parse(
                  "org.apache.cassandra.db.marshal.MapType("
                      + "org.apache.cassandra.db.marshal.UTF8Type,"
                      + "org.apache.cassandra.db.marshal.UTF8Type)"))
          .isEqualTo(DataTypes.mapOf(DataTypes.TEXT, DataTypes.TEXT));

      assertThat(
              parse(
                  "org.apache.cassandra.db.marshal.MapType("
                      + "org.apache.cassandra.db.marshal.UTF8Type,"
                      + "org.apache.cassandra.db.marshal.FrozenType("
                      + ("org.apache.cassandra.db.marshal.MapType("
                          + "org.apache.cassandra.db.marshal.Int32Type,"
                          + "org.apache.cassandra.db.marshal.Int32Type)))")))
          .isEqualTo(
              DataTypes.mapOf(DataTypes.TEXT, DataTypes.frozenMapOf(DataTypes.INT, DataTypes.INT)));
    } finally {
      Locale.setDefault(def);
    }
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "locales")
  public void should_parse_user_type_when_definition_not_already_available(Locale locale) {
    Locale def = Locale.getDefault();
    try {
      Locale.setDefault(locale);
      UserDefinedType addressType =
          (UserDefinedType)
              parse(
                  "org.apache.cassandra.db.marshal.UserType("
                      + "foo,61646472657373,"
                      + ("737472656574:org.apache.cassandra.db.marshal.UTF8Type,"
                          + "7a6970636f6465:org.apache.cassandra.db.marshal.Int32Type,"
                          + ("70686f6e6573:org.apache.cassandra.db.marshal.SetType("
                              + "org.apache.cassandra.db.marshal.UserType(foo,70686f6e65,"
                              + "6e616d65:org.apache.cassandra.db.marshal.UTF8Type,"
                              + "6e756d626572:org.apache.cassandra.db.marshal.UTF8Type)")
                          + "))"));

      assertThat(addressType.getKeyspace().asInternal()).isEqualTo("foo");
      assertThat(addressType.getName().asInternal()).isEqualTo("address");
      assertThat(addressType.isFrozen()).isTrue();
      assertThat(addressType.getFieldNames().size()).isEqualTo(3);

      assertThat(addressType.getFieldNames().get(0).asInternal()).isEqualTo("street");
      assertThat(addressType.getFieldTypes().get(0)).isEqualTo(DataTypes.TEXT);

      assertThat(addressType.getFieldNames().get(1).asInternal()).isEqualTo("zipcode");
      assertThat(addressType.getFieldTypes().get(1)).isEqualTo(DataTypes.INT);

      assertThat(addressType.getFieldNames().get(2).asInternal()).isEqualTo("phones");
      DataType phonesType = addressType.getFieldTypes().get(2);
      assertThat(phonesType).isInstanceOf(SetType.class);
      UserDefinedType phoneType = ((UserDefinedType) ((SetType) phonesType).getElementType());

      assertThat(phoneType.getKeyspace().asInternal()).isEqualTo("foo");
      assertThat(phoneType.getName().asInternal()).isEqualTo("phone");
      assertThat(phoneType.isFrozen()).isTrue();
      assertThat(phoneType.getFieldNames().size()).isEqualTo(2);

      assertThat(phoneType.getFieldNames().get(0).asInternal()).isEqualTo("name");
      assertThat(phoneType.getFieldTypes().get(0)).isEqualTo(DataTypes.TEXT);

      assertThat(phoneType.getFieldNames().get(1).asInternal()).isEqualTo("number");
      assertThat(phoneType.getFieldTypes().get(1)).isEqualTo(DataTypes.TEXT);
    } finally {
      Locale.setDefault(def);
    }
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "locales")
  public void should_make_a_frozen_copy_user_type_when_definition_already_available(Locale locale) {
    Locale def = Locale.getDefault();
    try {
      Locale.setDefault(locale);
      UserDefinedType existing = mock(UserDefinedType.class);

      parse(
          "org.apache.cassandra.db.marshal.UserType(foo,70686f6e65,"
              + "6e616d65:org.apache.cassandra.db.marshal.UTF8Type,"
              + "6e756d626572:org.apache.cassandra.db.marshal.UTF8Type)",
          ImmutableMap.of(CqlIdentifier.fromInternal("phone"), existing));

      verify(existing).copy(true);
    } finally {
      Locale.setDefault(def);
    }
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "locales")
  public void should_parse_tuple(Locale locale) {
    Locale def = Locale.getDefault();
    try {
      Locale.setDefault(locale);
      TupleType tupleType =
          (TupleType)
              parse(
                  "org.apache.cassandra.db.marshal.TupleType("
                      + "org.apache.cassandra.db.marshal.Int32Type,"
                      + "org.apache.cassandra.db.marshal.UTF8Type,"
                      + "org.apache.cassandra.db.marshal.FloatType)");

      assertThat(tupleType.getComponentTypes().size()).isEqualTo(3);
      assertThat(tupleType.getComponentTypes().get(0)).isEqualTo(DataTypes.INT);
      assertThat(tupleType.getComponentTypes().get(1)).isEqualTo(DataTypes.TEXT);
      assertThat(tupleType.getComponentTypes().get(2)).isEqualTo(DataTypes.FLOAT);
    } finally {
      Locale.setDefault(def);
    }
  }

  private DataType parse(String toParse) {
    return parse(toParse, null);
  }

  private DataType parse(String toParse, Map<CqlIdentifier, UserDefinedType> existingTypes) {
    return parser.parse(KEYSPACE_ID, toParse, existingTypes, context);
  }
}
