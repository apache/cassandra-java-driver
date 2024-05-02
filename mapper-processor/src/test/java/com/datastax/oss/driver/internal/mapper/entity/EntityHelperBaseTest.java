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
package com.datastax.oss.driver.internal.mapper.entity;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class EntityHelperBaseTest {

  @Test
  @UseDataProvider("typesProvider")
  public void should_find_not_matching_types(
      Map<CqlIdentifier, GenericType<?>> entityColumns,
      Map<CqlIdentifier, ColumnMetadata> cqlColumns,
      List<String> expected) {
    // when
    List<String> missingTypes =
        EntityHelperBase.findTypeMismatches(entityColumns, cqlColumns, CodecRegistry.DEFAULT);

    // then
    assertThat(missingTypes).isEqualTo(expected);
  }

  @Test
  public void should_throw_if_there_is_not_matching_cql_column() {
    // given
    ImmutableMap<CqlIdentifier, GenericType<?>> entityColumns =
        ImmutableMap.of(CqlIdentifier.fromCql("c1"), GenericType.of(Integer.class));
    ColumnMetadata columnMetadataInt = mock(ColumnMetadata.class);
    when(columnMetadataInt.getType()).thenReturn(DataTypes.INT);
    ImmutableMap<CqlIdentifier, ColumnMetadata> cqlColumns =
        ImmutableMap.of(CqlIdentifier.fromCql("c2"), columnMetadataInt);

    // when, then
    assertThatThrownBy(
            () ->
                EntityHelperBase.findTypeMismatches(
                    entityColumns, cqlColumns, CodecRegistry.DEFAULT))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("There is no cql column for entity column: c1");
  }

  @DataProvider
  public static Object[][] typesProvider() {
    ColumnMetadata columnMetadataText = mock(ColumnMetadata.class);
    when(columnMetadataText.getType()).thenReturn(DataTypes.TEXT);
    ColumnMetadata columnMetadataInt = mock(ColumnMetadata.class);
    when(columnMetadataInt.getType()).thenReturn(DataTypes.INT);

    CqlIdentifier c1 = CqlIdentifier.fromCql("c1");
    CqlIdentifier c2 = CqlIdentifier.fromCql("c2");
    return new Object[][] {
      {
        ImmutableMap.of(c1, GenericType.of(String.class)),
        ImmutableMap.of(c1, columnMetadataText),
        Collections.emptyList()
      },
      {
        ImmutableMap.of(c1, GenericType.of(Integer.class)),
        ImmutableMap.of(c1, columnMetadataText),
        ImmutableList.of("Field: c1, Entity Type: java.lang.Integer, CQL type: TEXT")
      },
      {
        ImmutableMap.of(c1, GenericType.of(String.class), c2, GenericType.of(Integer.class)),
        ImmutableMap.of(c1, columnMetadataText, c2, columnMetadataInt),
        Collections.emptyList()
      },
      {
        ImmutableMap.of(c1, GenericType.of(String.class), c2, GenericType.of(Integer.class)),
        ImmutableMap.of(c1, columnMetadataInt, c2, columnMetadataText),
        ImmutableList.of(
            "Field: c1, Entity Type: java.lang.String, CQL type: INT",
            "Field: c2, Entity Type: java.lang.Integer, CQL type: TEXT")
      }
    };
  }
}
