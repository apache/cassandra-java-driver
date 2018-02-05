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
package com.datastax.oss.driver.internal.core.type;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.SerializationHelper;
import org.junit.Test;

public class DataTypeSerializationTest {

  @Test
  public void should_serialize_and_deserialize() {
    TupleType tuple = DataTypes.tupleOf(DataTypes.INT, DataTypes.TEXT);
    UserDefinedType udt =
        new UserDefinedTypeBuilder(
                CqlIdentifier.fromInternal("ks"), CqlIdentifier.fromInternal("type"))
            .withField(CqlIdentifier.fromInternal("field1"), DataTypes.INT)
            .withField(CqlIdentifier.fromInternal("field2"), DataTypes.TEXT)
            .build();

    // Because primitive and custom types never use the codec registry, we consider them always
    // attached
    should_serialize_and_deserialize(DataTypes.INT, false);
    should_serialize_and_deserialize(DataTypes.custom("some.class.name"), false);

    should_serialize_and_deserialize(tuple, true);
    should_serialize_and_deserialize(udt, true);
    should_serialize_and_deserialize(DataTypes.listOf(DataTypes.INT), false);
    should_serialize_and_deserialize(DataTypes.listOf(tuple), true);
    should_serialize_and_deserialize(DataTypes.setOf(udt), true);
    should_serialize_and_deserialize(DataTypes.mapOf(tuple, udt), true);
  }

  private void should_serialize_and_deserialize(DataType in, boolean expectDetached) {
    // When
    DataType out = SerializationHelper.serializeAndDeserialize(in);

    // Then
    assertThat(out).isEqualTo(in);
    assertThat(out.isDetached()).isEqualTo(expectDetached);
  }
}
