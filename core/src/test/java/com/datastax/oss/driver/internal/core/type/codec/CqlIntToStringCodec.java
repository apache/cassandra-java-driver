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
package com.datastax.oss.driver.internal.core.type.codec;

import com.datastax.oss.driver.api.core.type.codec.MappingCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A sample user codec implementation that we use in our tests.
 *
 * <p>It maps a CQL string to a Java string containing its textual representation.
 */
public class CqlIntToStringCodec extends MappingCodec<Integer, String> {

  public CqlIntToStringCodec() {
    super(TypeCodecs.INT, GenericType.STRING);
  }

  @Nullable
  @Override
  protected String innerToOuter(@Nullable Integer value) {
    return value == null ? null : value.toString();
  }

  @Nullable
  @Override
  protected Integer outerToInner(@Nullable String value) {
    return value == null ? null : Integer.parseInt(value);
  }
}
