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
package com.datastax.oss.driver.api.core.type;

import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;

public interface TupleType extends DataType {

  @NonNull
  List<DataType> getComponentTypes();

  @NonNull
  TupleValue newValue();

  /**
   * Creates a new instance with the specified values for the fields.
   *
   * <p>To encode the values, this method uses the {@link CodecRegistry} that this type is {@link
   * #getAttachmentPoint() attached} to; it looks for the best codec to handle the target CQL type
   * and actual runtime type of each value (see {@link CodecRegistry#codecFor(DataType, Object)}).
   *
   * @param values the values of the tuple's fields. They must be in the same order as the fields in
   *     the tuple's definition. You can specify less values than there are fields (the remaining
   *     ones will be set to NULL), but not more (a runtime exception will be thrown). Individual
   *     values can be {@code null}, but the array itself can't.
   * @throws IllegalArgumentException if there are too many values.
   */
  @NonNull
  TupleValue newValue(@NonNull Object... values);

  @NonNull
  AttachmentPoint getAttachmentPoint();

  @NonNull
  @Override
  default String asCql(boolean includeFrozen, boolean pretty) {
    StringBuilder builder = new StringBuilder();
    // Tuples are always frozen
    if (includeFrozen) {
      builder.append("frozen<");
    }
    boolean first = true;
    for (DataType type : getComponentTypes()) {
      builder.append(first ? "tuple<" : ", ");
      first = false;
      builder.append(type.asCql(includeFrozen, pretty));
    }
    builder.append('>');
    if (includeFrozen) {
      builder.append('>');
    }
    return builder.toString();
  }

  @Override
  default int getProtocolCode() {
    return ProtocolConstants.DataType.TUPLE;
  }
}
