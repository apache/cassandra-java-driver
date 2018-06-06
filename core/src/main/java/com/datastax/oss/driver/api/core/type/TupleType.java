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

import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import java.util.List;

public interface TupleType extends DataType {
  List<DataType> getComponentTypes();

  TupleValue newValue();

  /**
   * Creates a new instance with the specified values for the fields.
   *
   * <p>The values must be in the same order as the fields in the tuple's definition. You can
   * specify less values than there are fields (the remaining ones will be set to NULL), but not
   * more (a runtime exception will be thrown).
   *
   * <p>To encode the values, this method uses the {@link CodecRegistry} that this type is {@link
   * #getAttachmentPoint() attached} to; it looks for the best codec to handle the target CQL type
   * and actual runtime type of each value (see {@link CodecRegistry#codecFor(DataType, Object)}).
   *
   * @throws IllegalArgumentException if there are too many values.
   */
  TupleValue newValue(Object... values);

  AttachmentPoint getAttachmentPoint();

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
