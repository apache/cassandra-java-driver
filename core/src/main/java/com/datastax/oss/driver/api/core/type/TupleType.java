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
import com.datastax.oss.protocol.internal.ProtocolConstants;
import java.util.List;

public interface TupleType extends DataType {
  List<DataType> getComponentTypes();

  TupleValue newValue();

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

  default int getProtocolCode() {
    return ProtocolConstants.DataType.TUPLE;
  }
}
