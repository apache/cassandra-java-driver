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

import com.datastax.oss.protocol.internal.ProtocolConstants;
import edu.umd.cs.findbugs.annotations.NonNull;

public interface MapType extends DataType {

  @NonNull
  DataType getKeyType();

  @NonNull
  DataType getValueType();

  boolean isFrozen();

  @NonNull
  @Override
  default String asCql(boolean includeFrozen, boolean pretty) {
    String template = (isFrozen() && includeFrozen) ? "frozen<map<%s, %s>>" : "map<%s, %s>";
    return String.format(
        template,
        getKeyType().asCql(includeFrozen, pretty),
        getValueType().asCql(includeFrozen, pretty));
  }

  @Override
  default int getProtocolCode() {
    return ProtocolConstants.DataType.MAP;
  }
}
