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

public interface SetType extends DataType {
  DataType getElementType();

  boolean isFrozen();

  @Override
  default String asCql(boolean includeFrozen, boolean pretty) {
    String template = (isFrozen() && includeFrozen) ? "frozen<set<%s>>" : "set<%s>";
    return String.format(template, getElementType().asCql(includeFrozen, pretty));
  }

  default int getProtocolCode() {
    return ProtocolConstants.DataType.SET;
  }
}
