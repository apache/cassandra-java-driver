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
package com.datastax.oss.driver.api.core.data;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import java.nio.ByteBuffer;

/**
 * A data structure that provides methods to set its values via an integer index, a name, or CQL
 * identifier.
 */
public interface Settable<T extends Settable<T>>
    extends Accessible, SettableByIndex<T>, SettableByName<T>, SettableById<T> {

  @Override
  default T setBytesUnsafe(CqlIdentifier id, ByteBuffer v) {
    return setBytesUnsafe(firstIndexOf(id), v);
  }

  @Override
  default T setBytesUnsafe(String name, ByteBuffer v) {
    return setBytesUnsafe(firstIndexOf(name), v);
  }
}
