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
package com.datastax.oss.driver.api.core.cql;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.AccessibleByName;
import com.datastax.oss.driver.api.core.data.GettableById;
import com.datastax.oss.driver.api.core.data.GettableByName;
import com.datastax.oss.driver.api.core.data.SettableById;
import com.datastax.oss.driver.api.core.data.SettableByName;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import edu.umd.cs.findbugs.annotations.CheckReturnValue;
import edu.umd.cs.findbugs.annotations.NonNull;

/** A data container with the ability to unset values. */
public interface Bindable<SelfT extends Bindable<SelfT>>
    extends GettableById, GettableByName, SettableById<SelfT>, SettableByName<SelfT> {
  /**
   * Whether the {@code i}th value has been set.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  @SuppressWarnings("ReferenceEquality")
  default boolean isSet(int i) {
    return getBytesUnsafe(i) != ProtocolConstants.UNSET_VALUE;
  }

  /**
   * Whether the value for the first occurrence of {@code id} has been set.
   *
   * <p>If you want to avoid the overhead of building a {@code CqlIdentifier}, use the variant of
   * this method that takes a string argument.
   *
   * @throws IndexOutOfBoundsException if the id is invalid.
   */
  @SuppressWarnings("ReferenceEquality")
  default boolean isSet(@NonNull CqlIdentifier id) {
    return getBytesUnsafe(id) != ProtocolConstants.UNSET_VALUE;
  }

  /**
   * Whether the value for the first occurrence of {@code name} has been set.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IndexOutOfBoundsException if the name is invalid.
   */
  @SuppressWarnings("ReferenceEquality")
  default boolean isSet(@NonNull String name) {
    return getBytesUnsafe(name) != ProtocolConstants.UNSET_VALUE;
  }

  /**
   * Unsets the {@code i}th value. This will leave the statement in the same state as if no setter
   * was ever called for this value.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  @NonNull
  @CheckReturnValue
  default SelfT unset(int i) {
    return setBytesUnsafe(i, ProtocolConstants.UNSET_VALUE);
  }

  /**
   * Unsets the value for the first occurrence of {@code id}. This will leave the statement in the
   * same state as if no setter was ever called for this value.
   *
   * @throws IndexOutOfBoundsException if the id is invalid.
   */
  @NonNull
  @CheckReturnValue
  default SelfT unset(@NonNull CqlIdentifier id) {
    return setBytesUnsafe(id, ProtocolConstants.UNSET_VALUE);
  }

  /**
   * Unsets the value for the first occurrence of {@code name}. This will leave the statement in the
   * same state as if no setter was ever called for this value.
   *
   * @throws IndexOutOfBoundsException if the name is invalid.
   */
  @NonNull
  @CheckReturnValue
  default SelfT unset(@NonNull String name) {
    return setBytesUnsafe(name, ProtocolConstants.UNSET_VALUE);
  }
}
