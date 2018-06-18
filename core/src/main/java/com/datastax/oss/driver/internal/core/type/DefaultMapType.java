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

import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Objects;
import net.jcip.annotations.Immutable;

@Immutable
public class DefaultMapType implements MapType, Serializable {

  private static final long serialVersionUID = 1;

  /** @serial */
  private final DataType keyType;
  /** @serial */
  private final DataType valueType;
  /** @serial */
  private final boolean frozen;

  public DefaultMapType(@NonNull DataType keyType, @NonNull DataType valueType, boolean frozen) {
    Preconditions.checkNotNull(keyType);
    Preconditions.checkNotNull(valueType);
    this.keyType = keyType;
    this.valueType = valueType;
    this.frozen = frozen;
  }

  @NonNull
  @Override
  public DataType getKeyType() {
    return keyType;
  }

  @NonNull
  @Override
  public DataType getValueType() {
    return valueType;
  }

  @Override
  public boolean isFrozen() {
    return frozen;
  }

  @Override
  public boolean isDetached() {
    return keyType.isDetached() || valueType.isDetached();
  }

  @Override
  public void attach(@NonNull AttachmentPoint attachmentPoint) {
    keyType.attach(attachmentPoint);
    valueType.attach(attachmentPoint);
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof MapType) {
      MapType that = (MapType) other;
      // frozen is not taken into account
      return this.keyType.equals(that.getKeyType()) && this.valueType.equals(that.getValueType());
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(DefaultMapType.class, keyType, valueType);
  }

  @Override
  public String toString() {
    return "Map(" + keyType + " => " + valueType + ", " + (frozen ? "" : "not ") + "frozen)";
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    Preconditions.checkNotNull(keyType);
    Preconditions.checkNotNull(valueType);
  }
}
