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
package com.datastax.oss.driver.internal.core.type;

import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Objects;
import net.jcip.annotations.Immutable;

@Immutable
public class DefaultListType implements ListType, Serializable {

  private static final long serialVersionUID = 1;

  /** @serial */
  private final DataType elementType;
  /** @serial */
  private final boolean frozen;

  public DefaultListType(@NonNull DataType elementType, boolean frozen) {
    Preconditions.checkNotNull(elementType);
    this.elementType = elementType;
    this.frozen = frozen;
  }

  @NonNull
  @Override
  public DataType getElementType() {
    return elementType;
  }

  @Override
  public boolean isFrozen() {
    return frozen;
  }

  @Override
  public boolean isDetached() {
    return elementType.isDetached();
  }

  @Override
  public void attach(@NonNull AttachmentPoint attachmentPoint) {
    elementType.attach(attachmentPoint);
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof ListType) {
      ListType that = (ListType) other;
      // frozen is not taken into account
      return this.elementType.equals(that.getElementType());
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(DefaultListType.class, this.elementType);
  }

  @Override
  public String toString() {
    return "List(" + elementType + ", " + (frozen ? "" : "not ") + "frozen)";
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    Preconditions.checkNotNull(elementType);
  }
}
