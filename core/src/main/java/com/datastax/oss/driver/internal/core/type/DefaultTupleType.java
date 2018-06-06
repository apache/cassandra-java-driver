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

import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.internal.core.data.DefaultTupleValue;
import com.datastax.oss.driver.shaded.guava.common.base.Joiner;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.List;
import net.jcip.annotations.Immutable;

@Immutable
public class DefaultTupleType implements TupleType {

  private static final long serialVersionUID = 1;

  /** @serial */
  private final ImmutableList<DataType> componentTypes;

  private transient volatile AttachmentPoint attachmentPoint;

  public DefaultTupleType(List<DataType> componentTypes, AttachmentPoint attachmentPoint) {
    Preconditions.checkNotNull(componentTypes);
    this.componentTypes = ImmutableList.copyOf(componentTypes);
    this.attachmentPoint = attachmentPoint;
  }

  public DefaultTupleType(List<DataType> componentTypes) {
    this(componentTypes, AttachmentPoint.NONE);
  }

  @Override
  public List<DataType> getComponentTypes() {
    return componentTypes;
  }

  @Override
  public TupleValue newValue() {
    return new DefaultTupleValue(this);
  }

  @Override
  public TupleValue newValue(Object... values) {
    return new DefaultTupleValue(this, values);
  }

  @Override
  public boolean isDetached() {
    return attachmentPoint == AttachmentPoint.NONE;
  }

  @Override
  public void attach(AttachmentPoint attachmentPoint) {
    this.attachmentPoint = attachmentPoint;
    for (DataType componentType : componentTypes) {
      componentType.attach(attachmentPoint);
    }
  }

  @Override
  public AttachmentPoint getAttachmentPoint() {
    return attachmentPoint;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof TupleType) {
      TupleType that = (TupleType) other;
      return this.componentTypes.equals(that.getComponentTypes());
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return componentTypes.hashCode();
  }

  @Override
  public String toString() {
    return "Tuple(" + WITH_COMMA.join(componentTypes) + ")";
  }

  private static final Joiner WITH_COMMA = Joiner.on(", ");

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    Preconditions.checkNotNull(componentTypes);
    this.attachmentPoint = AttachmentPoint.NONE;
  }
}
