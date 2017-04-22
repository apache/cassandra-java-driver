/*
 * Copyright (C) 2017-2017 DataStax Inc.
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
package com.datastax.oss.driver.internal.type;

import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.type.CustomType;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.ObjectInputStream;

public class DefaultCustomType implements CustomType {

  private static final long serialVersionUID = 1;

  /** @serial */
  private final String className;

  public DefaultCustomType(String className) {
    Preconditions.checkNotNull(className);
    this.className = className;
  }

  @Override
  public String getClassName() {
    return className;
  }

  @Override
  public boolean isDetached() {
    return false;
  }

  @Override
  public void attach(AttachmentPoint attachmentPoint) {
    // nothing to do
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof CustomType) {
      CustomType that = (CustomType) other;
      return this.className.equals(that.getClassName());
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return className.hashCode();
  }

  @Override
  public String toString() {
    return "Custom(" + className + ")";
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    Preconditions.checkNotNull(className);
  }
}
