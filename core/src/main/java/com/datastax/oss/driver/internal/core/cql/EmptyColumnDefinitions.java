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
package com.datastax.oss.driver.internal.core.cql;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import java.util.Collections;
import java.util.Iterator;

/**
 * The singleton that represents no column definitions (implemented as an enum which provides the
 * serialization machinery for free).
 */
public enum EmptyColumnDefinitions implements ColumnDefinitions {
  INSTANCE;

  @Override
  public int size() {
    return 0;
  }

  @Override
  public ColumnDefinition get(int i) {
    throw new ArrayIndexOutOfBoundsException();
  }

  @Override
  public boolean contains(String name) {
    return false;
  }

  @Override
  public boolean contains(CqlIdentifier id) {
    return false;
  }

  @Override
  public int firstIndexOf(String name) {
    return -1;
  }

  @Override
  public int firstIndexOf(CqlIdentifier id) {
    return -1;
  }

  @Override
  public boolean isDetached() {
    return false;
  }

  @Override
  public void attach(AttachmentPoint attachmentPoint) {}

  @Override
  public Iterator<ColumnDefinition> iterator() {
    return Collections.<ColumnDefinition>emptyList().iterator();
  }
}
