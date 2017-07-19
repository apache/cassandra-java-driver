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
package com.datastax.oss.driver.internal.core.cql;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.internal.core.data.IdentifierIndex;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DefaultColumnDefinitions implements ColumnDefinitions {

  static ColumnDefinitions valueOf(List<ColumnDefinition> definitions) {
    return definitions.isEmpty()
        ? EmptyColumnDefinitions.INSTANCE
        : new DefaultColumnDefinitions(definitions);
  }

  private final List<ColumnDefinition> definitions;
  private final IdentifierIndex index;

  private DefaultColumnDefinitions(List<ColumnDefinition> definitions) {
    assert definitions != null && definitions.size() > 0;
    this.definitions = definitions;
    this.index = buildIndex(definitions);
  }

  @Override
  public int size() {
    return definitions.size();
  }

  @Override
  public ColumnDefinition get(int i) {
    return definitions.get(i);
  }

  @Override
  public Iterator<ColumnDefinition> iterator() {
    return definitions.iterator();
  }

  @Override
  public boolean contains(String name) {
    return index.firstIndexOf(name) >= 0;
  }

  @Override
  public boolean contains(CqlIdentifier id) {
    return index.firstIndexOf(id) >= 0;
  }

  @Override
  public int firstIndexOf(String name) {
    return index.firstIndexOf(name);
  }

  @Override
  public int firstIndexOf(CqlIdentifier id) {
    return index.firstIndexOf(id);
  }

  @Override
  public boolean isDetached() {
    return definitions.get(0).isDetached();
  }

  @Override
  public void attach(AttachmentPoint attachmentPoint) {
    for (ColumnDefinition definition : definitions) {
      definition.attach(attachmentPoint);
    }
  }

  private static IdentifierIndex buildIndex(List<ColumnDefinition> definitions) {
    List<CqlIdentifier> identifiers = new ArrayList<>(definitions.size());
    for (ColumnDefinition definition : definitions) {
      identifiers.add(definition.getName());
    }
    return new IdentifierIndex(identifiers);
  }

  /**
   * @serialData The list of definitions (the identifier index is reconstructed at deserialization).
   */
  private Object writeReplace() {
    return new SerializationProxy(this);
  }

  private void readObject(ObjectInputStream stream) throws InvalidObjectException {
    // Should never be called since we serialized a proxy
    throw new InvalidObjectException("Proxy required");
  }

  private static class SerializationProxy implements Serializable {

    private static final long serialVersionUID = 1;

    private final List<ColumnDefinition> definitions;

    private SerializationProxy(DefaultColumnDefinitions columnDefinitions) {
      this.definitions = columnDefinitions.definitions;
    }

    private Object readResolve() {
      return new DefaultColumnDefinitions(this.definitions);
    }
  }
}
