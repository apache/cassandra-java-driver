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
package com.datastax.driver.mapping;

class AliasedMappedProperty implements Comparable<AliasedMappedProperty> {

  final MappedProperty<Object> mappedProperty;
  final String alias;

  @SuppressWarnings("unchecked")
  AliasedMappedProperty(MappedProperty<?> mappedProperty, String alias) {
    this.mappedProperty = (MappedProperty<Object>) mappedProperty;
    this.alias = alias;
  }

  @Override
  public int compareTo(AliasedMappedProperty that) {
    String thisColName = mappedProperty.getMappedName();
    String thatColName = that.mappedProperty.getMappedName();
    return thisColName.compareTo(thatColName);
  }
}
