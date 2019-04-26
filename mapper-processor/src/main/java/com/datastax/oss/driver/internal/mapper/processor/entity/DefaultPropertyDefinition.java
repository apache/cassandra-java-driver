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
package com.datastax.oss.driver.internal.mapper.processor.entity;

import com.datastax.oss.driver.internal.mapper.processor.util.generation.PropertyType;

public class DefaultPropertyDefinition implements PropertyDefinition {

  private final String cqlName;
  private final String getterName;
  private final String setterName;
  private final PropertyType type;

  public DefaultPropertyDefinition(
      String cqlName, String getterName, String setterName, PropertyType type) {
    this.cqlName = cqlName;
    this.getterName = getterName;
    this.setterName = setterName;
    this.type = type;
  }

  @Override
  public String getCqlName() {
    return cqlName;
  }

  @Override
  public String getGetterName() {
    return getterName;
  }

  @Override
  public String getSetterName() {
    return setterName;
  }

  @Override
  public PropertyType getType() {
    return type;
  }
}
