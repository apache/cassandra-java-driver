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

import com.squareup.javapoet.TypeName;
import javax.lang.model.element.TypeElement;

public class DefaultPropertyDefinition implements PropertyDefinition {

  private final String cqlName;
  private final String getterName;
  private final String setterName;
  private final TypeName type;
  private final TypeElement entityElement;

  public DefaultPropertyDefinition(
      String cqlName,
      String getterName,
      String setterName,
      TypeName type,
      TypeElement entityElement) {
    this.cqlName = cqlName;
    this.getterName = getterName;
    this.setterName = setterName;
    this.type = type;
    this.entityElement = entityElement;
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
  public TypeName getType() {
    return type;
  }

  @Override
  public TypeElement getEntityElement() {
    return entityElement;
  }

  public static class Builder {
    private final String cqlName;
    private final TypeName type;
    private final TypeElement entityElement;
    private String getterName;
    private String setterName;

    public Builder(String cqlName, TypeName type, TypeElement entityElement) {
      this.cqlName = cqlName;
      this.type = type;
      this.entityElement = entityElement;
    }

    public Builder withGetterName(String getterName) {
      this.getterName = getterName;
      return this;
    }

    public Builder withSetterName(String setterName) {
      this.setterName = setterName;
      return this;
    }

    public TypeName getType() {
      return type;
    }

    public String getGetterName() {
      return getterName;
    }

    public String getSetterName() {
      return setterName;
    }

    DefaultPropertyDefinition build() {
      return new DefaultPropertyDefinition(cqlName, getterName, setterName, type, entityElement);
    }
  }
}
