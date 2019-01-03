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
package com.datastax.oss.driver.internal.mapper.processor;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.TypeName;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;

public class EntityParser {

  private final ClassName className;
  private final EntityDefinition definition;

  public EntityParser(TypeElement element, GenerationContext context) {
    className = ClassName.get(element);

    ImmutableList.Builder<FieldDefinition> fields = ImmutableList.builder();
    for (Element child : element.getEnclosedElements()) {
      // Minimal implementation: this makes A LOT of assumptions, it will be much more complex:
      // naming strategies, inheritance, etc.
      if (child.getKind() == ElementKind.FIELD) {
        VariableElement field = (VariableElement) child;
        String name = field.getSimpleName().toString();
        String capitalizedName = name.substring(0, 1).toUpperCase() + name.substring(1);
        String getterName = "get" + capitalizedName;
        String setterName = "set" + capitalizedName;
        TypeName type = ClassName.get(field.asType());
        fields.add(new FieldDefinition(name, getterName, setterName, type));
      }
    }
    definition = new EntityDefinition(fields.build());
  }

  public ClassName getClassName() {
    return className;
  }

  public EntityDefinition getDefinition() {
    return definition;
  }
}
