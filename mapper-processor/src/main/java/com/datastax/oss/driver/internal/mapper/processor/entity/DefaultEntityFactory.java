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

import com.datastax.oss.driver.internal.mapper.processor.ProcessorContext;
import com.datastax.oss.driver.internal.mapper.processor.util.generation.PropertyType;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.squareup.javapoet.ClassName;
import java.beans.Introspector;
import java.util.HashMap;
import java.util.Map;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;

public class DefaultEntityFactory implements EntityFactory {

  private final ProcessorContext context;

  public DefaultEntityFactory(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public EntityDefinition getDefinition(TypeElement classElement) {

    // Basic implementation to get things started: look for pairs of getter/setter methods that
    // share the same name and operate on the same type.
    // This will get revisited in future tickets:
    // TODO support custom naming conventions
    // TODO property annotations: PK, custom name, computed, ignored...
    // TODO inherit annotations and properties from superclass / parent interface
    // TODO handle annotations on fields...

    Map<String, DefaultPropertyDefinition.Builder> propertyBuilders = new HashMap<>();
    for (Element child : classElement.getEnclosedElements()) {
      if (child.getKind() == ElementKind.METHOD) {
        ExecutableElement method = (ExecutableElement) child;
        String methodName = method.getSimpleName().toString();
        if (methodName.startsWith("get") && method.getParameters().isEmpty()) {
          TypeMirror typeMirror = method.getReturnType();
          if (typeMirror.getKind() == TypeKind.VOID) {
            continue;
          }
          String propertyName = Introspector.decapitalize(methodName.substring(3));
          DefaultPropertyDefinition.Builder builder = propertyBuilders.get(propertyName);
          if (builder == null) {
            builder =
                new DefaultPropertyDefinition.Builder(
                    propertyName, typeMirror, PropertyType.parse(typeMirror, context));
            propertyBuilders.put(propertyName, builder);
          } else if (!context.getTypeUtils().isSameType(builder.getRawType(), typeMirror)) {
            context
                .getMessager()
                .warn(
                    method,
                    "Ignoring method %s %s() because there is a setter "
                        + "with the same name but a different type: %s(%s)",
                    typeMirror,
                    methodName,
                    builder.getSetterName(),
                    builder.getRawType());
            continue;
          }
          builder.withGetterName(methodName);
        } else if (methodName.startsWith("set") && method.getParameters().size() == 1) {
          String propertyName = Introspector.decapitalize(methodName.substring(3));
          VariableElement parameter = method.getParameters().get(0);
          TypeMirror typeMirror = parameter.asType();
          DefaultPropertyDefinition.Builder builder = propertyBuilders.get(propertyName);
          if (builder == null) {
            builder =
                new DefaultPropertyDefinition.Builder(
                    propertyName, typeMirror, PropertyType.parse(typeMirror, context));
            propertyBuilders.put(propertyName, builder);
          } else if (!context.getTypeUtils().isSameType(builder.getRawType(), typeMirror)) {
            context
                .getMessager()
                .warn(
                    method,
                    "Ignoring method %s(%s) because there is a getter "
                        + "with the same name but a different type: %s %s()",
                    methodName,
                    typeMirror,
                    builder.getRawType(),
                    builder.getGetterName());
            continue;
          }
          builder.withSetterName(methodName);
        }
      }
    }

    ImmutableList.Builder<PropertyDefinition> definitions = ImmutableList.builder();
    for (DefaultPropertyDefinition.Builder builder : propertyBuilders.values()) {
      if (builder.getGetterName() != null && builder.getSetterName() != null) {
        definitions.add(builder.build());
      }
    }

    String entityName = Introspector.decapitalize(classElement.getSimpleName().toString());
    return new DefaultEntityDefinition(
        ClassName.get(classElement), entityName, definitions.build());
  }
}
