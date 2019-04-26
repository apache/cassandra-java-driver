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

import com.datastax.oss.driver.api.mapper.annotations.ClusteringColumn;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.datastax.oss.driver.internal.mapper.processor.ProcessorContext;
import com.datastax.oss.driver.internal.mapper.processor.util.generation.PropertyType;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.squareup.javapoet.ClassName;
import java.beans.Introspector;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
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
    // TODO property annotations: custom name, computed, ignored...
    // TODO inherit annotations and properties from superclass / parent interface

    SortedMap<Integer, PropertyDefinition> partitionKey = new TreeMap<>();
    SortedMap<Integer, PropertyDefinition> clusteringColumns = new TreeMap<>();
    ImmutableList.Builder<PropertyDefinition> regularColumns = ImmutableList.builder();
    for (Element child : classElement.getEnclosedElements()) {
      Set<Modifier> modifiers = child.getModifiers();
      if (child.getKind() != ElementKind.METHOD
          || modifiers.contains(Modifier.STATIC)
          || modifiers.contains(Modifier.PRIVATE)) {
        continue;
      }
      ExecutableElement getMethod = (ExecutableElement) child;
      String getMethodName = getMethod.getSimpleName().toString();
      if (!getMethodName.startsWith("get") || !getMethod.getParameters().isEmpty()) {
        continue;
      }
      TypeMirror typeMirror = getMethod.getReturnType();
      if (typeMirror.getKind() == TypeKind.VOID) {
        continue;
      }
      String propertyName = Introspector.decapitalize(getMethodName.substring(3));
      String setMethodName = getMethodName.replaceFirst("get", "set");
      ExecutableElement setMethod = findSetMethod(classElement, setMethodName, typeMirror);
      if (setMethod == null) {
        continue; // must have both
      }
      VariableElement field = findField(classElement, propertyName, typeMirror);

      PropertyType propertyType = PropertyType.parse(typeMirror, context);
      PropertyDefinition property =
          new DefaultPropertyDefinition(propertyName, getMethodName, setMethodName, propertyType);

      int partitionKeyIndex = getPartitionKeyIndex(getMethod, field);
      int clusteringColumnIndex = getClusteringColumnIndex(getMethod, field, partitionKeyIndex);
      if (partitionKeyIndex >= 0) {
        PropertyDefinition previous = partitionKey.putIfAbsent(partitionKeyIndex, property);
        if (previous != null) {
          context
              .getMessager()
              .error(
                  getMethod,
                  "Duplicate partition key index: if multiple properties are annotated "
                      + "with @%s, the annotation must be parameterized with an integer "
                      + "indicating the position. Found duplicate index %d for %s and %s.",
                  PartitionKey.class.getSimpleName(),
                  partitionKeyIndex,
                  previous.getGetterName(),
                  property.getGetterName());
        }
      } else if (clusteringColumnIndex >= 0) {
        PropertyDefinition previous =
            clusteringColumns.putIfAbsent(clusteringColumnIndex, property);
        if (previous != null) {
          context
              .getMessager()
              .error(
                  getMethod,
                  "Duplicate clustering column index: if multiple properties are annotated "
                      + "with @%s, the annotation must be parameterized with an integer "
                      + "indicating the position. Found duplicate index %d for %s and %s.",
                  ClusteringColumn.class.getSimpleName(),
                  clusteringColumnIndex,
                  previous.getGetterName(),
                  property.getGetterName());
        }
      } else {
        regularColumns.add(property);
      }
    }

    String entityName = Introspector.decapitalize(classElement.getSimpleName().toString());
    return new DefaultEntityDefinition(
        ClassName.get(classElement),
        entityName,
        ImmutableList.copyOf(partitionKey.values()),
        ImmutableList.copyOf(clusteringColumns.values()),
        regularColumns.build());
  }

  private VariableElement findField(
      TypeElement classElement, String propertyName, TypeMirror fieldType) {
    for (Element child : classElement.getEnclosedElements()) {
      if (child.getKind() != ElementKind.FIELD) {
        continue;
      }
      VariableElement field = (VariableElement) child;
      if (field.getSimpleName().toString().equals(propertyName)
          && context.getTypeUtils().isSameType(field.asType(), fieldType)) {
        return field;
      }
    }
    return null;
  }

  private ExecutableElement findSetMethod(
      TypeElement classElement, String setMethodName, TypeMirror fieldType) {
    for (Element child : classElement.getEnclosedElements()) {
      Set<Modifier> modifiers = child.getModifiers();
      if (child.getKind() != ElementKind.METHOD
          || modifiers.contains(Modifier.STATIC)
          || modifiers.contains(Modifier.PRIVATE)) {
        continue;
      }
      ExecutableElement setMethod = (ExecutableElement) child;
      List<? extends VariableElement> parameters = setMethod.getParameters();

      if (setMethod.getSimpleName().toString().equals(setMethodName)
          && parameters.size() == 1
          && context.getTypeUtils().isSameType(parameters.get(0).asType(), fieldType)) {
        return setMethod;
      }
    }
    return null;
  }

  private int getPartitionKeyIndex(ExecutableElement getMethod, VariableElement field) {
    PartitionKey annotation = getMethod.getAnnotation(PartitionKey.class);
    PartitionKey fieldAnnotation = (field == null) ? null : field.getAnnotation(PartitionKey.class);

    if (annotation != null) {
      if (fieldAnnotation != null) {
        context
            .getMessager()
            .warn(
                field,
                "@%s should be used either on the field or the getter, but not both. "
                    + "The annotation on this field will be ignored.",
                PartitionKey.class.getSimpleName());
      }
      return annotation.value();
    } else if (fieldAnnotation != null) {
      return fieldAnnotation.value();
    } else {
      return -1;
    }
  }

  private int getClusteringColumnIndex(
      ExecutableElement getMethod, VariableElement field, int partitionKeyIndex) {
    ClusteringColumn annotation = getMethod.getAnnotation(ClusteringColumn.class);
    ClusteringColumn fieldAnnotation =
        (field == null) ? null : field.getAnnotation(ClusteringColumn.class);

    if (annotation != null) {
      if (partitionKeyIndex >= 0) {
        context
            .getMessager()
            .error(
                getMethod,
                "Properties can't be annotated with both @%s and @%s.",
                PartitionKey.class.getSimpleName(),
                ClusteringColumn.class.getSimpleName());
        return -1;
      } else if (fieldAnnotation != null) {
        context
            .getMessager()
            .warn(
                field,
                "@%s should be used either on the field or the getter, but not both. "
                    + "The annotation on this field will be ignored.",
                ClusteringColumn.class.getSimpleName());
      }
      return annotation.value();
    } else if (fieldAnnotation != null) {
      if (partitionKeyIndex >= 0) {
        context
            .getMessager()
            .error(
                field,
                "Properties can't be annotated with both @%s and @%s.",
                PartitionKey.class.getSimpleName(),
                ClusteringColumn.class.getSimpleName());
      }
      return fieldAnnotation.value();
    } else {
      return -1;
    }
  }
}
