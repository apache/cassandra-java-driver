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
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.NamingStrategy;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.datastax.oss.driver.api.mapper.entity.naming.NamingConvention;
import com.datastax.oss.driver.internal.mapper.processor.ProcessorContext;
import com.datastax.oss.driver.internal.mapper.processor.util.generation.PropertyType;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.squareup.javapoet.ClassName;
import java.beans.Introspector;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.AnnotationValue;
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

    // TODO property annotations: computed, ignored...
    // TODO inherit annotations and properties from superclass / parent interface

    CqlNameGenerator cqlNameGenerator = buildCqlNameGenerator(classElement);

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
          new DefaultPropertyDefinition(
              propertyName,
              getCustomCqlName(getMethod, field),
              getMethodName,
              setMethodName,
              propertyType,
              cqlNameGenerator);

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
        Optional.ofNullable(classElement.getAnnotation(CqlName.class)).map(CqlName::value),
        ImmutableList.copyOf(partitionKey.values()),
        ImmutableList.copyOf(clusteringColumns.values()),
        regularColumns.build(),
        cqlNameGenerator);
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

  private Optional<String> getCustomCqlName(ExecutableElement getMethod, VariableElement field) {
    CqlName getterAnnotation = getMethod.getAnnotation(CqlName.class);
    CqlName fieldAnnotation = (field == null) ? null : field.getAnnotation(CqlName.class);
    if (getterAnnotation != null) {
      if (fieldAnnotation != null) {
        context
            .getMessager()
            .warn(
                field,
                "@%s should be used either on the field or the getter, but not both. "
                    + "The annotation on this field will be ignored.",
                CqlName.class.getSimpleName());
      }
      return Optional.of(getterAnnotation.value());
    } else if (fieldAnnotation != null) {
      return Optional.of(fieldAnnotation.value());
    } else {
      return Optional.empty();
    }
  }

  private int getPartitionKeyIndex(ExecutableElement getMethod, VariableElement field) {
    PartitionKey getterAnnotation = getMethod.getAnnotation(PartitionKey.class);
    PartitionKey fieldAnnotation = (field == null) ? null : field.getAnnotation(PartitionKey.class);

    if (getterAnnotation != null) {
      if (fieldAnnotation != null) {
        context
            .getMessager()
            .warn(
                field,
                "@%s should be used either on the field or the getter, but not both. "
                    + "The annotation on this field will be ignored.",
                PartitionKey.class.getSimpleName());
      }
      return getterAnnotation.value();
    } else if (fieldAnnotation != null) {
      return fieldAnnotation.value();
    } else {
      return -1;
    }
  }

  private int getClusteringColumnIndex(
      ExecutableElement getMethod, VariableElement field, int partitionKeyIndex) {
    ClusteringColumn getterAnnotation = getMethod.getAnnotation(ClusteringColumn.class);
    ClusteringColumn fieldAnnotation =
        (field == null) ? null : field.getAnnotation(ClusteringColumn.class);

    if (getterAnnotation != null) {
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
      return getterAnnotation.value();
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

  private CqlNameGenerator buildCqlNameGenerator(TypeElement classElement) {

    NamingStrategy namingStrategy = classElement.getAnnotation(NamingStrategy.class);
    if (namingStrategy == null) {
      return CqlNameGenerator.DEFAULT;
    }

    NamingConvention[] conventions = namingStrategy.convention();
    TypeMirror[] customConverterClasses = readCustomConverterClasses(classElement);

    if (conventions.length > 0 && customConverterClasses.length > 0) {
      context
          .getMessager()
          .error(
              classElement,
              "Invalid annotation configuration: %s must have either a 'convention' "
                  + "or 'customConverterClass' argument, but not both",
              NamingStrategy.class.getSimpleName());
      // Return a generator anyway, so that the processor doesn't crash downstream
      return new CqlNameGenerator(conventions[0]);
    } else if (conventions.length == 0 && customConverterClasses.length == 0) {
      context
          .getMessager()
          .error(
              classElement,
              "Invalid annotation configuration: %s must have either a 'convention' "
                  + "or 'customConverterClass' argument",
              NamingStrategy.class.getSimpleName());
      return CqlNameGenerator.DEFAULT;
    } else if (conventions.length > 0) {
      if (conventions.length > 1) {
        context
            .getMessager()
            .warn(
                classElement,
                "Too many naming conventions: %s must have at most one 'convention' "
                    + "argument (will use the first one: %s)",
                NamingStrategy.class.getSimpleName(),
                conventions[0]);
      }
      return new CqlNameGenerator(conventions[0]);
    } else {
      if (customConverterClasses.length > 1) {
        context
            .getMessager()
            .warn(
                classElement,
                "Too many custom converters: %s must have at most one "
                    + "'customConverterClass' argument (will use the first one: %s)",
                NamingStrategy.class.getSimpleName(),
                customConverterClasses[0]);
      }
      return new CqlNameGenerator(customConverterClasses[0]);
    }
  }

  private TypeMirror[] readCustomConverterClasses(TypeElement classElement) {
    // customConverterClass references a class that might not be compiled yet, so we can't read it
    // directly, we need to go through mirrors.
    AnnotationMirror annotationMirror = null;
    for (AnnotationMirror candidate : classElement.getAnnotationMirrors()) {
      if (context.getClassUtils().isSame(candidate.getAnnotationType(), NamingStrategy.class)) {
        annotationMirror = candidate;
        break;
      }
    }
    assert annotationMirror != null; // We've checked that in the caller already

    for (Map.Entry<? extends ExecutableElement, ? extends AnnotationValue> entry :
        annotationMirror.getElementValues().entrySet()) {
      if (entry.getKey().getSimpleName().contentEquals("customConverterClass")) {
        @SuppressWarnings("unchecked")
        List<? extends AnnotationValue> values = (List) entry.getValue().getValue();
        TypeMirror[] result = new TypeMirror[values.size()];
        for (int i = 0; i < values.size(); i++) {
          result[i] = ((TypeMirror) values.get(i).getValue());
        }
        return result;
      }
    }
    return new TypeMirror[0];
  }
}
