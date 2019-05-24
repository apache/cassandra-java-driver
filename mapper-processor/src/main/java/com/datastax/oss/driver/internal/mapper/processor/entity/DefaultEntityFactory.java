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
import com.datastax.oss.driver.api.mapper.annotations.Transient;
import com.datastax.oss.driver.api.mapper.annotations.TransientProperties;
import com.datastax.oss.driver.api.mapper.entity.naming.NamingConvention;
import com.datastax.oss.driver.internal.mapper.processor.ProcessorContext;
import com.datastax.oss.driver.internal.mapper.processor.util.generation.PropertyType;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import com.datastax.oss.driver.shaded.guava.common.collect.Maps;
import com.datastax.oss.driver.shaded.guava.common.collect.Sets;
import com.squareup.javapoet.ClassName;
import java.beans.Introspector;
import java.lang.annotation.Annotation;
import java.util.Collections;
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

  // property annotations of which only 1 is allowed on a property
  private static final Set<Class<? extends Annotation>> EXCLUSIVE_PROPERTY_ANNOTATIONS =
      ImmutableSet.of(ClusteringColumn.class, PartitionKey.class, Transient.class);

  // all valid property annotations to scan for.
  private static final Set<Class<? extends Annotation>> PROPERTY_ANNOTATIONS =
      ImmutableSet.<Class<? extends Annotation>>builder()
          .addAll(EXCLUSIVE_PROPERTY_ANNOTATIONS)
          .add(CqlName.class)
          .build();

  public DefaultEntityFactory(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public EntityDefinition getDefinition(TypeElement classElement) {

    // TODO property annotations: computed...
    // TODO inherit annotations and properties from superclass / parent interface

    CqlNameGenerator cqlNameGenerator = buildCqlNameGenerator(classElement);
    Set<String> transientProperties = getTransientPropertyNames(classElement);

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

      Map<Class<? extends Annotation>, Annotation> propertyAnnotations =
          scanPropertyAnnotations(getMethod, field);
      if (isTransient(propertyAnnotations, propertyName, transientProperties, getMethod, field)) {
        continue;
      }

      int partitionKeyIndex = getPartitionKeyIndex(propertyAnnotations);
      int clusteringColumnIndex = getClusteringColumnIndex(propertyAnnotations);
      PropertyType propertyType = PropertyType.parse(typeMirror, context);
      PropertyDefinition property =
          new DefaultPropertyDefinition(
              propertyName,
              getCustomCqlName(propertyAnnotations),
              getMethodName,
              setMethodName,
              propertyType,
              cqlNameGenerator);

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

  private Optional<String> getCustomCqlName(
      Map<Class<? extends Annotation>, Annotation> annotations) {
    CqlName cqlName = (CqlName) annotations.get(CqlName.class);
    return cqlName != null ? Optional.of(cqlName.value()) : Optional.empty();
  }

  private int getPartitionKeyIndex(Map<Class<? extends Annotation>, Annotation> annotations) {
    PartitionKey partitionKey = (PartitionKey) annotations.get(PartitionKey.class);
    return partitionKey != null ? partitionKey.value() : -1;
  }

  private int getClusteringColumnIndex(Map<Class<? extends Annotation>, Annotation> annotations) {
    ClusteringColumn clusteringColumn = (ClusteringColumn) annotations.get(ClusteringColumn.class);
    return clusteringColumn != null ? clusteringColumn.value() : -1;
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

  private boolean isTransient(
      Map<Class<? extends Annotation>, Annotation> annotations,
      String propertyName,
      Set<String> transientProperties,
      ExecutableElement getMethod,
      VariableElement field) {

    Transient transientAnnotation = (Transient) annotations.get(Transient.class);
    // check if property name is included in @TransientProperties
    // -or- if property is annotated with @Transient
    // -or- if field has transient keyword modifier
    boolean isTransient =
        transientProperties.contains(propertyName)
            || transientAnnotation != null
            || field != null && field.getModifiers().contains(Modifier.TRANSIENT);

    // if annotations contains an exclusive annotation that isn't transient, raise
    // an error here.
    Class<? extends Annotation> exclusiveAnnotation = getExclusiveAnnotation(annotations);
    if (isTransient && transientAnnotation == null && exclusiveAnnotation != null) {
      Element element = field != null ? field : getMethod;
      context
          .getMessager()
          .error(
              element,
              "Property that is considered transient cannot be annotated with @%s.",
              exclusiveAnnotation.getSimpleName());
    }

    return isTransient;
  }

  private Set<String> getTransientPropertyNames(TypeElement classElement) {
    TransientProperties transientProperties = classElement.getAnnotation(TransientProperties.class);

    return transientProperties != null
        ? Sets.newHashSet(transientProperties.value())
        : Collections.emptySet();
  }

  private void reportMultipleAnnotationError(
      Element element, Class<? extends Annotation> a0, Class<? extends Annotation> a1) {
    if (a0 == a1) {
      context
          .getMessager()
          .warn(
              element,
              "@%s should be used either on the field or the getter, but not both. "
                  + "The annotation on this field will be ignored.",
              a0.getSimpleName());
    } else {
      context
          .getMessager()
          .error(
              element,
              "Properties can't be annotated with both @%s and @%s.",
              a0.getSimpleName(),
              a1.getSimpleName());
    }
  }

  private Map<Class<? extends Annotation>, Annotation> scanPropertyAnnotations(
      ExecutableElement getMethod, VariableElement field) {
    Map<Class<? extends Annotation>, Annotation> annotations = Maps.newHashMap();

    // scan methods first as they should take precedence.
    scanMethodAnnotations(getMethod, annotations, null);
    if (field != null) {
      scanFieldAnnotations(field, annotations, getExclusiveAnnotation(annotations));
    }

    return ImmutableMap.copyOf(annotations);
  }

  private Class<? extends Annotation> getExclusiveAnnotation(
      Map<Class<? extends Annotation>, Annotation> annotations) {
    for (Class<? extends Annotation> annotationClass : annotations.keySet()) {
      if (EXCLUSIVE_PROPERTY_ANNOTATIONS.contains(annotationClass)) {
        return annotationClass;
      }
    }
    return null;
  }

  private void scanFieldAnnotations(
      VariableElement field,
      Map<Class<? extends Annotation>, Annotation> annotations,
      Class<? extends Annotation> exclusiveAnnotation) {
    for (Class<? extends Annotation> annotationClass : PROPERTY_ANNOTATIONS) {
      Annotation annotation = field.getAnnotation(annotationClass);
      if (annotation != null) {
        if (EXCLUSIVE_PROPERTY_ANNOTATIONS.contains(annotationClass)) {
          if (exclusiveAnnotation == null) {
            exclusiveAnnotation = annotationClass;
          } else {
            reportMultipleAnnotationError(field, exclusiveAnnotation, annotationClass);
          }
        }
        annotations.put(annotationClass, annotation);
      }
    }
  }

  private void scanMethodAnnotations(
      ExecutableElement getMethod,
      Map<Class<? extends Annotation>, Annotation> annotations,
      Class<? extends Annotation> exclusiveAnnotation) {
    for (Class<? extends Annotation> annotationClass : PROPERTY_ANNOTATIONS) {
      Annotation annotation = getMethod.getAnnotation(annotationClass);
      if (annotation != null) {
        if (EXCLUSIVE_PROPERTY_ANNOTATIONS.contains(annotationClass)) {
          if (exclusiveAnnotation == null) {
            exclusiveAnnotation = annotationClass;
          } else {
            reportMultipleAnnotationError(getMethod, exclusiveAnnotation, annotationClass);
          }
        }
        annotations.put(annotationClass, annotation);
      }
    }
  }
}
