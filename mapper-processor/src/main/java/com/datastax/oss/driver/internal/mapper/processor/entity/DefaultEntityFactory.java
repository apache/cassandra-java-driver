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
import com.datastax.oss.driver.api.mapper.annotations.Computed;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.NamingStrategy;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.datastax.oss.driver.api.mapper.annotations.PropertyStrategy;
import com.datastax.oss.driver.api.mapper.annotations.Transient;
import com.datastax.oss.driver.api.mapper.annotations.TransientProperties;
import com.datastax.oss.driver.api.mapper.entity.naming.GetterStyle;
import com.datastax.oss.driver.api.mapper.entity.naming.NamingConvention;
import com.datastax.oss.driver.api.mapper.entity.naming.SetterStyle;
import com.datastax.oss.driver.internal.mapper.processor.ProcessorContext;
import com.datastax.oss.driver.internal.mapper.processor.util.AnnotationScanner;
import com.datastax.oss.driver.internal.mapper.processor.util.Capitalizer;
import com.datastax.oss.driver.internal.mapper.processor.util.HierarchyScanner;
import com.datastax.oss.driver.internal.mapper.processor.util.ResolvedAnnotation;
import com.datastax.oss.driver.internal.mapper.processor.util.generation.PropertyType;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import com.datastax.oss.driver.shaded.guava.common.collect.Maps;
import com.datastax.oss.driver.shaded.guava.common.collect.Sets;
import com.squareup.javapoet.ClassName;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.lang.annotation.Annotation;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.AnnotationValue;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.Name;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;

public class DefaultEntityFactory implements EntityFactory {

  private final ProcessorContext context;

  // property annotations of which only 1 is allowed on a property
  private static final Set<Class<? extends Annotation>> EXCLUSIVE_PROPERTY_ANNOTATIONS =
      ImmutableSet.of(ClusteringColumn.class, PartitionKey.class, Transient.class, Computed.class);

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
  public EntityDefinition getDefinition(TypeElement processedClass) {
    Set<TypeMirror> types = HierarchyScanner.resolveTypeHierarchy(processedClass, context);
    Set<TypeElement> typeHierarchy = Sets.newLinkedHashSet();
    for (TypeMirror type : types) {
      typeHierarchy.add((TypeElement) context.getTypeUtils().asElement(type));
    }

    Language language = Language.detect(typeHierarchy);

    Optional<PropertyStrategy> propertyStrategy = getPropertyStrategy(typeHierarchy);
    GetterStyle getterStyle =
        propertyStrategy.map(PropertyStrategy::getterStyle).orElse(language.defaultGetterStyle);
    SetterStyle setterStyle =
        propertyStrategy.map(PropertyStrategy::setterStyle).orElse(language.defaultSetterStyle);
    boolean mutable =
        propertyStrategy.map(PropertyStrategy::mutable).orElse(language.defaultMutable);
    CqlNameGenerator cqlNameGenerator = buildCqlNameGenerator(typeHierarchy);
    Set<String> transientProperties = getTransientPropertyNames(typeHierarchy);

    Set<String> encounteredPropertyNames = Sets.newHashSet();
    SortedMap<Integer, PropertyDefinition> partitionKey = new TreeMap<>();
    SortedMap<Integer, PropertyDefinition> clusteringColumns = new TreeMap<>();
    ImmutableList.Builder<PropertyDefinition> regularColumns = ImmutableList.builder();
    ImmutableList.Builder<PropertyDefinition> computedValues = ImmutableList.builder();

    // scan hierarchy for properties
    for (TypeElement typeElement : typeHierarchy) {
      for (Element child : typeElement.getEnclosedElements()) {
        Set<Modifier> modifiers = child.getModifiers();
        if (child.getKind() != ElementKind.METHOD
            || modifiers.contains(Modifier.STATIC)
            || modifiers.contains(Modifier.PRIVATE)) {
          continue;
        }
        ExecutableElement getMethod = (ExecutableElement) child;
        if (!getMethod.getParameters().isEmpty()) {
          continue;
        }
        TypeMirror typeMirror = getMethod.getReturnType();
        if (typeMirror.getKind() == TypeKind.VOID) {
          continue;
        }

        String getMethodName = getMethod.getSimpleName().toString();

        // Skip methods that test as false positives with the fluent getter style: toString(),
        // hashCode() and a few Scala or Kotlin methods.
        if (getMethodName.equals("toString")
            || getMethodName.equals("hashCode")
            || (language == Language.SCALA_CASE_CLASS
                && (getMethodName.equals("productPrefix")
                    || getMethodName.equals("productArity")
                    || getMethodName.equals("productIterator")
                    || getMethodName.equals("productElementNames")
                    || getMethodName.startsWith("copy$default$")))
            || (language == Language.KOTLIN_DATA_CLASS
                && getMethodName.matches("component[0-9]+"))) {
          continue;
        }

        String propertyName = inferPropertyName(getMethodName, getterStyle, typeMirror);
        if (propertyName == null) {
          // getMethodName does not follow a known pattern => this is not a getter, skip
          continue;
        }

        // skip properties we've already encountered.
        if (encounteredPropertyNames.contains(propertyName)) {
          continue;
        }

        String setMethodName;
        if (mutable) {
          setMethodName = inferSetMethodName(propertyName, setterStyle);
          ExecutableElement setMethod = findSetMethod(typeHierarchy, setMethodName, typeMirror);
          if (setMethod == null) {
            continue; // must have both
          }
        } else {
          setMethodName = null;
        }
        VariableElement field = findField(typeHierarchy, propertyName, typeMirror);

        Map<Class<? extends Annotation>, Annotation> propertyAnnotations =
            scanPropertyAnnotations(typeHierarchy, getMethod, field);
        if (isTransient(propertyAnnotations, propertyName, transientProperties, getMethod, field)) {
          continue;
        }

        int partitionKeyIndex = getPartitionKeyIndex(propertyAnnotations);
        int clusteringColumnIndex = getClusteringColumnIndex(propertyAnnotations);
        Optional<String> customCqlName = getCustomCqlName(propertyAnnotations);
        Optional<String> computedFormula =
            getComputedFormula(propertyAnnotations, getMethod, field);

        PropertyType propertyType = PropertyType.parse(typeMirror, context);
        PropertyDefinition property =
            new DefaultPropertyDefinition(
                propertyName,
                customCqlName,
                computedFormula,
                getMethodName,
                setMethodName,
                propertyType,
                cqlNameGenerator);
        encounteredPropertyNames.add(propertyName);

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
        } else if (computedFormula.isPresent()) {
          computedValues.add(property);
        } else {
          regularColumns.add(property);
        }
      }
    }

    if (encounteredPropertyNames.isEmpty()) {
      context
          .getMessager()
          .error(
              processedClass,
              "@%s-annotated class must have at least one property defined.",
              Entity.class.getSimpleName());
    }

    String entityName = Capitalizer.decapitalize(processedClass.getSimpleName().toString());
    String defaultKeyspace = processedClass.getAnnotation(Entity.class).defaultKeyspace();

    EntityDefinition entityDefinition =
        new DefaultEntityDefinition(
            ClassName.get(processedClass),
            entityName,
            defaultKeyspace.isEmpty() ? null : defaultKeyspace,
            Optional.ofNullable(processedClass.getAnnotation(CqlName.class)).map(CqlName::value),
            ImmutableList.copyOf(partitionKey.values()),
            ImmutableList.copyOf(clusteringColumns.values()),
            regularColumns.build(),
            computedValues.build(),
            cqlNameGenerator,
            mutable);
    validateConstructor(entityDefinition, processedClass);
    return entityDefinition;
  }

  private String inferPropertyName(String getMethodName, GetterStyle getterStyle, TypeMirror type) {
    switch (getterStyle) {
      case FLUENT:
        return getMethodName;
      case JAVABEANS:
        if (getMethodName.startsWith("get") && getMethodName.length() > 3) {
          return Capitalizer.decapitalize(getMethodName.substring(3));
        } else if (getMethodName.startsWith("is")
            && getMethodName.length() > 2
            && (type.getKind() == TypeKind.BOOLEAN
                || context.getClassUtils().isSame(type, Boolean.class))) {
          return Capitalizer.decapitalize(getMethodName.substring(2));
        } else {
          return null;
        }
      default:
        throw new AssertionError("Unsupported getter style " + getterStyle);
    }
  }

  private String inferSetMethodName(String propertyName, SetterStyle setterStyle) {
    String setMethodName;
    switch (setterStyle) {
      case JAVABEANS:
        setMethodName = "set" + Capitalizer.capitalize(propertyName);
        break;
      case FLUENT:
        setMethodName = propertyName;
        break;
      default:
        throw new AssertionError("Unsupported setter style " + setterStyle);
    }
    return setMethodName;
  }

  @Nullable
  private VariableElement findField(
      Set<TypeElement> typeHierarchy, String propertyName, TypeMirror fieldType) {
    for (TypeElement classElement : typeHierarchy) {
      // skip interfaces as they can't have fields
      if (classElement.getKind().isInterface()) {
        continue;
      }
      for (Element child : classElement.getEnclosedElements()) {
        if (child.getKind() != ElementKind.FIELD) {
          continue;
        }
        VariableElement field = (VariableElement) child;
        if (field.getSimpleName().toString().equals(propertyName)
            && context.getTypeUtils().isAssignable(fieldType, field.asType())) {
          return field;
        }
      }
    }
    return null;
  }

  @Nullable
  private ExecutableElement findSetMethod(
      Set<TypeElement> typeHierarchy, String setMethodName, TypeMirror fieldType) {
    for (TypeElement classElement : typeHierarchy) {
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
            && context.getTypeUtils().isAssignable(fieldType, parameters.get(0).asType())) {
          return setMethod;
        }
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

  private Optional<String> getComputedFormula(
      Map<Class<? extends Annotation>, Annotation> annotations,
      ExecutableElement getMethod,
      @Nullable VariableElement field) {
    Computed annotation = (Computed) annotations.get(Computed.class);

    if (annotation != null) {
      // ensure formula is non-empty
      String value = annotation.value();
      if (value.isEmpty()) {
        Element element =
            field != null && field.getAnnotation(Computed.class) != null ? field : getMethod;
        context.getMessager().error(element, "@Computed value should be non-empty.");
      }
      return Optional.of(value);
    }
    return Optional.empty();
  }

  private CqlNameGenerator buildCqlNameGenerator(Set<TypeElement> typeHierarchy) {
    Optional<ResolvedAnnotation<NamingStrategy>> annotation =
        AnnotationScanner.getClassAnnotation(NamingStrategy.class, typeHierarchy);
    if (!annotation.isPresent()) {
      return CqlNameGenerator.DEFAULT;
    }

    NamingStrategy namingStrategy = annotation.get().getAnnotation();
    // Safe cast because the annotation can only be used on types:
    TypeElement classElement = (TypeElement) annotation.get().getElement();
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

  private TypeMirror[] readCustomConverterClasses(Element classElement) {
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
      @Nullable VariableElement field) {

    Transient transientAnnotation = (Transient) annotations.get(Transient.class);
    // check if property name is included in @TransientProperties
    // -or- if property is annotated with @Transient
    // -or- if field has transient keyword modifier
    boolean isTransient =
        transientProperties.contains(propertyName)
            || transientAnnotation != null
            || (field != null && field.getModifiers().contains(Modifier.TRANSIENT));

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

  private Set<String> getTransientPropertyNames(Set<TypeElement> typeHierarchy) {
    Optional<ResolvedAnnotation<TransientProperties>> annotation =
        AnnotationScanner.getClassAnnotation(TransientProperties.class, typeHierarchy);

    return annotation.isPresent()
        ? Sets.newHashSet(annotation.get().getAnnotation().value())
        : Collections.emptySet();
  }

  private Optional<PropertyStrategy> getPropertyStrategy(Set<TypeElement> typeHierarchy) {
    return AnnotationScanner.getClassAnnotation(PropertyStrategy.class, typeHierarchy)
        .map(ResolvedAnnotation::getAnnotation);
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
      Set<TypeElement> typeHierarchy,
      ExecutableElement getMethod,
      @Nullable VariableElement field) {
    Map<Class<? extends Annotation>, Annotation> annotations = Maps.newHashMap();

    // scan methods first as they should take precedence.
    scanMethodAnnotations(typeHierarchy, getMethod, annotations);
    if (field != null) {
      scanFieldAnnotations(field, annotations);
    }

    return ImmutableMap.copyOf(annotations);
  }

  @Nullable
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
      VariableElement field, Map<Class<? extends Annotation>, Annotation> annotations) {
    Class<? extends Annotation> exclusiveAnnotation = getExclusiveAnnotation(annotations);
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
        if (!annotations.containsKey(annotationClass)) {
          annotations.put(annotationClass, annotation);
        }
      }
    }
  }

  private void scanMethodAnnotations(
      Set<TypeElement> typeHierarchy,
      ExecutableElement getMethod,
      Map<Class<? extends Annotation>, Annotation> annotations) {
    Class<? extends Annotation> exclusiveAnnotation = getExclusiveAnnotation(annotations);
    for (Class<? extends Annotation> annotationClass : PROPERTY_ANNOTATIONS) {
      Optional<? extends ResolvedAnnotation<? extends Annotation>> annotation =
          AnnotationScanner.getMethodAnnotation(annotationClass, getMethod, typeHierarchy);
      if (annotation.isPresent()) {
        if (EXCLUSIVE_PROPERTY_ANNOTATIONS.contains(annotationClass)) {
          if (exclusiveAnnotation == null) {
            exclusiveAnnotation = annotationClass;
          } else {
            reportMultipleAnnotationError(
                annotation.get().getElement(), exclusiveAnnotation, annotationClass);
          }
        }
        if (!annotations.containsKey(annotationClass)) {
          annotations.put(annotationClass, annotation.get().getAnnotation());
        }
      }
    }
  }

  private void validateConstructor(EntityDefinition entity, TypeElement processedClass) {
    if (entity.isMutable()) {
      validateNoArgConstructor(processedClass);
    } else {
      validateAllValuesConstructor(processedClass, entity.getAllValues());
    }
  }

  private void validateNoArgConstructor(TypeElement processedClass) {
    for (Element child : processedClass.getEnclosedElements()) {
      if (child.getKind() == ElementKind.CONSTRUCTOR) {
        ExecutableElement constructor = (ExecutableElement) child;
        Set<Modifier> modifiers = constructor.getModifiers();
        if (!modifiers.contains(Modifier.PRIVATE) && constructor.getParameters().isEmpty()) {
          return;
        }
      }
    }
    context
        .getMessager()
        .error(
            processedClass,
            "Mutable @%s-annotated class must have a no-arg constructor.",
            Entity.class.getSimpleName());
  }

  private void validateAllValuesConstructor(
      TypeElement processedClass, List<PropertyDefinition> columns) {
    for (Element child : processedClass.getEnclosedElements()) {
      if (child.getKind() == ElementKind.CONSTRUCTOR) {
        ExecutableElement constructor = (ExecutableElement) child;
        Set<Modifier> modifiers = constructor.getModifiers();
        if (!modifiers.contains(Modifier.PRIVATE)
            && areAssignable(columns, constructor.getParameters())) {
          return;
        }
      }
    }
    String signature =
        columns.stream()
            .map(
                column ->
                    String.format("%s %s", column.getType().asTypeMirror(), column.getJavaName()))
            .collect(Collectors.joining(", "));
    context
        .getMessager()
        .error(
            processedClass,
            "Immutable @%s-annotated class must have an \"all values\" constructor. "
                + "Expected signature: %s(%s).",
            Entity.class.getSimpleName(),
            processedClass.getSimpleName(),
            signature);
  }

  private boolean areAssignable(
      List<PropertyDefinition> columns, List<? extends VariableElement> parameters) {
    if (columns.size() != parameters.size()) {
      return false;
    } else {
      for (int i = 0; i < columns.size(); i++) {
        // What the generated code will pass to the constructor:
        TypeMirror argumentType = columns.get(i).getType().asTypeMirror();
        // What the constructor declares:
        TypeMirror parameterType = parameters.get(i).asType();
        if (!context.getTypeUtils().isAssignable(argumentType, parameterType)) {
          return false;
        }
      }
      return true;
    }
  }

  /**
   * The source language (and construct) of an entity type. It impacts the defaults for entities
   * that do not explicitly declare the {@link PropertyStrategy} annotation.
   */
  private enum Language {
    SCALA_CASE_CLASS(false, GetterStyle.FLUENT, null),
    KOTLIN_DATA_CLASS(false, GetterStyle.JAVABEANS, null),
    JAVA14_RECORD(false, GetterStyle.FLUENT, null),
    UNKNOWN(true, GetterStyle.JAVABEANS, SetterStyle.JAVABEANS),
    ;

    final boolean defaultMutable;
    final GetterStyle defaultGetterStyle;
    final SetterStyle defaultSetterStyle;

    Language(
        boolean defaultMutable, GetterStyle defaultGetterStyle, SetterStyle defaultSetterStyle) {
      this.defaultMutable = defaultMutable;
      this.defaultGetterStyle = defaultGetterStyle;
      this.defaultSetterStyle = defaultSetterStyle;
    }

    static Language detect(Set<TypeElement> typeHierarchy) {
      for (TypeElement type : typeHierarchy) {
        if (isNamed(type, "scala.Product")) {
          return SCALA_CASE_CLASS;
        }
        if (isNamed(type, "java.lang.Record")) {
          return JAVA14_RECORD;
        }
      }

      TypeElement entityClass = typeHierarchy.iterator().next();
      // Kotlin adds `@kotlin.Metadata` on every generated class, we also check `component1` which
      // is a generated method specific to data classes (to eliminate regular Kotlin classes).
      if (entityClass.getAnnotationMirrors().stream().anyMatch(Language::isKotlinMetadata)
          && entityClass.getEnclosedElements().stream()
              .anyMatch(e -> isMethodNamed(e, "component1"))) {
        return KOTLIN_DATA_CLASS;
      }

      return UNKNOWN;
    }

    private static boolean isNamed(TypeElement type, String expectedName) {
      Name name = type.getQualifiedName();
      return name != null && name.toString().equals(expectedName);
    }

    private static boolean isKotlinMetadata(AnnotationMirror a) {
      DeclaredType declaredType = a.getAnnotationType();
      if (declaredType.getKind() == TypeKind.DECLARED) {
        TypeElement element = (TypeElement) declaredType.asElement();
        return element.getQualifiedName().toString().equals("kotlin.Metadata");
      }
      return false;
    }

    private static boolean isMethodNamed(Element element, String methodName) {
      return element.getKind() == ElementKind.METHOD
          && element.getSimpleName().toString().equals(methodName);
    }
  }
}
