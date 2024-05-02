/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.internal.mapper.processor.dao;

import static com.datastax.oss.driver.api.mapper.MapperBuilder.SCHEMA_VALIDATION_ENABLED_SETTING;

import com.datastax.dse.driver.internal.mapper.reactive.ReactiveDaoBase;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.mapper.MapperContext;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.DefaultNullSavingStrategy;
import com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy;
import com.datastax.oss.driver.internal.core.util.concurrent.BlockingOperation;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.driver.internal.mapper.DaoBase;
import com.datastax.oss.driver.internal.mapper.processor.GeneratedNames;
import com.datastax.oss.driver.internal.mapper.processor.MethodGenerator;
import com.datastax.oss.driver.internal.mapper.processor.ProcessorContext;
import com.datastax.oss.driver.internal.mapper.processor.SingleFileCodeGenerator;
import com.datastax.oss.driver.internal.mapper.processor.util.Capitalizer;
import com.datastax.oss.driver.internal.mapper.processor.util.HierarchyScanner;
import com.datastax.oss.driver.internal.mapper.processor.util.NameIndex;
import com.datastax.oss.driver.internal.mapper.processor.util.generation.GenericTypeConstantGenerator;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import com.datastax.oss.driver.shaded.guava.common.collect.Maps;
import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.Name;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.type.TypeVariable;

public class DaoImplementationGenerator extends SingleFileCodeGenerator
    implements DaoImplementationSharedCode {

  private static final TypeName PREPARED_STATEMENT_STAGE =
      ParameterizedTypeName.get(CompletionStage.class, PreparedStatement.class);

  private final TypeElement interfaceElement;
  private final ClassName implementationName;
  private final NameIndex nameIndex = new NameIndex();
  private final GenericTypeConstantGenerator genericTypeConstantGenerator =
      new GenericTypeConstantGenerator(nameIndex);
  private final Map<ClassName, String> entityHelperFields = new LinkedHashMap<>();
  private final List<GeneratedPreparedStatement> preparedStatements = new ArrayList<>();
  private final List<GeneratedQueryProvider> queryProviders = new ArrayList<>();
  private final NullSavingStrategyValidation nullSavingStrategyValidation;

  // tracks interface type variable mappings as child interfaces discover them.
  private final Map<TypeMirror, Map<Name, TypeElement>> typeMappingsForInterface =
      Maps.newHashMap();

  private final Set<TypeMirror> interfaces;
  private final Map<Class<? extends Annotation>, Annotation> annotations;

  private static final Set<Class<? extends Annotation>> ANNOTATIONS_TO_SCAN =
      ImmutableSet.of(DefaultNullSavingStrategy.class);

  public DaoImplementationGenerator(TypeElement interfaceElement, ProcessorContext context) {
    super(context);
    this.interfaceElement = interfaceElement;
    this.interfaces = HierarchyScanner.resolveTypeHierarchy(interfaceElement, context);
    this.annotations = scanAnnotations();
    implementationName = GeneratedNames.daoImplementation(interfaceElement);
    nullSavingStrategyValidation = new NullSavingStrategyValidation(context);
  }

  private Map<Class<? extends Annotation>, Annotation> scanAnnotations() {
    Map<Class<? extends Annotation>, Annotation> annotations = Maps.newHashMap();
    for (TypeMirror mirror : interfaces) {
      Element element = context.getTypeUtils().asElement(mirror);
      for (Class<? extends Annotation> annotationClass : ANNOTATIONS_TO_SCAN) {
        Annotation annotation = element.getAnnotation(annotationClass);
        if (annotation != null) {
          // don't replace annotations from lower levels.
          annotations.putIfAbsent(annotationClass, annotation);
        }
      }
    }
    return ImmutableMap.copyOf(annotations);
  }

  private <A extends Annotation> Optional<A> getAnnotation(Class<A> annotationClass) {
    return Optional.ofNullable(annotationClass.cast(annotations.get(annotationClass)));
  }

  @Override
  public NameIndex getNameIndex() {
    return nameIndex;
  }

  @Override
  public String addGenericTypeConstant(TypeName type) {
    return genericTypeConstantGenerator.add(type);
  }

  @Override
  public String addEntityHelperField(ClassName entityClassName) {
    ClassName helperClass = GeneratedNames.entityHelper(entityClassName);
    return entityHelperFields.computeIfAbsent(
        helperClass,
        k -> {
          String baseName = Capitalizer.decapitalize(entityClassName.simpleName()) + "Helper";
          return nameIndex.uniqueField(baseName);
        });
  }

  @Override
  public String addPreparedStatement(
      ExecutableElement methodElement,
      BiConsumer<MethodSpec.Builder, String> simpleStatementGenerator) {
    // Prepared statements are not shared between methods, so always generate a new name
    String fieldName =
        nameIndex.uniqueField(methodElement.getSimpleName().toString() + "Statement");
    preparedStatements.add(
        new GeneratedPreparedStatement(methodElement, fieldName, simpleStatementGenerator));
    return fieldName;
  }

  @Override
  public String addQueryProvider(
      ExecutableElement methodElement,
      TypeMirror providerClass,
      List<ClassName> entityHelperTypes) {
    // Invokers are not shared between methods, so always generate a new name
    String fieldName = nameIndex.uniqueField(methodElement.getSimpleName().toString() + "Invoker");
    List<String> entityHelperNames = new ArrayList<>();
    for (ClassName type : entityHelperTypes) {
      entityHelperNames.add(addEntityHelperField(type));
    }
    queryProviders.add(new GeneratedQueryProvider(fieldName, providerClass, entityHelperNames));
    return fieldName;
  }

  @Override
  public Optional<NullSavingStrategy> getNullSavingStrategy() {
    return getAnnotation(DefaultNullSavingStrategy.class).map(DefaultNullSavingStrategy::value);
  }

  @Override
  protected ClassName getPrincipalTypeName() {
    return implementationName;
  }

  /*
   * Parses the given interface mirror and returns a mapping of type variable names to their
   * resolved concrete declared type element.
   *
   * Also updates typeMappingsForInterface for parent interfaces of this interface with
   * the type variable to concrete type mappings declared on this interface. This is needed to
   * resolve declared types parameter values between the interface hierarchy.
   *
   * For example, given the following hierarchy:
   *
   *   interface BaseDao<T>
   *   interface NamedDeviceDao<Y extends Device> extends BaseDao<Y>
   *   interface TrackedDeviceDao extends NamedDeviceDao<TrackedDevice>
   *
   * In getContents(), the following mirrors would be parsed for type parameters when
   * generating code for TrackedDeviceDao:
   *
   *  * parseTypeParameters(TrackedDeviceDao): returns empty map
   *  * parseTypeParameters(NamedDeviceDao<TrackedDevice>): returns Y -> TrackedDevice,
   *    typeMappingsForInterface(BaseDao<Y>) updated with <code>Y -> TrackedDevice
   *  * parseTypeParameters(BaseDao<Y>): returns T -> TrackedDevice
   *
   */
  private Map<Name, TypeElement> parseTypeParameters(TypeMirror mirror) {
    // Map interface type variable names to their concrete class.
    Map<Name, TypeElement> typeParameters = Maps.newHashMap();
    Map<Name, TypeElement> childTypeParameters =
        typeMappingsForInterface.getOrDefault(mirror, Collections.emptyMap());

    if (mirror instanceof DeclaredType) {
      TypeElement element = (TypeElement) context.getTypeUtils().asElement(mirror);
      DeclaredType declaredType = (DeclaredType) mirror;
      // For each type argument on this interface, resolve the declared (concrete) type
      for (int i = 0; i < declaredType.getTypeArguments().size(); i++) {
        Name name = element.getTypeParameters().get(i).getSimpleName();
        TypeMirror typeArgument = declaredType.getTypeArguments().get(i);
        if (typeArgument instanceof DeclaredType) {
          /* If its a DeclaredType, we have the concrete type.
           *
           * For example, given:
           *  * interface: NamedDeviceDao<Y extends Device> extends BaseDao<Y>
           *  * mirror: NamedDeviceDao<TrackedDevice>
           *
           * Type parameter name would be 'Y', type argument would be declared type 'TrackedDevice',
           * enabling mapping Y -> TrackedDevice.
           */
          Element concreteType = ((DeclaredType) typeArgument).asElement();
          typeParameters.put(name, (TypeElement) concreteType);
        } else if (typeArgument instanceof TypeVariable) {
          /* If its a TypeVariable, we resolve the concrete type from type parameters declared in
           * a child interface and alias it to the type on this class.
           *
           * For example, given:
           * * interface: BaseDao<T>
           * * mirror: BaseDao<Y>
           *
           * Type parameter name would be 'T', type argument would be declared type 'Y',
           * enabling mapping T -> Y.
           */
          Name typeVariableName = ((TypeVariable) typeArgument).asElement().getSimpleName();
          /* Resolve the concrete type from previous child interfaces we parsed types for.
           *
           * For example, given a child with:
           * * interface: NamedDeviceDao<Y extends Device> extends BaseDao<Y>
           * * mirror: NamedDeviceDao<TrackedDevice>
           *
           * We would have childTypeParameters mapping Y -> TrackedDevice enabling the resolution
           * of T -> TrackedDevice from T -> Y -> TrackedDevice.
           */
          if (childTypeParameters.containsKey(typeVariableName)) {
            typeParameters.put(name, childTypeParameters.get(typeVariableName));
          } else {
            context
                .getMessager()
                .error(
                    element,
                    "Could not resolve type parameter %s "
                        + "on %s from child interfaces. This error usually means an interface "
                        + "was inappropriately annotated with @%s. Interfaces should only be annotated "
                        + "with @%s if all generic type variables are declared.",
                    name,
                    mirror,
                    Dao.class.getSimpleName(),
                    Dao.class.getSimpleName());
          }
        }
      }

      /* For each parent interface of this type, check that parent's type arguments for
       * type variables.  For each of these variables keep track of the discovered concrete
       * type on the current interface so it has access to it.  See the comments in the code
       * above for an explanation of how these mappings are used.
       */
      for (TypeMirror parentInterface : element.getInterfaces()) {
        if (parentInterface instanceof DeclaredType) {
          Map<Name, TypeElement> typeMappingsForParent =
              typeMappingsForInterface.computeIfAbsent(parentInterface, k -> Maps.newHashMap());
          DeclaredType parentInterfaceType = (DeclaredType) parentInterface;
          for (TypeMirror parentTypeArgument : parentInterfaceType.getTypeArguments()) {
            if (parentTypeArgument instanceof TypeVariable) {
              TypeVariable parentTypeVariable = (TypeVariable) parentTypeArgument;
              Name parentTypeName = parentTypeVariable.asElement().getSimpleName();
              TypeElement typeElement = typeParameters.get(parentTypeName);
              if (typeElement != null) {
                typeMappingsForParent.put(parentTypeName, typeElement);
              }
            }
          }
        }
      }
    }

    return typeParameters;
  }

  @Override
  protected JavaFile.Builder getContents() {

    TypeSpec.Builder classBuilder =
        TypeSpec.classBuilder(implementationName)
            .addJavadoc(JAVADOC_GENERATED_WARNING)
            .addAnnotation(
                AnnotationSpec.builder(SuppressWarnings.class)
                    .addMember("value", "\"all\"")
                    .build())
            .addModifiers(Modifier.PUBLIC)
            .addSuperinterface(ClassName.get(interfaceElement));

    boolean reactive = false;
    for (TypeMirror mirror : interfaces) {
      TypeElement parentInterfaceElement = (TypeElement) context.getTypeUtils().asElement(mirror);
      Map<Name, TypeElement> typeParameters = parseTypeParameters(mirror);

      for (Element child : parentInterfaceElement.getEnclosedElements()) {
        if (child.getKind() == ElementKind.METHOD) {
          ExecutableElement methodElement = (ExecutableElement) child;
          Set<Modifier> modifiers = methodElement.getModifiers();
          if (!modifiers.contains(Modifier.STATIC) && !modifiers.contains(Modifier.DEFAULT)) {
            Optional<MethodGenerator> maybeGenerator =
                context
                    .getCodeGeneratorFactory()
                    .newDaoImplementationMethod(
                        methodElement, typeParameters, interfaceElement, this);
            if (!maybeGenerator.isPresent()) {
              context
                  .getMessager()
                  .error(
                      methodElement,
                      "Unrecognized method signature: no implementation will be generated");
            } else {
              maybeGenerator.flatMap(MethodGenerator::generate).ifPresent(classBuilder::addMethod);
              reactive |= maybeGenerator.get().requiresReactive();
            }
          }
        }
      }
    }

    classBuilder = classBuilder.superclass(getDaoParentClass(reactive));

    genericTypeConstantGenerator.generate(classBuilder);

    MethodSpec.Builder initAsyncBuilder = getInitAsyncContents();

    MethodSpec.Builder initBuilder = getInitContents();

    MethodSpec.Builder constructorBuilder =
        MethodSpec.constructorBuilder()
            .addModifiers(Modifier.PRIVATE)
            .addParameter(MapperContext.class, "context")
            .addStatement("super(context)");

    context.getLoggingGenerator().addLoggerField(classBuilder, implementationName);

    // For each entity helper that was requested by a method generator, create a field for it and
    // add a constructor parameter for it (the instance gets created in initAsync).
    for (Map.Entry<ClassName, String> entry : entityHelperFields.entrySet()) {
      ClassName fieldTypeName = entry.getKey();
      String fieldName = entry.getValue();

      classBuilder.addField(
          FieldSpec.builder(fieldTypeName, fieldName, Modifier.PRIVATE, Modifier.FINAL).build());
      constructorBuilder
          .addParameter(fieldTypeName, fieldName)
          .addStatement("this.$1L = $1L", fieldName);
    }

    // Same for prepared statements:
    for (GeneratedPreparedStatement preparedStatement : preparedStatements) {
      classBuilder.addField(
          FieldSpec.builder(
                  PreparedStatement.class,
                  preparedStatement.fieldName,
                  Modifier.PRIVATE,
                  Modifier.FINAL)
              .build());
      constructorBuilder
          .addParameter(PreparedStatement.class, preparedStatement.fieldName)
          .addStatement("this.$1L = $1L", preparedStatement.fieldName);
    }

    // Same for method invokers:
    for (GeneratedQueryProvider queryProvider : queryProviders) {
      TypeName providerClassName = TypeName.get(queryProvider.providerClass);
      classBuilder.addField(
          providerClassName, queryProvider.fieldName, Modifier.PRIVATE, Modifier.FINAL);
      constructorBuilder
          .addParameter(providerClassName, queryProvider.fieldName)
          .addStatement("this.$1L = $1L", queryProvider.fieldName);
    }

    classBuilder.addMethod(initAsyncBuilder.build());
    classBuilder.addMethod(initBuilder.build());
    classBuilder.addMethod(constructorBuilder.build());

    return JavaFile.builder(implementationName.packageName(), classBuilder.build());
  }

  @NonNull
  protected Class<?> getDaoParentClass(boolean requiresReactive) {
    if (requiresReactive) {
      return ReactiveDaoBase.class;
    } else {
      return DaoBase.class;
    }
  }

  /**
   * Generates the DAO's initAsync() builder: this is the entry point, that the main mapper will use
   * to build instances.
   *
   * <p>In this method we want to instantiate any entity helper or prepared statement that will be
   * needed by methods of the DAO. Then we call the DAO's private constructor, passing that
   * information.
   */
  private MethodSpec.Builder getInitAsyncContents() {
    MethodSpec.Builder initAsyncBuilder =
        MethodSpec.methodBuilder("initAsync")
            .returns(
                ParameterizedTypeName.get(
                    ClassName.get(CompletableFuture.class), ClassName.get(interfaceElement)))
            .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
            .addParameter(MapperContext.class, "context");

    LoggingGenerator loggingGenerator = context.getLoggingGenerator();
    loggingGenerator.debug(
        initAsyncBuilder,
        "[{}] Initializing new instance for keyspace = {} and table = {}",
        CodeBlock.of("context.getSession().getName()"),
        CodeBlock.of("context.getKeyspaceId()"),
        CodeBlock.of("context.getTableId()"));

    generateProtocolVersionCheck(initAsyncBuilder);

    initAsyncBuilder.beginControlFlow("try");

    // Start a constructor call: we build it dynamically because the number of parameters depends on
    // the entity helpers and prepared statements below.
    CodeBlock.Builder newDaoStatement = CodeBlock.builder();
    newDaoStatement.add("new $1T(context$>$>", implementationName);

    initAsyncBuilder.addComment("Initialize all entity helpers");
    // For each entity helper that was requested by a method generator:
    for (Map.Entry<ClassName, String> entry : entityHelperFields.entrySet()) {
      ClassName fieldTypeName = entry.getKey();
      String fieldName = entry.getValue();
      // - create an instance
      initAsyncBuilder.addStatement("$1T $2L = new $1T(context)", fieldTypeName, fieldName);
      // - validate entity schema
      generateValidationCheck(initAsyncBuilder, fieldName);
      // - add it as a parameter to the constructor call
      newDaoStatement.add(",\n$L", fieldName);
    }

    initAsyncBuilder.addStatement(
        "$T<$T> prepareStages = new $T<>()", List.class, PREPARED_STATEMENT_STAGE, ArrayList.class);
    // For each prepared statement that was requested by a method generator:
    for (GeneratedPreparedStatement preparedStatement : preparedStatements) {
      initAsyncBuilder.addComment(
          "Prepare the statement for `$L`:", preparedStatement.methodElement.toString());
      // - generate the simple statement
      String simpleStatementName = preparedStatement.fieldName + "_simple";
      preparedStatement.simpleStatementGenerator.accept(initAsyncBuilder, simpleStatementName);
      // - prepare it asynchronously, store all CompletionStages in a list
      loggingGenerator.debug(
          initAsyncBuilder,
          String.format(
              "[{}] Preparing query `{}` for method %s",
              preparedStatement.methodElement.toString()),
          CodeBlock.of("context.getSession().getName()"),
          CodeBlock.of("$L.getQuery()", simpleStatementName));
      initAsyncBuilder
          .addStatement(
              "$T $L = prepare($L, context)",
              PREPARED_STATEMENT_STAGE,
              preparedStatement.fieldName,
              simpleStatementName)
          .addStatement("prepareStages.add($L)", preparedStatement.fieldName);
      // - add the stage's result to the constructor call (which will be executed once all stages
      //   are complete)
      newDaoStatement.add(
          ",\n$T.getCompleted($L)", CompletableFutures.class, preparedStatement.fieldName);
    }

    initAsyncBuilder.addComment("Initialize all method invokers");
    // For each query provider that was requested by a method generator:
    for (GeneratedQueryProvider queryProvider : queryProviders) {
      // - create an instance
      initAsyncBuilder.addCode(
          "$[$1T $2L = new $1T(context", queryProvider.providerClass, queryProvider.fieldName);
      for (String helperName : queryProvider.entityHelperNames) {
        initAsyncBuilder.addCode(", $L", helperName);
      }
      initAsyncBuilder.addCode(");$]\n");

      // - add it as a parameter to the constructor call
      newDaoStatement.add(",\n$L", queryProvider.fieldName);
    }

    newDaoStatement.add(")");

    initAsyncBuilder
        .addComment("Build the DAO when all statements are prepared")
        .addCode("$[return $T.allSuccessful(prepareStages)", CompletableFutures.class)
        .addCode("\n.thenApply(v -> ($T) ", interfaceElement)
        .addCode(newDaoStatement.build())
        .addCode(")\n$<$<.toCompletableFuture();$]\n")
        .nextControlFlow("catch ($T t)", Throwable.class)
        .addStatement("return $T.failedFuture(t)", CompletableFutures.class)
        .endControlFlow();
    return initAsyncBuilder;
  }

  private void generateValidationCheck(MethodSpec.Builder initAsyncBuilder, String fieldName) {
    initAsyncBuilder.beginControlFlow(
        "if (($1T)context.getCustomState().get($2S))",
        Boolean.class,
        SCHEMA_VALIDATION_ENABLED_SETTING);
    initAsyncBuilder.addStatement("$1L.validateEntityFields()", fieldName);
    initAsyncBuilder.endControlFlow();
  }

  private void generateProtocolVersionCheck(MethodSpec.Builder builder) {
    List<ExecutableElement> methodElements =
        preparedStatements.stream().map(v -> v.methodElement).collect(Collectors.toList());
    DefaultNullSavingStrategy interfaceAnnotation =
        getAnnotation(DefaultNullSavingStrategy.class).orElse(null);
    if (nullSavingStrategyValidation.hasDoNotSetOnAnyLevel(methodElements, interfaceAnnotation)) {
      builder.addStatement("throwIfProtocolVersionV3(context)");
    }
  }

  /** Generates the DAO's init() method: it's a simple synchronous wrapper of initAsync(). */
  private MethodSpec.Builder getInitContents() {
    return MethodSpec.methodBuilder("init")
        .returns(ClassName.get(interfaceElement))
        .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
        .addParameter(MapperContext.class, "context")
        .addStatement("$T.checkNotDriverThread()", BlockingOperation.class)
        .addStatement("return $T.getUninterruptibly(initAsync(context))", CompletableFutures.class);
  }

  private static class GeneratedPreparedStatement {
    final ExecutableElement methodElement;
    final String fieldName;
    final BiConsumer<MethodSpec.Builder, String> simpleStatementGenerator;

    GeneratedPreparedStatement(
        ExecutableElement methodElement,
        String fieldName,
        BiConsumer<MethodSpec.Builder, String> simpleStatementGenerator) {
      this.methodElement = methodElement;
      this.fieldName = fieldName;
      this.simpleStatementGenerator = simpleStatementGenerator;
    }
  }

  private static class GeneratedQueryProvider {
    final String fieldName;
    final TypeMirror providerClass;
    final List<String> entityHelperNames;

    GeneratedQueryProvider(
        String fieldName, TypeMirror providerClass, List<String> entityHelperNames) {
      this.fieldName = fieldName;
      this.providerClass = providerClass;
      this.entityHelperNames = entityHelperNames;
    }
  }
}
