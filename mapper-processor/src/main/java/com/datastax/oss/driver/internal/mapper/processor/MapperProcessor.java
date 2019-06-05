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

import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.datastax.oss.driver.shaded.guava.common.base.Strings;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import com.google.auto.service.AutoService;
import com.google.common.base.Throwables;
import java.lang.annotation.Annotation;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Filer;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.Processor;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.TypeElement;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;

@AutoService(Processor.class)
public class MapperProcessor extends AbstractProcessor {
  private static final boolean DEFAULT_MAPPER_LOGS_ENABLED = true;

  private static final String INDENT_AMOUNT_OPTION = "com.datastax.oss.driver.mapper.indent";
  private static final String INDENT_WITH_TABS_OPTION =
      "com.datastax.oss.driver.mapper.indentWithTabs";
  private static final String MAPPER_LOGS_ENABLED_OPTION =
      "com.datastax.oss.driver.mapper.logs.enabled";

  private DecoratedMessager messager;
  private Types typeUtils;
  private Elements elementUtils;
  private Filer filer;
  private String indent;
  private boolean logsEnabled;

  @Override
  public synchronized void init(ProcessingEnvironment processingEnvironment) {
    super.init(processingEnvironment);
    messager = new DecoratedMessager(processingEnvironment.getMessager());
    typeUtils = processingEnvironment.getTypeUtils();
    elementUtils = processingEnvironment.getElementUtils();
    filer = processingEnvironment.getFiler();
    indent = computeIndent(processingEnvironment.getOptions());
    logsEnabled = isLogsEnabled(processingEnvironment.getOptions());
  }

  private boolean isLogsEnabled(Map<String, String> options) {
    String mapperLogsEnabled = options.get(MAPPER_LOGS_ENABLED_OPTION);
    if (mapperLogsEnabled != null) {
      return Boolean.parseBoolean(mapperLogsEnabled);
    }
    return DEFAULT_MAPPER_LOGS_ENABLED;
  }

  @Override
  public boolean process(
      Set<? extends TypeElement> annotations, RoundEnvironment roundEnvironment) {
    ProcessorContext context =
        buildContext(messager, typeUtils, elementUtils, filer, indent, logsEnabled);

    CodeGeneratorFactory generatorFactory = context.getCodeGeneratorFactory();
    processAnnotatedTypes(
        roundEnvironment, Entity.class, ElementKind.CLASS, generatorFactory::newEntity);
    processAnnotatedTypes(
        roundEnvironment, Dao.class, ElementKind.INTERFACE, generatorFactory::newDaoImplementation);
    processAnnotatedTypes(
        roundEnvironment, Mapper.class, ElementKind.INTERFACE, generatorFactory::newMapper);
    return true;
  }

  protected ProcessorContext buildContext(
      DecoratedMessager messager,
      Types typeUtils,
      Elements elementUtils,
      Filer filer,
      String indent,
      boolean logsEnabled) {
    return new DefaultProcessorContext(
        messager, typeUtils, elementUtils, filer, indent, logsEnabled);
  }

  protected void processAnnotatedTypes(
      RoundEnvironment roundEnvironment,
      Class<? extends Annotation> annotationClass,
      ElementKind expectedKind,
      Function<TypeElement, CodeGenerator> generatorFactory) {
    for (Element element : roundEnvironment.getElementsAnnotatedWith(annotationClass)) {
      if (element.getKind() != expectedKind) {
        messager.error(
            element,
            "Only %s elements can be annotated with %s",
            expectedKind,
            annotationClass.getSimpleName());
      } else {
        // Safe cast given that we checked the kind above
        TypeElement typeElement = (TypeElement) element;
        try {
          generatorFactory.apply(typeElement).generate();
        } catch (Exception e) {
          messager.error(
              element,
              "Unexpected error while writing generated code: %s",
              Throwables.getStackTraceAsString(e));
        }
      }
    }
  }

  @Override
  public Set<String> getSupportedAnnotationTypes() {
    return ImmutableSet.of(Entity.class.getName(), Mapper.class.getName(), Dao.class.getName());
  }

  @Override
  public Set<String> getSupportedOptions() {
    return ImmutableSet.of(
        INDENT_AMOUNT_OPTION, INDENT_WITH_TABS_OPTION, MAPPER_LOGS_ENABLED_OPTION);
  }

  @Override
  public SourceVersion getSupportedSourceVersion() {
    return SourceVersion.latest();
  }

  private String computeIndent(Map<String, String> options) {
    boolean tabs = options.containsKey(INDENT_WITH_TABS_OPTION);
    String amountSpec = options.get(INDENT_AMOUNT_OPTION);
    if (amountSpec != null) {
      try {
        int amount = Integer.parseInt(amountSpec);
        return Strings.repeat(tabs ? "\t" : " ", amount);
      } catch (NumberFormatException e) {
        messager.warn(
            "Could not parse %s: expected a number, got '%s'. Defaulting to %s.",
            INDENT_AMOUNT_OPTION, amountSpec, tabs ? "1 tab" : "2 spaces");
      }
    }
    return tabs ? "\t" : "  ";
  }
}
