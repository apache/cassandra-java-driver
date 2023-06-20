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
import com.datastax.oss.driver.shaded.guava.common.base.Throwables;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import java.lang.annotation.Annotation;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Filer;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.TypeElement;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;

public class MapperProcessor extends AbstractProcessor {
  private static final boolean DEFAULT_MAPPER_LOGS_ENABLED = true;
  private static final boolean DEFAULT_CUSTOM_RESULTS_ENABLED = true;

  private static final String INDENT_AMOUNT_OPTION = "com.datastax.oss.driver.mapper.indent";
  private static final String INDENT_WITH_TABS_OPTION =
      "com.datastax.oss.driver.mapper.indentWithTabs";
  private static final String MAPPER_LOGS_ENABLED_OPTION =
      "com.datastax.oss.driver.mapper.logs.enabled";
  private static final String CUSTOM_RESULTS_ENABLED_OPTION =
      "com.datastax.oss.driver.mapper.customResults.enabled";

  private DecoratedMessager messager;
  private Types typeUtils;
  private Elements elementUtils;
  private Filer filer;
  private String indent;
  private boolean logsEnabled;
  private boolean customResultsEnabled;

  @Override
  public synchronized void init(ProcessingEnvironment processingEnvironment) {
    super.init(processingEnvironment);
    messager = new DecoratedMessager(processingEnvironment.getMessager());
    typeUtils = processingEnvironment.getTypeUtils();
    elementUtils = processingEnvironment.getElementUtils();
    filer = processingEnvironment.getFiler();
    Map<String, String> options = processingEnvironment.getOptions();
    indent = computeIndent(options);
    logsEnabled =
        getBooleanOption(options, MAPPER_LOGS_ENABLED_OPTION, DEFAULT_MAPPER_LOGS_ENABLED);
    customResultsEnabled =
        getBooleanOption(options, CUSTOM_RESULTS_ENABLED_OPTION, DEFAULT_CUSTOM_RESULTS_ENABLED);
  }

  private boolean getBooleanOption(
      Map<String, String> options, String optionName, boolean defaultValue) {
    String value = options.get(optionName);
    return (value == null) ? defaultValue : Boolean.parseBoolean(value);
  }

  @Override
  public boolean process(
      Set<? extends TypeElement> annotations, RoundEnvironment roundEnvironment) {
    ProcessorContext context =
        buildContext(
            messager, typeUtils, elementUtils, filer, indent, logsEnabled, customResultsEnabled);

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
      boolean logsEnabled,
      boolean customResultsEnabled) {
    return new DefaultProcessorContext(
        messager, typeUtils, elementUtils, filer, indent, logsEnabled, customResultsEnabled);
  }

  protected void processAnnotatedTypes(
      RoundEnvironment roundEnvironment,
      Class<? extends Annotation> annotationClass,
      ElementKind expectedKind,
      Function<TypeElement, CodeGenerator> generatorFactory) {
    for (Element element : roundEnvironment.getElementsAnnotatedWith(annotationClass)) {
      ElementKind actualKind = element.getKind();
      boolean isExpectedElement =
          actualKind == expectedKind
              // Hack to support Java 14 records without having to compile against JDK 14 (also
              // possible because we only expect CLASS for entities).
              || (expectedKind == ElementKind.CLASS && actualKind.name().equals("RECORD"));
      if (isExpectedElement) {
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
      } else {
        messager.error(
            element,
            "Only %s elements can be annotated with %s (got %s)",
            expectedKind,
            annotationClass.getSimpleName(),
            actualKind);
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
        INDENT_AMOUNT_OPTION,
        INDENT_WITH_TABS_OPTION,
        MAPPER_LOGS_ENABLED_OPTION,
        CUSTOM_RESULTS_ENABLED_OPTION);
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
