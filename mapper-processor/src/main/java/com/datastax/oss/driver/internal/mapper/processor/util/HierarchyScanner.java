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
package com.datastax.oss.driver.internal.mapper.processor.util;

import com.datastax.oss.driver.api.mapper.annotations.HierarchyScanStrategy;
import com.datastax.oss.driver.internal.mapper.processor.ProcessorContext;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import com.datastax.oss.driver.shaded.guava.common.collect.Sets;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.AnnotationValue;
import javax.lang.model.element.Element;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;

/** Provides mechanisms for building and traversing a class/interfaces hierarchy. */
public class HierarchyScanner {

  // placeholder class for resolving the default HierarchyScanStrategy
  @HierarchyScanStrategy
  private static final class ClassForDefaultScanStrategy {}

  private static final HierarchyScanStrategy defaultStrategy =
      ClassForDefaultScanStrategy.class.getAnnotation(HierarchyScanStrategy.class);

  /**
   * Resolves the type hierarchy for the given type by first looking for a {@link
   * HierarchyScanStrategy}-annotated class or interface in the given types hierarchy. The hierarchy
   * is traversed until a {@link HierarchyScanStrategy} is encountered in a depth-first manner as
   * follows:
   *
   * <ol>
   *   <li>Initialize <code>interfacesToScan</code> as an empty set
   *   <li>Visit <code>typeElement</code>
   *   <li>Visit <code>typeElement</code>'s parent class (<code>superClassElement</code>), if <code>
   *       superClassElement</code> is null, stop traversing after evaluating remaining interfaces
   *   <li>Visit <code>typeElements</code>'s interfaces, and record those interfaces' parents for
   *       later use (<code>newInterfacesToScan</code>)
   *   <li>Visit <code>interfacesToScan</code>, and append those interface's parents to <code>
   *       newInterfacesToScan</code> for later use)
   *   <li>If <code>superClassElement != null</code> Set <code>typeElement :=
   *       superClassElement, interfacesToScan := newInterfacesToScan</code> and repeat starting at
   *       step 3
   *   <li>Visit <code>newInterfacesToScan</code> interfaces and their parents until we've reached
   *       root
   * </ol>
   *
   * Once a {@link HierarchyScanStrategy} is identified, the returning hierarchy is built by
   * traversing the hierarchy again using the chosen strategy.
   *
   * @param typeElement The type whose hierarchy will be traversed.
   * @param context provides utilities for working with types.
   * @return The type hierarchy, ordered from typeElement to the highestAncestor, as dictated by the
   *     resolved {@link HierarchyScanStrategy}
   */
  public static Set<TypeMirror> resolveTypeHierarchy(
      TypeElement typeElement, ProcessorContext context) {
    HierarchyScanStrategyOptions hierarchyScanStrategy =
        HierarchyScanner.resolveHierarchyScanStrategy(typeElement, context);

    ImmutableSet.Builder<TypeMirror> hierarchy = ImmutableSet.builder();
    traverseFullHierarchy(hierarchyScanStrategy, typeElement, context, hierarchy::add);
    return hierarchy.build();
  }

  private static HierarchyScanStrategyOptions resolveHierarchyScanStrategy(
      TypeElement classElement, ProcessorContext context) {
    // Use the default HierarchyScanStrategy to find the configured HierarchyScanStrategy.
    // This is done because the default strategy is the most permissive.

    // traverse hierarchy until a strategy is found.
    final HierarchyScanStrategyOptions defaultOptions =
        new HierarchyScanStrategyOptions(context.getElementUtils());
    final AtomicReference<HierarchyScanStrategyOptions> ref = new AtomicReference<>(defaultOptions);
    traverseHierarchy(
        defaultOptions,
        classElement,
        context,
        (TypeMirror mirror) -> {
          // if we find a strategy, set it and stop traversing.
          Element element = context.getTypeUtils().asElement(mirror);
          for (AnnotationMirror candidate : element.getAnnotationMirrors()) {
            if (context
                .getClassUtils()
                .isSame(candidate.getAnnotationType(), HierarchyScanStrategy.class)) {
              ref.compareAndSet(defaultOptions, new HierarchyScanStrategyOptions(candidate));
              return false;
            }
          }
          return true;
        });

    return ref.get();
  }

  private static class HierarchyScanStrategyOptions {
    boolean scanAncestors = defaultStrategy.scanAncestors();
    TypeMirror highestAncestor = null;
    boolean includeHighestAncestor = defaultStrategy.includeHighestAncestor();

    HierarchyScanStrategyOptions(Elements elements) {
      // its assumed the defaultStrategy's highestAncestor is Object.class, or something
      // that is already resolvable.
      this.highestAncestor =
          elements.getTypeElement(defaultStrategy.highestAncestor().getName()).asType();
    }

    HierarchyScanStrategyOptions(AnnotationMirror annotationMirror) {
      for (Map.Entry<? extends ExecutableElement, ? extends AnnotationValue> entry :
          annotationMirror.getElementValues().entrySet()) {
        if (entry.getKey().getSimpleName().contentEquals("scanAncestors")) {
          this.scanAncestors = (Boolean) entry.getValue().getValue();
        } else if (entry.getKey().getSimpleName().contentEquals("highestAncestor")) {
          this.highestAncestor = (TypeMirror) entry.getValue().getValue();
        } else if (entry.getKey().getSimpleName().contentEquals("includeHighestAncestor")) {
          this.includeHighestAncestor = (Boolean) entry.getValue().getValue();
        }
      }
    }

    boolean atHighest(TypeMirror mirror, ProcessorContext context) {
      return highestAncestor != null && context.getTypeUtils().isSameType(mirror, highestAncestor);
    }
  }

  private static void traverseFullHierarchy(
      HierarchyScanStrategyOptions hierarchyScanStrategy,
      TypeElement classElement,
      ProcessorContext context,
      Consumer<TypeMirror> typeConsumer) {
    traverseHierarchy(
        hierarchyScanStrategy,
        classElement,
        context,
        (TypeMirror t) -> {
          typeConsumer.accept(t);
          return true;
        });
  }

  private static void traverseHierarchy(
      HierarchyScanStrategyOptions hierarchyScanStrategy,
      TypeElement classElement,
      ProcessorContext context,
      Function<TypeMirror, Boolean> typeConsumer) {

    if (!typeConsumer.apply(classElement.asType()) || !hierarchyScanStrategy.scanAncestors) {
      return;
    }

    Set<TypeMirror> interfacesToScan = Collections.emptySet();
    boolean atHighestClass = hierarchyScanStrategy.atHighest(classElement.asType(), context);
    while (!atHighestClass) {
      // add super class
      TypeMirror superClass = classElement.getSuperclass();
      TypeElement superClassElement = null;
      if (superClass.getKind() == TypeKind.DECLARED) {
        superClassElement = (TypeElement) context.getTypeUtils().asElement(superClass);
        atHighestClass = hierarchyScanStrategy.atHighest(superClass, context);
        if (!atHighestClass || hierarchyScanStrategy.includeHighestAncestor) {
          if (!typeConsumer.apply(superClass)) {
            return;
          }
        }
      } else {
        // at highest level, no need to proceed.
        atHighestClass = true;
      }

      // as we encounter interfaces, also keep track of their parents.
      Set<TypeMirror> newInterfacesToScan = Sets.newLinkedHashSet();

      // scan parent classes interfaces and add them.
      scanInterfaces(
          hierarchyScanStrategy,
          classElement.getInterfaces(),
          newInterfacesToScan,
          context,
          typeConsumer);
      // and then add interfaces to scan from previous class interfaces parents.
      scanInterfaces(
          hierarchyScanStrategy, interfacesToScan, newInterfacesToScan, context, typeConsumer);

      // navigate up to the superclass and to the class' encountered interfaces' parents.
      classElement = superClassElement;
      interfacesToScan = newInterfacesToScan;
    }

    // if we've exhausted the class hierarchy, we may still need to consume the interface hierarchy.
    while (!interfacesToScan.isEmpty()) {
      Set<TypeMirror> newInterfacesToScan = Sets.newLinkedHashSet();
      scanInterfaces(
          hierarchyScanStrategy, interfacesToScan, newInterfacesToScan, context, typeConsumer);
      interfacesToScan = newInterfacesToScan;
    }
  }

  private static void scanInterfaces(
      HierarchyScanStrategyOptions hierarchyScanStrategy,
      Collection<? extends TypeMirror> interfacesToScan,
      Set<TypeMirror> newInterfacesToScan,
      ProcessorContext context,
      Function<TypeMirror, Boolean> typeConsumer) {
    for (TypeMirror interfaceType : interfacesToScan) {
      if (interfaceType.getKind() == TypeKind.DECLARED) {
        TypeElement interfaceElement =
            (TypeElement) context.getTypeUtils().asElement(interfaceType);
        // skip if at highest ancestor.
        boolean atHighest = hierarchyScanStrategy.atHighest(interfaceType, context);
        if (!atHighest || hierarchyScanStrategy.includeHighestAncestor) {
          if (!typeConsumer.apply(interfaceType)) {
            return;
          }
        }
        if (!atHighest) {
          newInterfacesToScan.addAll(interfaceElement.getInterfaces());
        }
      }
    }
  }
}
