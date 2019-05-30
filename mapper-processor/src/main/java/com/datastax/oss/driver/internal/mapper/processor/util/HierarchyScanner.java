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
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import com.datastax.oss.driver.shaded.guava.common.collect.Sets;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Types;

/** Provides mechanisms for building and traversing a class/interfaces hierarchy. */
public class HierarchyScanner {

  // placeholder class for resolving the default HierarchyScanStrategy
  @HierarchyScanStrategy
  private static final class ClassForDefaultScanStrategy {}

  /**
   * Resolves the type hierarchy for the given type by first looking for a {@link
   * HierarchyScanStrategy}-annotated class or interface in the given types hierarchy. The hierarchy
   * is traversed until a {@link HierarchyScanStrategy} is encountered in a depth-first manner as
   * follows:
   *
   * <ol>
   *   <li>Initialize <code>i</code> as an empty set
   *   <li>Visit typeElement (<code>e</code>)
   *   <li>Visit <code>e</code>'s parent class (<code>p</code>), if <code>p</code> is null, stop
   *       traversing after evaluating remaining interfaces
   *   <li>Visit <code>e</code>'s interfaces, and record those interfaces' parents for later use (
   *       <code>ni</code>)
   *   <li>Visit <code>i</code>, and append those interface's parents to <code>ni</code> for later
   *       use)
   *   <li>If <code>p != null</code> </code>Set <code>e := p, i := ni</code> and repeat starting at
   *       step 3
   *   <li>Visit <code>ni</code> interfaces and their parents until we've reached root
   * </ol>
   *
   * Once a {@link HierarchyScanStrategy} is identified, the returning hierarchy is built by
   * traversing the hierarchy again using the chosen strategy.
   *
   * @param typeElement The type whose hierarchy will be traversed.
   * @param types Used to resolve parent type elements
   * @return The type hierarchy, ordered from typeElement to the highestAncestor, as dictated by the
   *     resolved {@link HierarchyScanStrategy}
   */
  public static Set<TypeElement> resolveTypeHierarchy(TypeElement typeElement, Types types) {
    HierarchyScanStrategy hierarchyScanStrategy =
        HierarchyScanner.resolveHierarchyScanStrategy(typeElement, types);

    ImmutableSet.Builder<TypeElement> hierarchy = ImmutableSet.builder();
    traverseFullHierarchy(hierarchyScanStrategy, typeElement, types, hierarchy::add);
    return hierarchy.build();
  }

  private static HierarchyScanStrategy resolveHierarchyScanStrategy(
      TypeElement classElement, Types types) {
    // Use the default HierarchyScanStrategy to find the configured HierarchyScanStrategy.
    // This is done because the default strategy is the most permissive.
    HierarchyScanStrategy strategy =
        ClassForDefaultScanStrategy.class.getAnnotation(HierarchyScanStrategy.class);

    // traverse hierarchy until a strategy is found.
    final AtomicReference<HierarchyScanStrategy> ref = new AtomicReference<>(strategy);
    traverseHierarchy(
        strategy,
        classElement,
        types,
        (TypeElement t) -> {
          HierarchyScanStrategy discoveredStrategy = t.getAnnotation(HierarchyScanStrategy.class);
          // if we find a strategy, set it and stop traversing.
          if (discoveredStrategy != null) {
            ref.compareAndSet(strategy, discoveredStrategy);
            return false;
          }
          return true;
        });

    return ref.get();
  }

  private static void traverseFullHierarchy(
      HierarchyScanStrategy hierarchyScanStrategy,
      TypeElement classElement,
      Types types,
      Consumer<TypeElement> typeConsumer) {
    traverseHierarchy(
        hierarchyScanStrategy,
        classElement,
        types,
        (TypeElement t) -> {
          typeConsumer.accept(t);
          return true;
        });
  }

  private static void traverseHierarchy(
      HierarchyScanStrategy hierarchyScanStrategy,
      TypeElement classElement,
      Types types,
      Function<TypeElement, Boolean> typeConsumer) {

    if (!typeConsumer.apply(classElement)) {
      return;
    }

    Set<TypeMirror> interfacesToScan = Collections.emptySet();
    // TODO: Probably should also consider interfaces for highest class
    boolean atHighestClass =
        classElement
            .getQualifiedName()
            .toString()
            .equals(hierarchyScanStrategy.highestAncestor().getName());
    while (!atHighestClass) {
      // add super class
      TypeMirror superClass = classElement.getSuperclass();
      TypeElement superClassElement = null;
      if (superClass.getKind() == TypeKind.DECLARED) {
        superClassElement = (TypeElement) types.asElement(superClass);
        atHighestClass =
            superClassElement
                .getQualifiedName()
                .toString()
                .equals(hierarchyScanStrategy.highestAncestor().getName());
        if (!atHighestClass || hierarchyScanStrategy.includeHighestAncestor()) {
          if (!typeConsumer.apply(superClassElement)) {
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
      scanInterfaces(classElement.getInterfaces(), newInterfacesToScan, types, typeConsumer);
      // and then add interfaces to scan from previous class interfaces parents.
      scanInterfaces(interfacesToScan, newInterfacesToScan, types, typeConsumer);

      // navigate up to the superclass and to the class' encountered interfaces' parents.
      classElement = superClassElement;
      interfacesToScan = newInterfacesToScan;
    }

    // if we've exhausted the class hierarchy, we may still need to consume the interface hierarchy.
    while (!interfacesToScan.isEmpty()) {
      Set<TypeMirror> newInterfacesToScan = Sets.newLinkedHashSet();
      scanInterfaces(interfacesToScan, newInterfacesToScan, types, typeConsumer);
      interfacesToScan = newInterfacesToScan;
    }
  }

  private static void scanInterfaces(
      Collection<? extends TypeMirror> interfacesToScan,
      Set<TypeMirror> newInterfacesToScan,
      Types types,
      Function<TypeElement, Boolean> typeConsumer) {
    for (TypeMirror interfaceType : interfacesToScan) {
      if (interfaceType.getKind() == TypeKind.DECLARED) {
        TypeElement interfaceElement = (TypeElement) types.asElement(interfaceType);
        if (!typeConsumer.apply(interfaceElement)) {
          return;
        }
        newInterfacesToScan.addAll(interfaceElement.getInterfaces());
      }
    }
  }
}
