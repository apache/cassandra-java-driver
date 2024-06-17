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
package com.datastax.oss.driver.internal.mapper.processor.util;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.mapper.annotations.HierarchyScanStrategy;
import com.datastax.oss.driver.internal.mapper.processor.ProcessorContext;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import com.datastax.oss.driver.shaded.guava.common.collect.Maps;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.AnnotationValue;
import javax.lang.model.element.Element;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Name;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;
import org.junit.Test;
import org.mockito.Mockito;

public class HierarchyScannerTest {

  private final ProcessorContext context;

  public HierarchyScannerTest() {
    this.context = Mockito.mock(ProcessorContext.class);
    Types types = Mockito.mock(Types.class);
    Classes classUtils = Mockito.mock(Classes.class);

    // used for resolving TypeMirror for default HierarchyScanStrategy highestAncestor
    // (Object.class), in this case just return a mocked TypeElement.
    Elements elements = Mockito.mock(Elements.class);
    Mockito.when(elements.getTypeElement(Mockito.anyString()))
        .thenReturn(Mockito.mock(TypeElement.class));

    Mockito.when(context.getTypeUtils()).thenReturn(types);
    Mockito.when(context.getClassUtils()).thenReturn(classUtils);
    Mockito.when(context.getElementUtils()).thenReturn(elements);

    Mockito.when(classUtils.isSame(Mockito.any(Element.class), Mockito.any(Class.class)))
        .thenReturn(false);
    Mockito.when(types.isSameType(Mockito.any(TypeMirror.class), Mockito.any(TypeMirror.class)))
        .thenAnswer(invocation -> invocation.getArgument(0) == invocation.getArgument(1));
  }

  @Test
  public void should_build_proper_hierarchy_with_default_strategy() {
    /*
     * given the following hierarchy
     *
     *                  a
     *                 /
     *                z
     *               /
     *         a    y
     *        /    /
     *        b   x w
     *         \ / /
     *          c
     */
    MockInterface a = i("a");
    MockInterface z = i("z", a);
    MockInterface y = i("y", z);
    MockInterface x = i("x", y);
    MockInterface w = i("w");
    MockClass b = c("b", null, a);
    MockClass c = c("c", b, x, w);

    // when no HierarchyScanStrategy is defined, then the default behavior should be used
    // which is to scan the entire tree and return it in the correct (bottom up) order.
    assertThat(HierarchyScanner.resolveTypeHierarchy(c.classElement, context).toString())
        .isEqualTo("[c, b, x, w, a, y, z]");
  }

  @Test
  public void should_not_scan_hierarchy_if_scanAncestors_is_false() {
    /*
     * given the following hierarchy
     *
     *                  a
     *                 /
     *                z
     *               /
     *         a   @y
     *        /    /
     *        b   x w
     *         \ / /
     *          c
     */
    // with a HierarchyScanStrategy annotation indicates to not scan ancestors
    // then only traverse the base element.
    // while odd, the strategy is defined on a parent interface, but for practical reasons
    // this seems reasonable.  If the intent is not to allow annotations beyond y, it
    // could be specified that way.
    HierarchyScanStrategy strategy = Mockito.mock(HierarchyScanStrategy.class);
    Mockito.when(strategy.scanAncestors()).thenReturn(false);
    MockInterface a = i("a");
    MockInterface z = i("z", a);
    MockInterface y = i("y", strategy, null, z);
    MockInterface x = i("x", y);
    MockInterface w = i("w");
    MockClass b = c("b", null, a);
    MockClass c = c("c", b, x, w);

    // when no HierarchyScanStrategy is defined, then the default behavior should be used
    // which is to scan the entire tree and return it in the correct (bottom up) order.
    assertThat(HierarchyScanner.resolveTypeHierarchy(c.classElement, context).toString())
        .isEqualTo("[c]");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void should_build_property_hierarchy_with_strategy_defined_on_class() {
    /*
     * given the following hierarchy
     *
     *                  a
     *                 /
     *        a r     z
     *       / /     /
     *      b       y
     *       \     /
     *       @d   x w
     *         \ / /
     *          c
     */
    // with a HierarchyScanStrategy annotation on "d" that dictates that "b" is the highest class,
    // but to not include it.
    HierarchyScanStrategy bHighest = Mockito.mock(HierarchyScanStrategy.class);
    // return this class' name just so we have something to check against.
    Mockito.when(bHighest.highestAncestor()).thenReturn((Class) this.getClass());
    Mockito.when(bHighest.includeHighestAncestor()).thenReturn(false);
    Mockito.when(bHighest.scanAncestors()).thenReturn(true);
    MockInterface a = i("a");
    MockInterface r = i("r");
    MockInterface z = i("z", a);
    MockInterface y = i("y", z);
    MockInterface x = i("x", y);
    MockInterface w = i("w");

    MockClass b = c("b", null, a, r);
    // when checking to see if we're at the 'highest' class (this.getClass()) return true
    // for b.
    Mockito.when(context.getClassUtils().isSame(b.classElement, this.getClass())).thenReturn(true);
    MockClass d = c("d", bHighest, b, b);
    MockClass c = c("c", d, x, w);

    // b and its interfaces should be skipped, however a will also be encountered
    // as it is still in the hierarchy of c -> x -> y -> z -> a
    assertThat(HierarchyScanner.resolveTypeHierarchy(c.classElement, context).toString())
        .isEqualTo("[c, d, x, w, y, z, a]");
  }

  private MockClass c(String name, MockClass parent, MockInterface... interfaces) {
    return new MockClass(name, null, null, parent, interfaces);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void should_build_property_hierarchy_with_strategy_defined_on_interface_include_highest() {
    /*
     * given the following hierarchy
     *
     *                  a
     *                 /
     *        a r     z
     *       / /     /
     *      b       y
     *       \     /
     *        d  @x w
     *         \ / /
     *          c
     */
    // with a HierarchyScanStrategy annotation on "x" that dictates that "y" is the highest class,
    // but to include it.
    HierarchyScanStrategy yHighest = Mockito.mock(HierarchyScanStrategy.class);
    Mockito.when(yHighest.includeHighestAncestor()).thenReturn(true);
    Mockito.when(yHighest.scanAncestors()).thenReturn(true);
    MockInterface a = i("a");
    MockInterface r = i("r");
    MockInterface z = i("z", a);
    MockInterface y = i("y", z);
    MockInterface x = i("x", yHighest, y, y);
    // when checking to see if we're at the 'highest' class (this.getClass()) return true
    // for y.
    Mockito.when(context.getClassUtils().isSame(y.classElement, this.getClass())).thenReturn(true);
    MockInterface w = i("w");

    MockClass b = c("b", null, a, r);
    MockClass d = c("d", b);
    MockClass c = c("c", d, x, w);

    // y's parent interface z should be skipped, however a will also be encountered
    // as it is still in the hierarchy of c -> d -> b -> a
    assertThat(HierarchyScanner.resolveTypeHierarchy(c.classElement, context).toString())
        .isEqualTo("[c, d, x, w, b, y, a, r]");
  }

  private MockClass c(
      String name,
      HierarchyScanStrategy strategy,
      MockElement highestAncestor,
      MockClass parent,
      MockInterface... interfaces) {
    return new MockClass(name, strategy, highestAncestor, parent, interfaces);
  }

  private MockInterface i(String name, MockInterface... interfaces) {
    return new MockInterface(name, null, null, interfaces);
  }

  private MockInterface i(
      String name,
      HierarchyScanStrategy strategy,
      MockElement highestAncestor,
      MockInterface... interfaces) {
    return new MockInterface(name, strategy, highestAncestor, interfaces);
  }

  private TypeMirror root() {
    TypeMirror noneMirror = Mockito.mock(TypeMirror.class);
    Mockito.when(noneMirror.getKind()).thenReturn(TypeKind.NONE);

    return noneMirror;
  }

  class MockElement {
    final String name;
    final TypeMirror mirror;
    final TypeElement classElement;
    final List<MockElement> interfaces;
    final MockClass parent;
    final Name qfName;

    @SuppressWarnings("unchecked")
    MockElement(
        String name,
        HierarchyScanStrategy strategy,
        MockElement highestAncestor,
        MockClass parent,
        MockInterface... interfaces) {
      this.name = name;
      this.parent = parent;
      this.interfaces = Arrays.asList(interfaces);

      this.classElement = Mockito.mock(TypeElement.class);
      Mockito.when(classElement.toString()).thenReturn(name);

      TypeMirror parentMirror = parent != null ? parent.mirror : root();
      Mockito.when(classElement.getSuperclass()).thenReturn(parentMirror);

      List interfaceList = Lists.newArrayList();
      for (MockElement i : interfaces) {
        interfaceList.add(i.mirror);
      }
      Mockito.when(classElement.getInterfaces()).thenReturn(interfaceList);

      this.mirror = Mockito.mock(TypeMirror.class);
      Mockito.when(mirror.getKind()).thenReturn(TypeKind.DECLARED);
      Mockito.when(mirror.toString()).thenReturn(name);
      Mockito.when(classElement.asType()).thenReturn(mirror);
      Mockito.when(context.getTypeUtils().asElement(mirror)).thenReturn(classElement);

      this.qfName = Mockito.mock(Name.class);
      Mockito.when(qfName.toString()).thenReturn(name);
      Mockito.when(classElement.getQualifiedName()).thenReturn(qfName);

      // mock annotation mirror logic.
      if (strategy != null) {
        Mockito.when(classElement.getAnnotation(HierarchyScanStrategy.class)).thenReturn(strategy);

        AnnotationMirror annotationMirror = Mockito.mock(AnnotationMirror.class);
        DeclaredType annotationType = Mockito.mock(DeclaredType.class);
        Mockito.when(annotationMirror.getAnnotationType()).thenReturn(annotationType);
        List annotationMirrors = Lists.newArrayList(annotationMirror);
        Mockito.when(classElement.getAnnotationMirrors()).thenReturn(annotationMirrors);
        Mockito.when(context.getClassUtils().isSame(annotationType, HierarchyScanStrategy.class))
            .thenReturn(true);

        Map annotationElementValues = Maps.newHashMap();
        annotationElementValues.put(
            mockAnnotationElement("scanAncestors"), mockAnnotationValue(strategy.scanAncestors()));
        annotationElementValues.put(
            mockAnnotationElement("includeHighestAncestor"),
            mockAnnotationValue(strategy.includeHighestAncestor()));
        if (highestAncestor != null) {
          annotationElementValues.put(
              mockAnnotationElement("highestAncestor"),
              mockAnnotationValue(highestAncestor.mirror));
        }
        Mockito.when(annotationMirror.getElementValues()).thenReturn(annotationElementValues);
      }
    }

    private ExecutableElement mockAnnotationElement(String key) {
      ExecutableElement element = Mockito.mock(ExecutableElement.class);
      Name name = Mockito.mock(Name.class);
      Mockito.when(name.contentEquals(key)).thenReturn(true);
      Mockito.when(element.getSimpleName()).thenReturn(name);
      return element;
    }

    private AnnotationValue mockAnnotationValue(Object value) {
      AnnotationValue aValue = Mockito.mock(AnnotationValue.class);
      Mockito.when(aValue.getValue()).thenReturn(value);
      return aValue;
    }
  }

  class MockClass extends MockElement {

    MockClass(
        String name,
        HierarchyScanStrategy strategy,
        MockElement highestAncestor,
        MockClass parent,
        MockInterface... interfaces) {
      super(name, strategy, highestAncestor, parent, interfaces);
    }
  }

  class MockInterface extends MockElement {

    MockInterface(
        String name,
        HierarchyScanStrategy strategy,
        MockElement highestAncestor,
        MockInterface... interfaces) {
      super(name, strategy, highestAncestor, null, interfaces);
    }
  }
}
