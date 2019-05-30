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

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.mapper.annotations.HierarchyScanStrategy;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import java.util.Arrays;
import java.util.List;
import javax.lang.model.element.Name;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Types;
import org.junit.Test;
import org.mockito.Mockito;

public class HierarchyScannerTest {

  Types types = Mockito.mock(Types.class);

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
    assertThat(HierarchyScanner.resolveTypeHierarchy(c.classElement, types).toString())
        .isEqualTo("[c, b, x, w, a, y, z]");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void should_build_property_hierarchy_with_strategy_defined() {
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
    // return this class's name, which will also be used for  b's class name.
    Mockito.when(bHighest.highestAncestor()).thenReturn((Class) this.getClass());
    Mockito.when(bHighest.includeHighestAncestor()).thenReturn(false);
    MockInterface a = i("a");
    MockInterface r = i("r");
    MockInterface z = i("z", a);
    MockInterface y = i("y", z);
    MockInterface x = i("x", y);
    MockInterface w = i("w");

    MockClass b = c(this.getClass().getName(), null, a, r);
    MockClass d = c("d", bHighest, b);
    MockClass c = c("c", d, x, w);

    // b and its interfaces should be skipped, however a will also be encountered
    // as it is still in the hierarchy of c -> x -> y -> z -> a
    assertThat(HierarchyScanner.resolveTypeHierarchy(c.classElement, types).toString())
        .isEqualTo("[c, d, x, w, y, z, a]");
  }

  private MockClass c(String name, MockClass parent, MockInterface... interfaces) {
    return new MockClass(name, null, types, parent, interfaces);
  }

  private MockClass c(
      String name, HierarchyScanStrategy strategy, MockClass parent, MockInterface... interfaces) {
    return new MockClass(name, strategy, types, parent, interfaces);
  }

  private MockInterface i(String name, MockInterface... interfaces) {
    return new MockInterface(name, null, types, interfaces);
  }

  private MockInterface i(
      String name, HierarchyScanStrategy strategy, MockInterface... interfaces) {
    return new MockInterface(name, strategy, types, interfaces);
  }

  private TypeMirror root() {
    TypeMirror noneMirror = Mockito.mock(TypeMirror.class);
    Mockito.when(noneMirror.getKind()).thenReturn(TypeKind.NONE);

    return noneMirror;
  }

  // TODO: Refactor these together.
  class MockClass {

    final String name;
    final TypeMirror mirror;
    final TypeElement classElement;
    final List<MockInterface> interfaces;
    final MockClass parent;
    final Name qfName;

    @SuppressWarnings("unchecked")
    MockClass(
        String name,
        HierarchyScanStrategy strategy,
        Types types,
        MockClass parent,
        MockInterface... interfaces) {
      this.name = name;
      this.parent = parent;
      this.interfaces = Arrays.asList(interfaces);

      this.classElement = Mockito.mock(TypeElement.class);
      Mockito.when(classElement.toString()).thenReturn(name);
      Mockito.when(classElement.getAnnotation(HierarchyScanStrategy.class)).thenReturn(strategy);

      TypeMirror parentMirror = parent != null ? parent.mirror : root();
      Mockito.when(classElement.getSuperclass()).thenReturn(parentMirror);

      List interfaceList = Lists.newArrayList();
      for (MockInterface i : interfaces) {
        interfaceList.add(i.mirror);
      }
      Mockito.when(classElement.getInterfaces()).thenReturn(interfaceList);

      this.mirror = Mockito.mock(TypeMirror.class);
      Mockito.when(mirror.getKind()).thenReturn(TypeKind.DECLARED);
      Mockito.when(types.asElement(mirror)).thenReturn(classElement);

      this.qfName = Mockito.mock(Name.class);
      Mockito.when(qfName.toString()).thenReturn(name);
      Mockito.when(classElement.getQualifiedName()).thenReturn(qfName);
    }
  }

  class MockInterface {
    final String name;
    final TypeMirror mirror;
    final TypeElement interfaceElement;
    final List<MockInterface> interfaces;
    final Name qfName;

    @SuppressWarnings("unchecked")
    public MockInterface(
        String name, HierarchyScanStrategy strategy, Types types, MockInterface... interfaces) {
      this.name = name;
      this.interfaces = Arrays.asList(interfaces);

      this.interfaceElement = Mockito.mock(TypeElement.class);
      Mockito.when(interfaceElement.toString()).thenReturn(name);
      Mockito.when(interfaceElement.getAnnotation(HierarchyScanStrategy.class))
          .thenReturn(strategy);

      TypeMirror root = root();
      Mockito.when(interfaceElement.getSuperclass()).thenReturn(root);
      List interfaceList = Lists.newArrayList();
      for (MockInterface i : interfaces) {
        interfaceList.add(i.mirror);
      }
      Mockito.when(interfaceElement.getInterfaces()).thenReturn(interfaceList);

      this.mirror = Mockito.mock(TypeMirror.class);
      Mockito.when(mirror.getKind()).thenReturn(TypeKind.DECLARED);
      Mockito.when(types.asElement(mirror)).thenReturn(interfaceElement);

      this.qfName = Mockito.mock(Name.class);
      Mockito.when(qfName.toString()).thenReturn(name);
      Mockito.when(interfaceElement.getQualifiedName()).thenReturn(qfName);
    }
  }
}
