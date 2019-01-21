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

import javax.annotation.processing.Messager;
import javax.lang.model.element.Element;
import javax.tools.Diagnostic;

/** Wraps {@link Messager} to provide convenience methods. */
public class DecoratedMessager {

  private final Messager messager;

  public DecoratedMessager(Messager messager) {
    this.messager = messager;
  }

  public void warn(Element element, String template, Object... arguments) {
    messager.printMessage(Diagnostic.Kind.WARNING, String.format(template, arguments), element);
  }

  public void warn(String template, Object... arguments) {
    messager.printMessage(Diagnostic.Kind.WARNING, String.format(template, arguments));
  }

  public void error(Element element, String template, Object... arguments) {
    messager.printMessage(Diagnostic.Kind.ERROR, String.format(template, arguments), element);
  }
}
