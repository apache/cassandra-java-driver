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
package com.datastax.oss.driver.internal.mapper.processor;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import javax.annotation.processing.Messager;
import javax.lang.model.element.Element;
import javax.tools.Diagnostic;

/** Wraps {@link Messager} to provide convenience methods. */
public class DecoratedMessager {

  private final Messager messager;
  private final Set<MessageId> emittedMessages = new HashSet<>();

  public DecoratedMessager(Messager messager) {
    this.messager = messager;
  }

  /** Emits a global warning that doesn't target a particular element. */
  public void warn(String template, Object... arguments) {
    messager.printMessage(Diagnostic.Kind.WARNING, String.format(template, arguments));
  }

  /** Emits a warning for a specific element. */
  public void warn(Element element, String template, Object... arguments) {
    message(Diagnostic.Kind.WARNING, element, template, arguments);
  }

  /** Emits an error for a specific element. */
  public void error(Element element, String template, Object... arguments) {
    message(Diagnostic.Kind.ERROR, element, template, arguments);
  }

  private void message(
      Diagnostic.Kind level, Element element, String template, Object[] arguments) {
    if (emittedMessages.add(new MessageId(level, element, template, arguments))) {
      messager.printMessage(
          level, formatLocation(element) + String.format(template, arguments), element);
    }
  }

  private static String formatLocation(Element element) {
    switch (element.getKind()) {
      case CLASS:
      case INTERFACE:
        return String.format("[%s] ", element.getSimpleName());
      case FIELD:
      case METHOD:
      case CONSTRUCTOR:
        return String.format(
            "[%s.%s] ", element.getEnclosingElement().getSimpleName(), element.getSimpleName());
      case PARAMETER:
        Element method = element.getEnclosingElement();
        Element type = method.getEnclosingElement();
        return String.format(
            "[%s.%s, parameter %s] ",
            type.getSimpleName(), method.getSimpleName(), element.getSimpleName());
      default:
        // We don't emit messages for other types of elements in the mapper processor. Handle
        // gracefully nevertheless:
        return String.format("[%s] ", element);
    }
  }

  private static class MessageId {

    private final Diagnostic.Kind level;
    private final Element element;
    private final String template;
    private final Object[] arguments;

    private MessageId(Diagnostic.Kind level, Element element, String template, Object[] arguments) {
      this.level = level;
      this.element = element;
      this.template = template;
      this.arguments = arguments;
    }

    @Override
    public boolean equals(Object other) {
      if (other == this) {
        return true;
      } else if (other instanceof MessageId) {
        MessageId that = (MessageId) other;
        return this.level == that.level
            && Objects.equals(this.element, that.element)
            && Objects.equals(this.template, that.template)
            && Arrays.deepEquals(this.arguments, that.arguments);
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Objects.hash(level, element, template, Arrays.hashCode(arguments));
    }
  }
}
