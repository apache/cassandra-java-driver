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

import com.squareup.javapoet.JavaFile;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.Writer;
import javax.annotation.processing.Filer;
import javax.tools.JavaFileObject;

/** Thin wrapper around {@link Filer} for content generated with JavaPoet. */
public class JavaPoetFiler {

  private final Filer filer;
  private final String indent;

  public JavaPoetFiler(Filer filer, String indent) {
    this.filer = filer;
    this.indent = indent;
  }

  public void write(String fileName, JavaFile.Builder contents) {
    try {
      JavaFileObject file = filer.createSourceFile(fileName);
      try (Writer writer = file.openWriter()) {
        contents.indent(indent).build().writeTo(writer);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
