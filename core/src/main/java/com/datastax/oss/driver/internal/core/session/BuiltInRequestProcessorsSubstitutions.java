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
package com.datastax.oss.driver.internal.core.session;

import static com.datastax.oss.driver.internal.core.util.Dependency.REACTIVE_STREAMS;
import static com.datastax.oss.driver.internal.core.util.Dependency.TINKERPOP;

import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.internal.core.util.GraalDependencyChecker;
import com.oracle.svm.core.annotate.Substitute;
import com.oracle.svm.core.annotate.TargetClass;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;

@SuppressWarnings("unused")
public class BuiltInRequestProcessorsSubstitutions {

  @TargetClass(value = BuiltInRequestProcessors.class, onlyWith = GraphMissingReactiveMissing.class)
  public static final class BuiltInRequestProcessorsGraphMissingReactiveMissing {

    @Substitute
    public static List<RequestProcessor<?, ?>> createDefaultProcessors(
        DefaultDriverContext context) {
      List<RequestProcessor<?, ?>> processors = new ArrayList<>();
      BuiltInRequestProcessors.addBasicProcessors(processors, context);
      return processors;
    }
  }

  @TargetClass(value = BuiltInRequestProcessors.class, onlyWith = GraphMissingReactivePresent.class)
  public static final class BuiltInRequestProcessorsGraphMissingReactivePresent {

    @Substitute
    public static List<RequestProcessor<?, ?>> createDefaultProcessors(
        DefaultDriverContext context) {
      List<RequestProcessor<?, ?>> processors = new ArrayList<>();
      BuiltInRequestProcessors.addBasicProcessors(processors, context);
      BuiltInRequestProcessors.addReactiveProcessors(processors);
      return processors;
    }
  }

  @TargetClass(value = BuiltInRequestProcessors.class, onlyWith = GraphPresentReactiveMissing.class)
  public static final class BuiltInRequestProcessorsGraphPresentReactiveMissing {

    @Substitute
    public static List<RequestProcessor<?, ?>> createDefaultProcessors(
        DefaultDriverContext context) {
      List<RequestProcessor<?, ?>> processors = new ArrayList<>();
      BuiltInRequestProcessors.addBasicProcessors(processors, context);
      BuiltInRequestProcessors.addGraphProcessors(context, processors);
      return processors;
    }
  }

  public static class GraphMissingReactiveMissing implements BooleanSupplier {
    @Override
    public boolean getAsBoolean() {
      return !GraalDependencyChecker.isPresent(TINKERPOP)
          && !GraalDependencyChecker.isPresent(REACTIVE_STREAMS);
    }
  }

  public static class GraphMissingReactivePresent implements BooleanSupplier {
    @Override
    public boolean getAsBoolean() {
      return !GraalDependencyChecker.isPresent(TINKERPOP)
          && GraalDependencyChecker.isPresent(REACTIVE_STREAMS);
    }
  }

  public static class GraphPresentReactiveMissing implements BooleanSupplier {
    @Override
    public boolean getAsBoolean() {
      return GraalDependencyChecker.isPresent(TINKERPOP)
          && !GraalDependencyChecker.isPresent(REACTIVE_STREAMS);
    }
  }
}
