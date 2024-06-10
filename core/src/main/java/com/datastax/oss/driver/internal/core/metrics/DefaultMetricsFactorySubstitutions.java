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
package com.datastax.oss.driver.internal.core.metrics;

import static com.datastax.oss.driver.internal.core.util.Dependency.DROPWIZARD;

import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.internal.core.util.GraalDependencyChecker;
import com.oracle.svm.core.annotate.Alias;
import com.oracle.svm.core.annotate.Delete;
import com.oracle.svm.core.annotate.Substitute;
import com.oracle.svm.core.annotate.TargetClass;
import com.oracle.svm.core.annotate.TargetElement;
import java.util.function.BooleanSupplier;

@SuppressWarnings("unused")
public class DefaultMetricsFactorySubstitutions {

  @TargetClass(value = DefaultMetricsFactory.class, onlyWith = DropwizardMissing.class)
  public static final class DefaultMetricsFactoryDropwizardMissing {

    @Alias
    @TargetElement(name = "delegate")
    @SuppressWarnings({"FieldCanBeLocal", "FieldMayBeFinal"})
    private MetricsFactory delegate;

    @Substitute
    @TargetElement(name = TargetElement.CONSTRUCTOR_NAME)
    public DefaultMetricsFactoryDropwizardMissing(DriverContext context) {
      this.delegate = new NoopMetricsFactory(context);
    }
  }

  @TargetClass(value = DropwizardMetricsFactory.class, onlyWith = DropwizardMissing.class)
  @Delete
  public static final class DeleteDropwizardMetricsFactory {}

  public static class DropwizardMissing implements BooleanSupplier {
    @Override
    public boolean getAsBoolean() {
      return !GraalDependencyChecker.isPresent(DROPWIZARD);
    }
  }
}
