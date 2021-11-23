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
package com.datastax.dse.driver.internal.core.type.codec;

import static com.datastax.oss.driver.internal.core.util.Dependency.ESRI;

import com.datastax.dse.driver.api.core.type.codec.DseTypeCodecs;
import com.datastax.oss.driver.api.core.type.codec.registry.MutableCodecRegistry;
import com.datastax.oss.driver.internal.core.util.GraalDependencyChecker;
import com.oracle.svm.core.annotate.Substitute;
import com.oracle.svm.core.annotate.TargetClass;
import java.util.function.BooleanSupplier;

@SuppressWarnings("unused")
public class DseTypeCodecsRegistrarSubstitutions {

  @TargetClass(value = DseTypeCodecsRegistrar.class, onlyWith = EsriMissing.class)
  public static final class DseTypeCodecsRegistrarEsriMissing {

    @Substitute
    public static void registerDseCodecs(MutableCodecRegistry registry) {
      registry.register(DseTypeCodecs.DATE_RANGE);
    }
  }

  public static class EsriMissing implements BooleanSupplier {
    @Override
    public boolean getAsBoolean() {
      return !GraalDependencyChecker.isPresent(ESRI);
    }
  }
}
