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
package com.google.common.primitives;

import com.oracle.svm.core.annotate.Alias;
import com.oracle.svm.core.annotate.RecomputeFieldValue;
import com.oracle.svm.core.annotate.Substitute;
import com.oracle.svm.core.annotate.TargetClass;
import java.util.Comparator;

@TargetClass(UnsignedBytes.LexicographicalComparatorHolder.class)
final class LexicographicalComparatorHolderSubstitution {

  @Alias
  @RecomputeFieldValue(kind = RecomputeFieldValue.Kind.FromAlias)
  static Comparator<byte[]> BEST_COMPARATOR = UnsignedBytes.lexicographicalComparatorJavaImpl();

  /* All known cases should be covered by the field substitution above... keeping this only
   * for sake of completeness */
  @Substitute
  static Comparator<byte[]> getBestComparator() {
    return UnsignedBytes.lexicographicalComparatorJavaImpl();
  }
}
