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
package com.datastax.oss.driver.internal.querybuilder.select;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.querybuilder.select.Selector;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import net.jcip.annotations.Immutable;

@Immutable
public class SetSelector extends CollectionSelector {

  public SetSelector(@NonNull Iterable<Selector> elementSelectors) {
    this(elementSelectors, null);
  }

  public SetSelector(@NonNull Iterable<Selector> elementSelectors, @Nullable CqlIdentifier alias) {
    super(elementSelectors, "{", "}", alias);
  }

  @NonNull
  @Override
  public Selector as(@NonNull CqlIdentifier alias) {
    return new SetSelector(getElementSelectors(), alias);
  }
}
