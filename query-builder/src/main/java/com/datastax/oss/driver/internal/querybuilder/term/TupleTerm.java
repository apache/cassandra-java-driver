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
package com.datastax.oss.driver.internal.querybuilder.term;

import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.datastax.oss.driver.internal.querybuilder.CqlHelper;
import edu.umd.cs.findbugs.annotations.NonNull;
import net.jcip.annotations.Immutable;

@Immutable
public class TupleTerm implements Term {

  private final Iterable<? extends Term> components;

  public TupleTerm(@NonNull Iterable<? extends Term> components) {
    this.components = components;
  }

  @Override
  public void appendTo(@NonNull StringBuilder builder) {
    CqlHelper.append(components, builder, "(", ",", ")");
  }

  @Override
  public boolean isIdempotent() {
    for (Term component : components) {
      if (!component.isIdempotent()) {
        return false;
      }
    }
    return true;
  }

  @NonNull
  public Iterable<? extends Term> getComponents() {
    return components;
  }
}
