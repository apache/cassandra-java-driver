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
package com.datastax.dse.driver.internal.core.metadata.schema;

import com.datastax.dse.driver.api.core.metadata.schema.DseVertexMetadata;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import java.io.Serializable;
import java.util.Objects;
import javax.annotation.Nonnull;

public class DefaultDseVertexMetadata implements DseVertexMetadata, Serializable {

  private static final long serialVersionUID = 1;

  @Nonnull private final CqlIdentifier labelName;

  public DefaultDseVertexMetadata(@Nonnull CqlIdentifier labelName) {
    this.labelName = Preconditions.checkNotNull(labelName);
  }

  @Nonnull
  @Override
  public CqlIdentifier getLabelName() {
    return labelName;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof DefaultDseVertexMetadata) {
      DefaultDseVertexMetadata that = (DefaultDseVertexMetadata) other;
      return Objects.equals(this.labelName, that.getLabelName());
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return labelName.hashCode();
  }
}
