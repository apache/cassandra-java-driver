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
package com.datastax.dse.driver.api.core.metadata.schema;

import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;

/**
 * Specialized table metadata for DSE.
 *
 * <p>Notes:
 *
 * <ul>
 *   <li>this type can always be safely downcast to {@link DseGraphTableMetadata} (the only reason
 *       the two interfaces are separate is for backward compatibility).
 *   <li>all returned {@link ColumnMetadata} can be cast to {@link DseColumnMetadata}, and all
 *       {@link IndexMetadata} to {@link DseIndexMetadata}.
 * </ul>
 */
public interface DseTableMetadata extends DseRelationMetadata, TableMetadata {}
