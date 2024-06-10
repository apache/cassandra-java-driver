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
package com.datastax.oss.driver.api.querybuilder.select;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * The beginning of a SELECT query.
 *
 * <p>It only knows about the table, and optionally whether the statement uses JSON or DISTINCT. It
 * is not buildable yet: at least one selector needs to be specified.
 */
public interface SelectFrom extends OngoingSelection {

  // Implementation note - this interface exists to make the following a compile-time error:
  // selectFrom("foo").distinct().build()

  @NonNull
  SelectFrom json();

  @NonNull
  SelectFrom distinct();
}
