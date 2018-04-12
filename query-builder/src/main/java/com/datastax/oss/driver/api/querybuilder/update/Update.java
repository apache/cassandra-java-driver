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
package com.datastax.oss.driver.api.querybuilder.update;

import com.datastax.oss.driver.api.querybuilder.BuildableQuery;
import com.datastax.oss.driver.api.querybuilder.condition.ConditionalStatement;
import com.datastax.oss.driver.api.querybuilder.relation.OngoingWhereClause;

/**
 * A buildable UPDATE statement that has at least one assignment and one WHERE clause. You can keep
 * adding WHERE clauses, or add IF conditions.
 */
public interface Update
    extends OngoingWhereClause<Update>, ConditionalStatement<Update>, BuildableQuery {}
