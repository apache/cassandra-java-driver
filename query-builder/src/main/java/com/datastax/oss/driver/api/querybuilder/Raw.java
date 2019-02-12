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
package com.datastax.oss.driver.api.querybuilder;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.querybuilder.condition.Condition;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.select.Selector;
import com.datastax.oss.driver.api.querybuilder.term.Term;

/**
 * A raw CQL snippet that will be appended to the query as-is, without any syntax checking or
 * escaping.
 *
 * <p>To build an instance of this type, use {@link QueryBuilder#raw(String)}.
 *
 * <p>It should be used with caution, as it's possible to generate invalid CQL that will fail at
 * execution time; on the other hand, it can be used as a workaround to handle new CQL features that
 * are not yet covered by the query builder.
 *
 * <p>For convenience, there is a single raw element in the query builder; it can be used in several
 * places: as a selector, relation, etc. The only downside is that the {@link #as(CqlIdentifier)}
 * method is only valid when used as a selector; make sure you don't use it elsewhere, or you will
 * generate invalid CQL that will fail at execution time.
 */
public interface Raw extends Selector, Relation, Condition, Term {}
