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
import com.datastax.oss.driver.api.querybuilder.select.Selector;
import com.datastax.oss.driver.api.querybuilder.term.Term;

/**
 * A value that will be appended as a CQL literal.
 *
 * <p>For convenience, it can be used both as a selector and a term. The only downside is that the
 * {@link #as(CqlIdentifier)} method is only valid when used as a selector; make sure you don't use
 * it elsewhere, or you will generate invalid CQL that will fail at execution time.
 */
public interface Literal extends Selector, Term {}
