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
package com.datastax.oss.driver.api.mapper.annotations;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.data.SettableByName;
import com.datastax.oss.driver.api.core.data.UdtValue;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotates a {@link Dao} method that fills a core driver data structure from an instance of an
 * {@link Entity} class.
 *
 * <p>Example:
 *
 * <pre>
 * public interface ProductDao {
 *   &#64;SetEntity
 *   BoundStatement bind(Product product, BoundStatement boundStatement);
 * }
 * </pre>
 *
 * The generated code will set each entity property on the target, such as:
 *
 * <pre>
 * boundStatement = boundStatement.set("id", product.getId(), UUID.class);
 * boundStatement = boundStatement.set("description", product.getDescription(), String.class);
 * ...
 * </pre>
 *
 * It does not perform a query. Instead, those methods are intended for cases where you will execute
 * the query yourself, and just need the conversion logic.
 *
 * <h3>Parameters</h3>
 *
 * The method must have two parameters: one is the entity instance, the other must be a subtype of
 * {@link SettableByName} (the most likely candidates are {@link BoundStatement}, {@link
 * BoundStatementBuilder} and {@link UdtValue}). Note that you can't use {@link SettableByName}
 * itself.
 *
 * <p>The order of the parameters does not matter.
 *
 * <h3>Return type</h3>
 *
 * The method can either be void, or return the exact same type as its settable parameter.
 *
 * <pre>
 * &#64;SetEntity
 * void bind(Product product, UdtValue udtValue);
 *
 * &#64;SetEntity
 * void bind(Product product, BoundStatementBuilder builder);
 * </pre>
 *
 * Note that if the settable parameter is immutable, the method should return a new instance,
 * because the generated code won't be able to modify the argument in place. This is the case for
 * {@link BoundStatement}, which is immutable in the driver:
 *
 * <pre>
 * // Wrong: statement won't be modified
 * &#64;SetEntity
 * void bind(Product product, BoundStatement statement);
 *
 * // Do this instead:
 * &#64;SetEntity
 * BoundStatement bind(Product product, BoundStatement statement);
 * </pre>
 *
 * If you use a void method with {@link BoundStatement}, the mapper processor will issue a
 * compile-time warning.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.CLASS)
public @interface SetEntity {}
