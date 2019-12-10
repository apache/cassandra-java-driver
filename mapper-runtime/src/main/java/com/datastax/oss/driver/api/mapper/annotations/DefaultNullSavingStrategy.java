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

import com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotates a {@link Dao} interface to define a default {@link NullSavingStrategy}, that will apply
 * to all methods that don't explicitly declare one.
 *
 * <p>For example, given this interface:
 *
 * <pre>
 * &#64;Dao
 * &#64;DefaultNullSavingStrategy(SET_TO_NULL)
 * public interface ProductDao {
 *
 *   &#64;Insert
 *   void insert(Product product);
 *
 *   &#64;Update(nullSavingStrategy = DO_NOT_SET)
 *   void update(Product product);
 * }
 * </pre>
 *
 * <ul>
 *   <li>{@code insert(Product)} will use {@link NullSavingStrategy#SET_TO_NULL SET_TO_NULL}
 *       (inherited from the DAO's default).
 *   <li>{@code update(Product)} will use {@link NullSavingStrategy#DO_NOT_SET DO_NOT_SET}.
 * </ul>
 *
 * If the DAO interface isn't annotated with {@link DefaultNullSavingStrategy}, any method that does
 * not declare its own value defaults to {@link NullSavingStrategy#DO_NOT_SET DO_NOT_SET}.
 *
 * <p>Note that null saving strategies are only relevant for {@link Update}, {@link Insert}, {@link
 * Query} and {@link SetEntity} methods.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface DefaultNullSavingStrategy {
  NullSavingStrategy value();
}
