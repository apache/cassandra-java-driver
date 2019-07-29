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

import com.datastax.oss.driver.api.mapper.MapperBuilder;
import com.datastax.oss.driver.api.mapper.MapperContext;
import com.datastax.oss.driver.api.mapper.entity.EntityHelper;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotates a {@link Dao} method that delegates the execution of the query to a user-provided
 * class.
 *
 * <p>Example:
 *
 * <pre>
 * &#64;Dao
 * public interface SensorDao {
 *   &#64;QueryProvider(providerClass = FindSliceProvider.class, entityHelpers = SensorReading.class)
 *   PagingIterable&lt;SensorReading&gt; findSlice(int id, Integer month, Integer day);
 * }
 *
 * public class FindSliceProvider {
 *   public FindSliceProvider(
 *       MapperContext context, EntityHelper&lt;SensorReading&gt; sensorReadingHelper) {
 *     ...
 *   }
 *
 *   public PagingIterable&lt;SensorReading&gt; findSlice(int id, Integer month, Integer day) {
 *     ... // implement the query logic here
 *   }
 * }
 * </pre>
 *
 * <p>Use this for requests that can't be expressed as static query strings, for example if some
 * clauses are added dynamically depending on the values of some parameters.
 *
 * <p>The parameters and return type are completely free-form, as long as they match those of the
 * provider method.
 *
 * @see MapperBuilder#withCustomState(Object, Object)
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface QueryProvider {

  /**
   * The class that will execute the query.
   *
   * <p>The mapper will create an instance of this class for each DAO instance. It must expose a
   * constructor that is accessible from the DAO interface's package, and takes the following
   * parameter types:
   *
   * <ul>
   *   <li>{@link MapperContext}.
   *   <li>zero or more {@link EntityHelper}s, as defined by {@link #entityHelpers()}.
   * </ul>
   *
   * It must also expose a method that will be invoked each time the DAO method is called, see
   * {@link #providerMethod()}.
   */
  Class<?> providerClass();

  /**
   * The method to invoke on the provider class.
   *
   * <p>It must be accessible from the DAO interface's package, and have the same parameters and
   * return type as the annotated DAO method.
   *
   * <p>This is optional; if not provided, it defaults to the name of the annotated DAO method.
   */
  String providerMethod() default "";

  /**
   * A list of entities for which {@link EntityHelper} instances should be injected into the
   * provider class's constructor (in addition to the mapper context).
   *
   * <p>For example, if {@code entityHelpers = {Product.class, Dimensions.class}}, your provider
   * class must expose a constructor that takes the parameter types{@code (MapperContext,
   * EntityHelper<Product>, EntityHelper<Dimensions>)}.
   *
   * <p>All provided classes must be annotated with {@link Entity}. Otherwise, the mapper will issue
   * a compile-time error.
   */
  Class<?>[] entityHelpers() default {};
}
