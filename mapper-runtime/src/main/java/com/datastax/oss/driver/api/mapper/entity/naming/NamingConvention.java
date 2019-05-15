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
package com.datastax.oss.driver.api.mapper.entity.naming;

import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.NamingStrategy;

/**
 * A built-in convention to infer CQL column names from the names used in an {@link
 * Entity}-annotated class.
 *
 * @see NamingStrategy
 */
public enum NamingConvention {

  /**
   * Uses the Java name as-is, as a <em>case-insensitive</em> CQL name, for example {@code Product
   * => Product, productId => productId}.
   *
   * <p>In practice this is the same as lower-casing everything ({@code product, productid}), but it
   * makes generated queries a bit easier to read.
   */
  CASE_INSENSITIVE,

  /**
   * Uses the Java name as-is, as a <em>case-sensitive</em> CQL name, for example {@code Product =>
   * "Product", productId => "productId"}.
   *
   * <p>Use this if your schema uses camel case and you want to preserve capitalization in table
   * names.
   */
  EXACT_CASE,

  /**
   * Divide the Java name into words, splitting on upper-case characters; capitalize every word
   * except the first; then concatenate the words and make the result a <em>case-sensitive</em> CQL
   * name, for example {@code Product => "product", productId => "productId"}.
   */
  LOWER_CAMEL_CASE,

  /**
   * Divide the Java name into words, splitting on upper-case characters; capitalize every word;
   * then concatenate the words and make the result a <em>case-sensitive</em> CQL name, for example
   * {@code Product => "Product", productId => "ProductId"}.
   */
  UPPER_CAMEL_CASE,

  /**
   * Divide the Java name into words, splitting on upper-case characters; lower-case everything;
   * then concatenate the words with underscore separators, and make the result a
   * <em>case-insensitive</em> CQL name, for example {@code Product => product, productId =>
   * product_id}.
   */
  SNAKE_CASE_INSENSITIVE,

  /**
   * Divide the Java name into words, splitting on upper-case characters; upper-case everything;
   * then concatenate the words with underscore separators, and make the result a
   * <em>case-sensitive</em> CQL name, for example {@code Product => "PRODUCT", productId =>
   * "PRODUCT_ID"}.
   */
  UPPER_SNAKE_CASE,

  /**
   * Upper-case everything, and make the result a <em>case-sensitive</em> CQL name, for example
   * {@code Product => "PRODUCT", productId => "PRODUCTID"}.
   */
  UPPER_CASE,
}
