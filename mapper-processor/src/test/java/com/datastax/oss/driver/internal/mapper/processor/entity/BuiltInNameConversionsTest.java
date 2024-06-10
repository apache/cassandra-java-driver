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
package com.datastax.oss.driver.internal.mapper.processor.entity;

import static com.datastax.oss.driver.api.mapper.entity.naming.NamingConvention.CASE_INSENSITIVE;
import static com.datastax.oss.driver.api.mapper.entity.naming.NamingConvention.EXACT_CASE;
import static com.datastax.oss.driver.api.mapper.entity.naming.NamingConvention.LOWER_CAMEL_CASE;
import static com.datastax.oss.driver.api.mapper.entity.naming.NamingConvention.SNAKE_CASE_INSENSITIVE;
import static com.datastax.oss.driver.api.mapper.entity.naming.NamingConvention.UPPER_CAMEL_CASE;
import static com.datastax.oss.driver.api.mapper.entity.naming.NamingConvention.UPPER_CASE;
import static com.datastax.oss.driver.api.mapper.entity.naming.NamingConvention.UPPER_SNAKE_CASE;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.mapper.entity.naming.NamingConvention;
import org.junit.Test;

public class BuiltInNameConversionsTest {

  @Test
  public void should_convert_to_cql() {
    should_convert_to_cql("Product", CASE_INSENSITIVE, "Product");
    should_convert_to_cql("productId", CASE_INSENSITIVE, "productId");

    should_convert_to_cql("Product", EXACT_CASE, "\"Product\"");
    should_convert_to_cql("productId", EXACT_CASE, "\"productId\"");

    should_convert_to_cql("Product", LOWER_CAMEL_CASE, "\"product\"");
    should_convert_to_cql("productId", LOWER_CAMEL_CASE, "\"productId\"");

    should_convert_to_cql("Product", UPPER_CAMEL_CASE, "\"Product\"");
    should_convert_to_cql("productId", UPPER_CAMEL_CASE, "\"ProductId\"");

    should_convert_to_cql("Product", SNAKE_CASE_INSENSITIVE, "product");
    should_convert_to_cql("productId", SNAKE_CASE_INSENSITIVE, "product_id");

    should_convert_to_cql("Product", UPPER_SNAKE_CASE, "\"PRODUCT\"");
    should_convert_to_cql("productId", UPPER_SNAKE_CASE, "\"PRODUCT_ID\"");

    should_convert_to_cql("Product", UPPER_CASE, "\"PRODUCT\"");
    should_convert_to_cql("productId", UPPER_CASE, "\"PRODUCTID\"");
  }

  private void should_convert_to_cql(
      String javaName, NamingConvention convention, String expectedCqlName) {
    String actualCqlName = BuiltInNameConversions.toCassandraName(javaName, convention);
    assertThat(actualCqlName).isEqualTo(expectedCqlName);
  }
}
