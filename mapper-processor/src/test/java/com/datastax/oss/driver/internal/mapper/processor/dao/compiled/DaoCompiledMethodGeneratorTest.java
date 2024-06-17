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
package com.datastax.oss.driver.internal.mapper.processor.dao.compiled;

import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.internal.mapper.processor.dao.DaoMethodGeneratorTest;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.TypeSpec;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import javax.lang.model.element.Modifier;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class DaoCompiledMethodGeneratorTest extends DaoMethodGeneratorTest {

  @Test
  public void should_fail_with_expected_error() {
    should_fail_with_expected_error(
        "[CompiledProductDao.findByDescriptionCompiledWrong] "
            + "Parameter arg0 is declared in a compiled method "
            + "and refers to a bind marker "
            + "and thus must be annotated with @CqlName",
        "test",
        ENTITY_SPEC,
        TypeSpec.interfaceBuilder(ClassName.get("test", "ProductDao"))
            .addModifiers(Modifier.PUBLIC)
            .addSuperinterface(CompiledProductDao.class)
            .addAnnotation(Dao.class)
            .build());
  }
}
