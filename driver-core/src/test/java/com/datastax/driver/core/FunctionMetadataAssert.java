/*
 * Copyright (C) 2012-2017 DataStax Inc.
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
package com.datastax.driver.core;

import org.assertj.core.api.AbstractAssert;

import static org.assertj.core.api.Assertions.assertThat;

public class FunctionMetadataAssert extends AbstractAssert<FunctionMetadataAssert, FunctionMetadata> {
    protected FunctionMetadataAssert(FunctionMetadata actual) {
        super(actual, FunctionMetadataAssert.class);
    }

    public FunctionMetadataAssert hasSignature(String name) {
        assertThat(actual.getSignature()).isEqualTo(name);
        return this;
    }

    public FunctionMetadataAssert isInKeyspace(String keyspaceName) {
        assertThat(actual.getKeyspace().getName()).isEqualTo(keyspaceName);
        return this;
    }

    public FunctionMetadataAssert hasBody(String body) {
        assertThat(actual.getBody()).isEqualTo(body);
        return this;
    }
}
