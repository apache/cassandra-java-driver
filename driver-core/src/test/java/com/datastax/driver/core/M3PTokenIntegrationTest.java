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

import com.datastax.driver.core.Token.M3PToken;

public class M3PTokenIntegrationTest extends TokenIntegrationTest {

    public M3PTokenIntegrationTest() {
        super(DataType.bigint(), false);
    }

    @Override
    protected Token.Factory tokenFactory() {
        return M3PToken.FACTORY;
    }
}
