/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.testng.annotations.Test;

public class UserTypeTest {
    @Test(groups = "unit")
    public void should_use_all_fields_in_equals_and_hashCode() {
        EqualsVerifier.forClass(UserType.class)
            .allFieldsShouldBeUsedExcept("protocolVersion", "codecRegistry", "byName")
            .verify();
    }
}
