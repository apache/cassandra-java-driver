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
package com.datastax.driver.mapping;

import com.datastax.driver.core.CCMTestsSupport;
import com.datastax.driver.mapping.configuration.naming.CommonNamingConventions;
import com.datastax.driver.mapping.configuration.naming.NamingStrategy;
import org.testng.annotations.Test;

import static org.testng.Assert.fail;

@SuppressWarnings("unused")
public class PropertyNamingStrategyTest extends CCMTestsSupport {

    @Override
    public void onTestContextInitialized() {
//        execute("CREATE TABLE foo (k int primary key, v int)");
//        execute("INSERT INTO foo (k, v) VALUES (1, 1)");
    }

    @Test(groups = "short")
    public void should_not_inherit_properties() {
        NamingStrategy strategy = new NamingStrategy(new CommonNamingConventions.LowerCamelCase(), new CommonNamingConventions.LowerSnakeCase());
        String x = strategy.toJava("camel_case_is_shit");
        x = strategy.toCassandra("m_MySQLPlayer");
        int i = 1;
    }

}
