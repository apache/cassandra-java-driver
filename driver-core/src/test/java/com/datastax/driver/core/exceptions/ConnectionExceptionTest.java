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
package com.datastax.driver.core.exceptions;

import org.testng.annotations.Test;

import java.net.InetSocketAddress;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class ConnectionExceptionTest {

    /**
     * @jira_ticket JAVA-1139
     */
    @Test(groups = "unit")
    public void getHost_should_return_null_if_address_is_null() {
        assertNull(new ConnectionException(null, "Test message").getHost());
    }

    /**
     * @jira_ticket JAVA-1139
     */
    @Test(groups = "unit")
    public void getMessage_should_return_message_if_address_is_null() {
        assertEquals(new ConnectionException(null, "Test message").getMessage(), "Test message");
    }

    /**
     * @jira_ticket JAVA-1139
     */
    @Test(groups = "unit")
    public void getMessage_should_return_message_if_address_is_unresolved() {
        assertEquals(new ConnectionException(InetSocketAddress.createUnresolved("127.0.0.1", 9042), "Test message").getMessage(), "[127.0.0.1:9042] Test message");
    }

    /**
     * @jira_ticket JAVA-1139
     */
    @Test(groups = "unit")
    public void getMessage_should_return_message_if_address_is_resolved() {
        assertEquals(new ConnectionException(new InetSocketAddress("127.0.0.1", 9042), "Test message").getMessage(), "[/127.0.0.1:9042] Test message");
    }
}
