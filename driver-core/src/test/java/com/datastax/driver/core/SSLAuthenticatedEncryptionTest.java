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

import com.google.common.base.Optional;
import org.testng.annotations.Test;

import com.datastax.driver.core.exceptions.NoHostAvailableException;

import static com.datastax.driver.core.CCMBridge.DEFAULT_CLIENT_KEYSTORE_PATH;
import static com.datastax.driver.core.CCMBridge.DEFAULT_CLIENT_TRUSTSTORE_PATH;

public class SSLAuthenticatedEncryptionTest extends SSLTestBase {

    public SSLAuthenticatedEncryptionTest() {
        super(true);
    }

    /**
     * <p>
     * Validates that an SSL connection can be established with client auth if the target
     * cassandra cluster is using SSL, requires client auth and would validate with the client's
     * certificate.
     * </p>
     *
     * @test_category connection:ssl, authentication
     * @expected_result Connection can be established to a cassandra node using SSL that requires client auth.
     */
    @Test(groups="short")
    public void should_connect_with_ssl_with_client_auth_and_node_requires_auth() throws Exception {
        connectWithSSLOptions(getSSLOptions(Optional.of(DEFAULT_CLIENT_KEYSTORE_PATH), Optional.of(DEFAULT_CLIENT_TRUSTSTORE_PATH)));
    }


    /**
     * <p>
     * Validates that an SSL connection can not be established with if the target
     * cassandra cluster is using SSL, requires client auth, but the client does not provide
     * sufficient certificate authentication.
     * </p>
     *
     * @test_category connection:ssl, authentication
     * @expected_result Connection is not established.
     */
    @Test(groups="short", expectedExceptions={NoHostAvailableException.class})
    public void should_not_connect_without_client_auth_but_node_requires_auth() throws Exception {
        connectWithSSLOptions(getSSLOptions(Optional.<String>absent(), Optional.of(DEFAULT_CLIENT_TRUSTSTORE_PATH)));
    }
}
