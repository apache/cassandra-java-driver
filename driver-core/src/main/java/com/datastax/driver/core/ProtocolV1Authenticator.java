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

import java.util.Map;

/**
 * Parent class for {@link Authenticator} implementations that support native protocol v1 authentication.
 * <p/>
 * Protocol v1 uses simple, credentials-based authentication (as opposed to SASL for later protocol versions).
 * In order to support protocol v1, an authenticator must extend this class.
 * <p/>
 * We use an abstract class instead of an interface because we don't want to expose {@link #getCredentials()}.
 *
 * @see <a href="https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v1.spec">Native protocol v1 specification</a>
 */
abstract class ProtocolV1Authenticator {
    abstract Map<String, String> getCredentials();
}
