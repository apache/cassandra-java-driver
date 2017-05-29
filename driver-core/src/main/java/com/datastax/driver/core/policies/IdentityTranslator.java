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
package com.datastax.driver.core.policies;

import com.datastax.driver.core.Cluster;

import java.net.InetSocketAddress;

/**
 * The default {@link AddressTranslator} used by the driver that do no
 * translation.
 */
public class IdentityTranslator implements AddressTranslator {

    @Override
    public void init(Cluster cluster) {
        // Nothing to do
    }

    /**
     * Translates a Cassandra {@code rpc_address} to another address if necessary.
     * <p/>
     * This method is the identity function, it always return the address passed
     * in argument, doing no translation.
     *
     * @param address the address of a node as returned by Cassandra.
     * @return {@code address} unmodified.
     */
    @Override
    public InetSocketAddress translate(InetSocketAddress address) {
        return address;
    }

    @Override
    public void close() {
    }
}
