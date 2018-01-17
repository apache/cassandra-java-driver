/*
 * Copyright DataStax, Inc.
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

import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * Indicates a syntactically correct but invalid query.
 */
public class InvalidQueryException extends QueryValidationException implements CoordinatorException {

    private static final long serialVersionUID = 0;

    private final InetSocketAddress address;

    public InvalidQueryException(String msg) {
        this(null, msg);
    }

    public InvalidQueryException(InetSocketAddress address, String msg) {
        super(msg);
        this.address = address;
    }

    public InvalidQueryException(String msg, Throwable cause) {
        this(null, msg, cause);
    }

    public InvalidQueryException(InetSocketAddress address, String msg, Throwable cause) {
        super(msg, cause);
        this.address = address;
    }

    @Override
    public DriverException copy() {
        return new InvalidQueryException(getAddress(), getMessage(), this);
    }

    @Override
    public InetAddress getHost() {
        return address != null ? address.getAddress() : null;
    }

    @Override
    public InetSocketAddress getAddress() {
        return address;
    }
}
