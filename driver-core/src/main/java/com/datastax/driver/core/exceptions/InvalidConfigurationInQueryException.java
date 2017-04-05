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

import java.net.InetSocketAddress;

/**
 * A specific invalid query exception that indicates that the query is invalid
 * because of some configuration problem.
 * <p/>
 * This is generally throw by query that manipulate the schema (CREATE and
 * ALTER) when the required configuration options are invalid.
 */
public class InvalidConfigurationInQueryException extends InvalidQueryException implements CoordinatorException {

    private static final long serialVersionUID = 0;

    public InvalidConfigurationInQueryException(InetSocketAddress address, String msg) {
        super(address, msg);
    }

    @Override
    public InvalidConfigurationInQueryException copy() {
        return new InvalidConfigurationInQueryException(getAddress(), getMessage());
    }
}
