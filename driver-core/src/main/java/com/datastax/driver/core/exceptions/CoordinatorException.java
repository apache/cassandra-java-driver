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

import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * An interface for exceptions that are able to report the address of the coordinator host
 * that was contacted.
 */
public interface CoordinatorException {

    /**
     * The coordinator host that was contacted.
     * <p/>
     * This is a shortcut for {@link InetSocketAddress#getAddress() getAddress().getAddress()}.
     *
     * @return The coordinator host that was contacted;
     * may be {@code null} if the coordinator is not known.
     */
    InetAddress getHost();

    /**
     * The full address of the coordinator host that was contacted.
     *
     * @return the full address of the coordinator host that was contacted;
     * may be {@code null} if the coordinator is not known.
     */
    InetSocketAddress getAddress();
}
