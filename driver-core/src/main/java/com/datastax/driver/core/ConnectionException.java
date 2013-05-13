/*
 *      Copyright (C) 2012 DataStax Inc.
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

import java.net.InetAddress;

class ConnectionException extends Exception {

    private static final long serialVersionUID = 0;

    public final InetAddress address;

    public ConnectionException(InetAddress address, String msg, Throwable cause)
    {
        super(msg, cause);
        this.address = address;
    }

    public ConnectionException(InetAddress address, String msg)
    {
        super(msg);
        this.address = address;
    }

    @Override
    public String getMessage() {
        return String.format("[%s] %s", address, super.getMessage());
    }
}
