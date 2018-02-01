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
package com.datastax.driver.core.utils;

import java.net.InetAddress;
import java.util.NoSuchElementException;

/**
 * Combine an InetAddress with an optional port value. Avoiding a dependency on j.u.Optional and c.g.c.Optional.
 *
 * The correct port value may not be known at any given time and will have to be
 * retrieved from some other config.
 */
public class InetAddressOptPort {
    public final InetAddress address;
    private final Integer port;

    public InetAddressOptPort(InetAddress address, Integer port)
    {
        if (address == null)
        {
            throw new NullPointerException("Address can't be null");
        }
        this.address = address;
        this.port = port;
    }

    /**
     * Return the port if it's available or the provided value.
     * @param otherPort Alternative port to use if the port is unknown.
     * @return The port if present or the provided alternative
     */
    public int portOrElse(int otherPort)
    {
        return port != null ? port : otherPort;
    }

    /**
     * True if the port is present, false otherwise
     * @return True if the port is present, false otherwise.
     */
    public boolean hasPort()
    {
        return port != null;
    }

    public int getPort()
    {
        if (port == null)
        {
            throw new NoSuchElementException("No port present");
        }
        return port;
    }

    @Override
    public String toString()
    {
        return port != null ? address.toString() + ":" + port : address.toString();
    }

}
