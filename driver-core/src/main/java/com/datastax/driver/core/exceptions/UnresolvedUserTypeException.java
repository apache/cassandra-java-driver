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
package com.datastax.driver.core.exceptions;

/**
 * Thrown when a user type cannot be resolved.
 * <p/>
 * This exception can be raised when the driver is rebuilding
 * its schema metadata, and a user-defined type cannot be completely
 * constructed due to some missing information.
 * It should only appear in the driver logs, never in client code.
 * It shouldn't be considered as a severe error as long as it only
 * appears occasionally.
 */
public class UnresolvedUserTypeException extends DriverException {

    private final String keyspaceName;

    private final String name;

    public UnresolvedUserTypeException(String keyspaceName, String name) {
        super(String.format("Cannot resolve user type %s.%s", keyspaceName, name));
        this.keyspaceName = keyspaceName;
        this.name = name;
    }

    private UnresolvedUserTypeException(String keyspaceName, String name, Throwable cause) {
        super(String.format("Cannot resolve user type %s.%s", keyspaceName, name), cause);
        this.keyspaceName = keyspaceName;
        this.name = name;
    }

    public String getKeyspaceName() {
        return keyspaceName;
    }

    public String getName() {
        return name;
    }

    @Override
    public UnresolvedUserTypeException copy() {
        return new UnresolvedUserTypeException(keyspaceName, name, this);
    }

}
