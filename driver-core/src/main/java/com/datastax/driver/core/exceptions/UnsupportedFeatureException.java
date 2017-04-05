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

import com.datastax.driver.core.ProtocolVersion;

/**
 * Exception thrown when a feature is not supported by the native protocol
 * currently in use.
 */
public class UnsupportedFeatureException extends DriverException {

    private static final long serialVersionUID = 0;

    private final ProtocolVersion currentVersion;

    public UnsupportedFeatureException(ProtocolVersion currentVersion, String msg) {
        super("Unsupported feature with the native protocol " + currentVersion + " (which is currently in use): " + msg);
        this.currentVersion = currentVersion;
    }

    public ProtocolVersion getCurrentVersion() {
        return currentVersion;
    }

}
