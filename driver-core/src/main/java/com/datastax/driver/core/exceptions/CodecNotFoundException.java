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

import com.datastax.driver.core.DataType;
import com.google.common.reflect.TypeToken;

/**
 * Thrown when a suitable {@link com.datastax.driver.core.TypeCodec} cannot be found by
 * {@link com.datastax.driver.core.CodecRegistry} instances.
 */
@SuppressWarnings("serial")
public class CodecNotFoundException extends DriverException {

    private final DataType cqlType;

    private final TypeToken<?> javaType;

    public CodecNotFoundException(String msg, DataType cqlType, TypeToken<?> javaType) {
        super(msg);
        this.cqlType = cqlType;
        this.javaType = javaType;
    }

    public CodecNotFoundException(Throwable cause, DataType cqlType, TypeToken<?> javaType) {
        super(cause);
        this.cqlType = cqlType;
        this.javaType = javaType;
    }

    public DataType getCqlType() {
        return cqlType;
    }

    public TypeToken<?> getJavaType() {
        return javaType;
    }
}
