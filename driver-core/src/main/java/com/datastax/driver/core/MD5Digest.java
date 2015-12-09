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

import com.datastax.driver.core.utils.Bytes;

import java.util.Arrays;

/**
 * The result of the computation of an MD5 digest.
 * <p/>
 * A MD5 is really just a byte[] but arrays are a no go as map keys. We could
 * wrap it in a ByteBuffer but:
 * 1. MD5Digest is a more explicit name than ByteBuffer to represent a md5.
 * 2. Using our own class allows to use our FastByteComparison for equals.
 */
class MD5Digest {

    public final byte[] bytes;

    private MD5Digest(byte[] bytes) {
        this.bytes = bytes;
    }

    public static MD5Digest wrap(byte[] digest) {
        return new MD5Digest(digest);
    }

    @Override
    public final int hashCode() {
        return Arrays.hashCode(bytes);
    }

    @Override
    public final boolean equals(Object o) {
        if (!(o instanceof MD5Digest))
            return false;
        MD5Digest that = (MD5Digest) o;
        // handles nulls properly
        return Arrays.equals(this.bytes, that.bytes);
    }

    @Override
    public String toString() {
        return Bytes.toHexString(bytes);
    }
}

