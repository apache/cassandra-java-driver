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

import jnr.ffi.LibraryLoader;
import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import jnr.ffi.Struct;
import jnr.ffi.annotations.Out;
import jnr.ffi.annotations.Transient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class to deal with native system call through JNR.
 */
class Native {

    private static final Logger LOGGER = LoggerFactory.getLogger(Native.class);

    /**
     * Interface for LIBC calls through JNR.
     * Note that this interface must be declared public.
     */
    public interface LibC {

        /**
         * Timeval struct.
         *
         * @see <a href="http://man7.org/linux/man-pages/man2/settimeofday.2.html">GETTIMEOFDAY(2)</a>
         */
        class Timeval extends Struct {

            public final time_t tv_sec = new time_t();

            public final Unsigned32 tv_usec = new Unsigned32();

            public Timeval(Runtime runtime) {
                super(runtime);
            }
        }

        /**
         * JNR call to {@code gettimeofday}.
         *
         * @param tv     Timeval struct
         * @param unused Timezone struct (unused)
         * @return 0 for success, or -1 for failure
         * @see <a href="http://man7.org/linux/man-pages/man2/settimeofday.2.html">GETTIMEOFDAY(2)</a>
         */
        int gettimeofday(@Out @Transient Timeval tv, Pointer unused);
    }

    private static final LibC LIB_C;

    private static final Runtime LIB_C_RUNTIME;

    static {
        LibC libc = null;
        Runtime runtime = null;
        try {
            libc = LibraryLoader.create(LibC.class).load("c");
            runtime = Runtime.getRuntime(libc);
        } catch (Throwable t) {
            if (LOGGER.isDebugEnabled())
                LOGGER.debug("Could not load JNR LibC Library, native calls will not be available", t);
            else
                LOGGER.info("Could not load JNR LibC Library, native calls will not be available (set this logger level to DEBUG to see the full stack trace)");
        }
        LIB_C = libc;
        LIB_C_RUNTIME = runtime;
    }

    /**
     * Returns true if LibC could be loaded with JNR.
     *
     * @return true if LibC could be loaded with JNR, false otherwise.
     */
    static boolean isLibCLoaded() {
        return LIB_C_RUNTIME != null;
    }

    /**
     * Returns the current timestamp with microsecond precision
     * via a system call to {@code gettimeofday}.
     *
     * @return the current timestamp with microsecond precision.
     */
    static long currentTimeMicros() {
        LibC.Timeval tv = new LibC.Timeval(LIB_C_RUNTIME);
        if (LIB_C.gettimeofday(tv, null) != 0)
            LOGGER.error("gettimeofday failed");
        return tv.tv_sec.get() * 1000000 + tv.tv_usec.get();
    }

}
