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
package com.datastax.driver.core;

import jnr.ffi.LibraryLoader;
import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import jnr.ffi.Struct;
import jnr.ffi.annotations.Out;
import jnr.ffi.annotations.Transient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

/**
 * Helper class to deal with native system calls through the
 * <a href="https://github.com/jnr/jnr-ffi">JNR library</a>.
 * <p/>
 * The driver can benefit from native system calls to improve its performance and accuracy
 * in some situations.
 * <p/>
 * Currently, the following features may be used by the driver when available:
 * <ol>
 * <li>{@link #currentTimeMicros()}: thanks to a system call to {@code gettimeofday()},
 * the driver is able to generate timestamps with true microsecond precision
 * (see {@link AtomicMonotonicTimestampGenerator} or {@link ThreadLocalMonotonicTimestampGenerator} for
 * more information);</li>
 * <li>{@link #processId()}: thanks to a system call to {@code getpid()},
 * the driver has access to the JVM's process ID it is running under – which
 * makes time-based UUID generation easier and more reliable (see {@link com.datastax.driver.core.utils.UUIDs UUIDs}
 * for more information).</li>
 * </ol>
 * <p/>
 * The availability of the aforementioned system calls depends on the underlying operation system's
 * capabilities. For instance, {@code gettimeofday()} is not available under Windows systems.
 * You can check if any of the system calls exposed through this class is available
 * by calling {@link #isGettimeofdayAvailable()} or {@link #isGetpidAvailable()}.
 * <p/>
 * Note: This class is public because it needs to be accessible from other packages of the Java driver,
 * but it is not meant to be used directly by client code.
 *
 * @see <a href="https://github.com/jnr/jnr-ffi">JNR library on Github</a>
 */
public final class Native {

    private static final Logger LOGGER = LoggerFactory.getLogger(Native.class);

    private static class LibCLoader {

        /**
         * Timeval struct.
         *
         * @see <a href="http://man7.org/linux/man-pages/man2/settimeofday.2.html">GETTIMEOFDAY(2)</a>
         */
        static class Timeval extends Struct {

            public final time_t tv_sec = new time_t();

            public final Unsigned32 tv_usec = new Unsigned32();

            public Timeval(Runtime runtime) {
                super(runtime);
            }
        }

        /**
         * Interface for LIBC calls through JNR.
         * Note that this interface must be declared public.
         */
        public interface LibC {

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

        private static final boolean GETTIMEOFDAY_AVAILABLE;

        static {
            LibC libc;
            Runtime runtime = null;
            try {
                libc = LibraryLoader.create(LibC.class).load("c");
                runtime = Runtime.getRuntime(libc);
            } catch (Throwable t) {
                libc = null; // dereference proxy to library if runtime could not be loaded
                if (LOGGER.isDebugEnabled())
                    LOGGER.debug("Could not load JNR C Library, native system calls through this library will not be available", t);
                else
                    LOGGER.info("Could not load JNR C Library, native system calls through this library will not be available " +
                            "(set this logger level to DEBUG to see the full stack trace).");

            }
            LIB_C = libc;
            LIB_C_RUNTIME = runtime;
            boolean gettimeofday = false;
            if (LIB_C_RUNTIME != null) {
                try {
                    gettimeofday = LIB_C.gettimeofday(new Timeval(LIB_C_RUNTIME), null) == 0;
                } catch (Throwable t) {
                    if (LOGGER.isDebugEnabled())
                        LOGGER.debug("Native calls to gettimeofday() not available on this system.", t);
                    else
                        LOGGER.info("Native calls to gettimeofday() not available on this system " +
                                "(set this logger level to DEBUG to see the full stack trace).");
                }
            }
            GETTIMEOFDAY_AVAILABLE = gettimeofday;
        }

    }

    private static class PosixLoader {

        public static final jnr.posix.POSIX POSIX;

        private static final boolean GETPID_AVAILABLE;

        static {
            jnr.posix.POSIX posix;
            try {
                // use reflection below to get the classloader a chance to load this class
                Class<?> posixHandler = Class.forName("jnr.posix.POSIXHandler");
                Class<?> defaultPosixHandler = Class.forName("jnr.posix.util.DefaultPOSIXHandler");
                Class<?> posixFactory = Class.forName("jnr.posix.POSIXFactory");
                Method getPOSIX = posixFactory.getMethod("getPOSIX", posixHandler, Boolean.TYPE);
                posix = (jnr.posix.POSIX) getPOSIX.invoke(null, defaultPosixHandler.newInstance(), true);
            } catch (Throwable t) {
                posix = null;
                if (LOGGER.isDebugEnabled())
                    LOGGER.debug("Could not load JNR POSIX Library, native system calls through this library will not be available.", t);
                else
                    LOGGER.info("Could not load JNR POSIX Library, native system calls through this library will not be available " +
                            "(set this logger level to DEBUG to see the full stack trace).");
            }
            POSIX = posix;
            boolean getpid = false;
            if (POSIX != null) {
                try {
                    POSIX.getpid();
                    getpid = true;
                } catch (Throwable t) {
                    if (LOGGER.isDebugEnabled())
                        LOGGER.debug("Native calls to getpid() not available on this system.", t);
                    else
                        LOGGER.info("Native calls to getpid() not available on this system " +
                                "(set this logger level to DEBUG to see the full stack trace).");
                }
            }
            GETPID_AVAILABLE = getpid;
        }

    }

    /**
     * Returns {@code true} if JNR C library is loaded and
     * a call to {@code gettimeofday} is possible through this library
     * on this system, and {@code false} otherwise.
     *
     * @return {@code true} if JNR C library is loaded and
     * a call to {@code gettimeofday} is possible.
     */
    public static boolean isGettimeofdayAvailable() {
        try {
            return LibCLoader.GETTIMEOFDAY_AVAILABLE;
        } catch (NoClassDefFoundError e) {
            return false;
        }
    }

    /**
     * Returns {@code true} if JNR POSIX library is loaded and
     * a call to {@code getpid} is possible through this library
     * on this system, and {@code false} otherwise.
     *
     * @return {@code true} if JNR POSIX library is loaded and
     * a call to {@code getpid} is possible.
     */
    public static boolean isGetpidAvailable() {
        try {
            return PosixLoader.GETPID_AVAILABLE;
        } catch (NoClassDefFoundError e) {
            return false;
        }

    }

    /**
     * Returns the current timestamp with microsecond precision
     * via a system call to {@code gettimeofday}, through JNR C library.
     *
     * @return the current timestamp with microsecond precision.
     * @throws UnsupportedOperationException if JNR C library is not loaded or {@code gettimeofday} is not available.
     * @throws IllegalStateException         if the call to {@code gettimeofday} did not complete with return code 0.
     */
    public static long currentTimeMicros() {
        if (!isGettimeofdayAvailable())
            throw new UnsupportedOperationException("JNR C library not loaded or gettimeofday not available");
        LibCLoader.Timeval tv = new LibCLoader.Timeval(LibCLoader.LIB_C_RUNTIME);
        int res = LibCLoader.LIB_C.gettimeofday(tv, null);
        if (res != 0)
            throw new IllegalStateException("Call to gettimeofday failed with result " + res);
        return tv.tv_sec.get() * 1000000 + tv.tv_usec.get();
    }

    /**
     * Returns the JVM's process identifier (PID)
     * via a system call to {@code getpid}.
     *
     * @return the JVM's process identifier (PID).
     * @throws UnsupportedOperationException if JNR POSIX library is not loaded or {@code getpid} is not available.
     */
    public static int processId() {
        if (!isGetpidAvailable())
            throw new UnsupportedOperationException("JNR POSIX library not loaded or getpid not available");
        return PosixLoader.POSIX.getpid();
    }

}
