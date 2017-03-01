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

import com.datastax.driver.core.exceptions.DriverInternalError;
import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.net.URL;
import java.util.Enumeration;
import java.util.concurrent.Executor;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A compatibility layer to support a wide range of Guava versions.
 * <p>
 * The driver is compatible with Guava 16.0.1 or higher, but Guava 20 introduced incompatible breaking changes in its
 * API, that could in turn be breaking for legacy driver clients if we simply upgraded our dependency. We don't want to
 * increment our major version "just" for Guava (we have other changes planned).
 * <p>
 * Therefore we depend on Guava 19, which has both the deprecated and the new APIs, and detect the actual version at
 * runtime in order to call the relevant methods.
 * <p>
 * This is a hack, and might not work with subsequent Guava releases; the real fix is to stop exposing Guava in our
 * public API. We'll address that in version 4 of the driver.
 */
@SuppressWarnings("deprecation")
public abstract class GuavaCompatibility {

    private static final Logger logger = LoggerFactory.getLogger(GuavaCompatibility.class);
    private static final Pattern GA_VERSION_EXTRACTOR = Pattern.compile("(\\d+\\.\\d+\\.\\d+).*");

    /**
     * The unique instance of this class, that is compatible with the Guava version found in the classpath.
     */
    public static final GuavaCompatibility INSTANCE = selectImplementation();

    /**
     * Force the initialization of the class. This should be called early to ensure a fast failure if an incompatible
     * version of Guava is in the classpath (the driver code calls it when loading the {@link Cluster} class).
     */
    public static void init() {
        // nothing to do, we just want the static initializers to run
    }

    /**
     * Returns a {@code Future} whose result is taken from the given primary
     * {@code input} or, if the primary input fails, from the {@code Future}
     * provided by the {@code fallback}.
     *
     * @see Futures#withFallback(ListenableFuture, FutureFallback)
     * @see Futures#catchingAsync(ListenableFuture, Class, AsyncFunction)
     */
    public abstract <V> ListenableFuture<V> withFallback(ListenableFuture<? extends V> input,
                                                         AsyncFunction<Throwable, V> fallback);

    /**
     * Returns a {@code Future} whose result is taken from the given primary
     * {@code input} or, if the primary input fails, from the {@code Future}
     * provided by the {@code fallback}.
     *
     * @see Futures#withFallback(ListenableFuture, FutureFallback, Executor)
     * @see Futures#catchingAsync(ListenableFuture, Class, AsyncFunction, Executor)
     */
    public abstract <V> ListenableFuture<V> withFallback(ListenableFuture<? extends V> input,
                                                         AsyncFunction<Throwable, V> fallback, Executor executor);

    /**
     * Returns a new {@code ListenableFuture} whose result is asynchronously
     * derived from the result of the given {@code Future}. More precisely, the
     * returned {@code Future} takes its result from a {@code Future} produced by
     * applying the given {@code AsyncFunction} to the result of the original
     * {@code Future}.
     *
     * @see Futures#transform(ListenableFuture, AsyncFunction)
     * @see Futures#transformAsync(ListenableFuture, AsyncFunction)
     */
    public abstract <I, O> ListenableFuture<O> transformAsync(ListenableFuture<I> input,
                                                              AsyncFunction<? super I, ? extends O> function);

    /**
     * Returns a new {@code ListenableFuture} whose result is asynchronously
     * derived from the result of the given {@code Future}. More precisely, the
     * returned {@code Future} takes its result from a {@code Future} produced by
     * applying the given {@code AsyncFunction} to the result of the original
     * {@code Future}.
     *
     * @see Futures#transform(ListenableFuture, AsyncFunction, Executor)
     * @see Futures#transformAsync(ListenableFuture, AsyncFunction, Executor)
     */
    public abstract <I, O> ListenableFuture<O> transformAsync(ListenableFuture<I> input,
                                                              AsyncFunction<? super I, ? extends O> function,
                                                              Executor executor);

    /**
     * Returns true if {@code target} is a supertype of {@code argument}. "Supertype" is defined
     * according to <a href="http://docs.oracle.com/javase/specs/jls/se8/html/jls-4.html#jls-4.5.1"
     * >the rules for type arguments</a> introduced with Java generics.
     *
     * @see TypeToken#isAssignableFrom(Type)
     * @see TypeToken#isSupertypeOf(Type)
     */
    public abstract boolean isSupertypeOf(TypeToken<?> target, TypeToken<?> argument);

    /**
     * Returns an {@link Executor} that runs each task in the thread that invokes
     * {@link Executor#execute execute}, as in {@link java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy}.
     *
     * @see MoreExecutors#sameThreadExecutor()
     * @see MoreExecutors#directExecutor()
     */
    public abstract Executor sameThreadExecutor();

    private static GuavaCompatibility selectImplementation() {
        String fullVersion = getBundleVersion(loadGuavaManifest());
        // Get rid of potential rc qualifier, as it could throw off the lexical comparisons
        String version = stripQualifiers(fullVersion);
        if (version.compareTo("16.0.1") < 0) {
            throw new DriverInternalError(String.format(
                    "Detected incompatible version of Guava in the classpath (%s). " +
                            "You need 16.0.1 or higher.", fullVersion));
        } else if (version.compareTo("19.0") < 0) {
            logger.info("Detected Guava {} in the classpath, using pre-19 compatibility layer", fullVersion);
            return new Version18OrLower();
        } else {
            logger.info("Detected Guava {} in the classpath, using 19+ compatibility layer", fullVersion);
            return new Version19OrHigher();
        }
    }

    private static Manifest loadGuavaManifest() {
        InputStream in = null;
        try {
            Enumeration<URL> resources = Preconditions.class.getClassLoader()
                    .getResources("META-INF/MANIFEST.MF");
            while (resources.hasMoreElements()) {
                in = resources.nextElement().openStream();
                Manifest manifest = new Manifest(in);
                Attributes mainAttributes = manifest.getMainAttributes();
                String symbolicName = mainAttributes.getValue("Bundle-SymbolicName");
                if ("com.google.guava".equals(symbolicName)) {
                    return manifest;
                }
            }
            throw new DriverInternalError("Error while looking up Guava manifest: " +
                    "no manifest with symbolic name 'com.google.guava' found in classpath.");
        } catch (Exception e) {
            throw new DriverInternalError("Error while looking up Guava manifest", e);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        }
    }

    private static String getBundleVersion(Manifest manifest) {
        return manifest.getMainAttributes().getValue("Bundle-Version");
    }

    private static String stripQualifiers(String fullVersion) {
        Matcher matcher = GA_VERSION_EXTRACTOR.matcher(fullVersion);
        if (matcher.matches()) {
            return matcher.group(1);
        } else {
            throw new DriverInternalError(String.format("Could not strip qualifiers from full Guava version %s", fullVersion));
        }
    }

    private static class Version18OrLower extends GuavaCompatibility {

        @Override
        public <V> ListenableFuture<V> withFallback(ListenableFuture<? extends V> input,
                                                    final AsyncFunction<Throwable, V> fallback) {
            return Futures.withFallback(input, new FutureFallback<V>() {
                @Override
                public ListenableFuture<V> create(Throwable t) throws Exception {
                    return fallback.apply(t);
                }
            });
        }

        @Override
        public <V> ListenableFuture<V> withFallback(ListenableFuture<? extends V> input,
                                                    final AsyncFunction<Throwable, V> fallback,
                                                    Executor executor) {
            return Futures.withFallback(input, new FutureFallback<V>() {
                @Override
                public ListenableFuture<V> create(Throwable t) throws Exception {
                    return fallback.apply(t);
                }
            }, executor);
        }

        @Override
        public <I, O> ListenableFuture<O> transformAsync(ListenableFuture<I> input, AsyncFunction<? super I, ? extends O> function) {
            return Futures.transform(input, function);
        }

        @Override
        public <I, O> ListenableFuture<O> transformAsync(ListenableFuture<I> input, AsyncFunction<? super I, ? extends O> function, Executor executor) {
            return Futures.transform(input, function, executor);
        }

        @Override
        public boolean isSupertypeOf(TypeToken<?> target, TypeToken<?> argument) {
            return target.isAssignableFrom(argument);
        }

        @Override
        public Executor sameThreadExecutor() {
            return MoreExecutors.sameThreadExecutor();
        }
    }

    private static class Version19OrHigher extends GuavaCompatibility {

        @Override
        public <V> ListenableFuture<V> withFallback(ListenableFuture<? extends V> input,
                                                    AsyncFunction<Throwable, V> fallback) {
            return Futures.catchingAsync(input, Throwable.class, fallback);
        }

        @Override
        public <V> ListenableFuture<V> withFallback(ListenableFuture<? extends V> input,
                                                    AsyncFunction<Throwable, V> fallback, Executor executor) {
            return Futures.catchingAsync(input, Throwable.class, fallback, executor);
        }

        @Override
        public <I, O> ListenableFuture<O> transformAsync(ListenableFuture<I> input, AsyncFunction<? super I, ? extends O> function) {
            return Futures.transformAsync(input, function);
        }

        @Override
        public <I, O> ListenableFuture<O> transformAsync(ListenableFuture<I> input, AsyncFunction<? super I, ? extends O> function, Executor executor) {
            return Futures.transformAsync(input, function, executor);
        }

        @Override
        public boolean isSupertypeOf(TypeToken<?> target, TypeToken<?> argument) {
            return target.isSupertypeOf(argument);
        }

        @Override
        public Executor sameThreadExecutor() {
            return MoreExecutors.directExecutor();
        }
    }
}
