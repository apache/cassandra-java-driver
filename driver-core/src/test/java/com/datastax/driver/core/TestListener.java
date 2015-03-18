/*
 *      Copyright (C) 2012-2014 DataStax Inc.
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

import org.testng.*;
import org.testng.internal.ConstructorOrMethod;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.utils.CassandraVersion;

public class TestListener extends TestListenerAdapter implements IInvokedMethodListener {

    private long start_time = System.nanoTime();
    private int test_index = 0;

    static {
        System.out.println("[CCMBridge] Using Cassandra version: " + CCMBridge.CASSANDRA_VERSION);
    }

    @Override
    public void onTestFailure(ITestResult tr) {
        long elapsedTime = TimeUnit.NANOSECONDS.toSeconds((System.nanoTime() - start_time));
        long testTime = tr.getEndMillis() - tr.getStartMillis();

        tr.getThrowable().printStackTrace();
        System.out.println("FAILED: " + tr.getName());
        System.out.println("Test: " + formatIntoHHMMSS(testTime / 1000));
        System.out.println("Elapsed: " + formatIntoHHMMSS(elapsedTime));
        System.out.println();
    }

    @Override
    public void onTestSkipped(ITestResult tr) {
        long elapsedTime = TimeUnit.NANOSECONDS.toSeconds((System.nanoTime() - start_time));
        long testTime = tr.getEndMillis() - tr.getStartMillis();

        System.out.println("SKIPPED: " + tr.getName());
        System.out.println("Test: " + formatIntoHHMMSS(testTime / 1000));
        System.out.println("Elapsed: " + formatIntoHHMMSS(elapsedTime));
        System.out.println();
    }

    @Override
    public void onTestSuccess(ITestResult tr) {
        long elapsedTime = TimeUnit.NANOSECONDS.toSeconds((System.nanoTime() - start_time));
        long testTime = tr.getEndMillis() - tr.getStartMillis();

        System.out.println("SUCCESS: " + tr.getName());
        System.out.println("Test: " + formatIntoHHMMSS(testTime / 1000));
        System.out.println("Elapsed: " + formatIntoHHMMSS(elapsedTime));
        System.out.println("\n");
    }

    @Override
    public void onTestStart(ITestResult tr) {
        System.out.println("Starting " + tr.getTestClass().getName() + '.' + tr.getName() + " [Test #" + ++test_index + "]...");
    }

    static String formatIntoHHMMSS(long secondsTotal) {
        long hours = secondsTotal / 3600,
        remainder = secondsTotal % 3600,
        minutes = remainder / 60,
        seconds = remainder % 60;

        return ((hours < 10 ? "0" : "") + hours
        + ':' + (minutes < 10 ? "0" : "") + minutes
        + ':' + (seconds< 10 ? "0" : "") + seconds);
    }

    @Override public void beforeInvocation(IInvokedMethod testMethod, ITestResult testResult) {
        // Check to see if the class or method is annotated with 'CassandraVersion', if so ensure the
        // version we are testing with meets the requirement, if not a SkipException is thrown
        // and this test is skipped.
        ITestNGMethod testNgMethod = testResult.getMethod();
        ConstructorOrMethod constructorOrMethod = testNgMethod.getConstructorOrMethod();

        Class<?> clazz = testNgMethod.getInstance().getClass();
        if(clazz != null && clazz.isAnnotationPresent(CassandraVersion.class)) {
            CassandraVersion cassandraVersion = clazz.getAnnotation(CassandraVersion.class);
            TestUtils.versionCheck(cassandraVersion.major(), cassandraVersion.minor(), cassandraVersion.description());
        }

        Method method = constructorOrMethod.getMethod();
        if(method != null && method.isAnnotationPresent(CassandraVersion.class)) {
            CassandraVersion cassandraVersion = method.getAnnotation(CassandraVersion.class);
            TestUtils.versionCheck(cassandraVersion.major(), cassandraVersion.minor(), cassandraVersion.description());
        }
    }

    @Override public void afterInvocation(IInvokedMethod testMethod, ITestResult testResult) {
        // Do nothing
    }
}
