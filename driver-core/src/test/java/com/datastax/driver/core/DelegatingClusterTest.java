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

import com.datastax.driver.core.DelegatingClusterIntegrationTest.SimpleDelegatingCluster;
import com.google.common.collect.ImmutableSet;
import org.mockito.Mockito;
import org.mockito.exceptions.verification.WantedButNotInvoked;
import org.mockito.invocation.Invocation;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;

public class DelegatingClusterTest {
    private static final Set<String> NON_DELEGATED_METHODS = ImmutableSet.of("getClusterName");

    /**
     * Checks that all methods of {@link DelegatingCluster} invoke their counterpart in {@link Cluster}.
     * This protects us from forgetting to add a method to the former when it gets added to the latter.
     * <p>
     * Note that a much better, compile-time solution would be to make {@link Cluster} an interface, but that's a
     * breaking change so it will have to wait until the next major version.
     */
    @Test(groups = "unit")
    public void should_call_delegate_methods() throws Exception {
        Cluster delegate = mock(Cluster.class);
        SimpleDelegatingCluster delegatingCluster = new SimpleDelegatingCluster(delegate);

        for (Method method : Cluster.class.getMethods()) {
            if ((method.getModifiers() & Modifier.STATIC) == Modifier.STATIC ||
                    NON_DELEGATED_METHODS.contains(method.getName()) ||
                    method.getDeclaringClass() == Object.class) {
                continue;
            }
            // we can leave all parameters to null since we're invoking a mock
            Object[] parameters = new Object[method.getParameterTypes().length];
            try {
                method.invoke(delegatingCluster, parameters);
            } catch (Exception ignored) {
            }
            verify(delegate, method, parameters);
            reset(delegate);
        }
    }

    private static void verify(Object mock, Method expectedMethod, Object... expectedArguments) {
        out:
        for (Invocation invocation : Mockito.mockingDetails(mock).getInvocations()) {
            if (invocation.getMethod().equals(expectedMethod)) {
                Object[] actualArguments = invocation.getArguments();
                assert actualArguments.length == expectedArguments.length; // because it's the same method
                for (int i = 0; i < actualArguments.length; i++) {
                    Object actual = actualArguments[i];
                    Object expected = expectedArguments[i];
                    boolean equal = (actual == null) ? expected == null : actual.equals(expected);
                    if (!equal) {
                        continue out;
                    }
                }
                invocation.markVerified();
                return;
            }
        }
        throw new WantedButNotInvoked("Not delegated: " + expectedMethod.toString());
    }
}