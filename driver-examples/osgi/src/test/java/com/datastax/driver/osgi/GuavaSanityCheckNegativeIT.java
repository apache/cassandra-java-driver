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
package com.datastax.driver.osgi;

import com.datastax.driver.core.Cluster;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.testng.listener.PaxExam;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import static com.datastax.driver.osgi.BundleOptions.*;
import static org.ops4j.pax.exam.CoreOptions.options;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Listeners({PaxExam.class})
@Test(groups = "short")
public class GuavaSanityCheckNegativeIT {

    @Configuration
    public Option[] guava14_0_1Config() {
        return options(
                driverBundle(),
                nettyBundles(),
                guavaBundle().version("14.0.1"),
                defaultOptions()
        );
    }

    @Configuration
    public Option[] guava15Config() {
        return options(
                driverBundle(),
                nettyBundles(),
                guavaBundle().version("15.0"),
                defaultOptions()
        );
    }

    @Configuration
    public Option[] guava16Config() {
        return options(
                driverBundle(),
                nettyBundles(),
                guavaBundle().version("16.0"),
                defaultOptions()
        );
    }

    /**
     * <p>
     * Validates that the driver is able to detect that the guava library in the classpath is
     * less than version 16.01 and throws an {@link IllegalStateException} in this case and
     * is raised as the cause of an {@link ExceptionInInitializerError} since the failure
     * occurs at static initialization.
     * </p>
     * <p/>
     * The following configurations are tried (defined via methods with the @Configuration annotation):
     * <ol>
     * <li>With Guava 14.0.1</li>
     * <li>With Guava 15.0</li>
     * <li>With Guava 16.0</li>
     * </ol>
     *
     * @test_category packaging
     * @expected_result An {@link IllegalStateException} is thrown.
     * @jira_ticket JAVA-961
     * @since 3.0.0-beta1
     */
    public void should_raise_guava_version_error_if_lt_16_0_1() throws Throwable {
        try {
            Cluster.builder();
            fail("Expected an IllegalStateException for Guava version incompatibility");
        } catch (ExceptionInInitializerError e) {
            try {
                throw e.getCause();
            } catch (IllegalStateException ise) {
                assertTrue(ise.getMessage().contains("Detected Guava issue #1635"));
            }
        }
    }
}
