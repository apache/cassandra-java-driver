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

import org.testng.annotations.Test;

import static com.datastax.driver.core.CCMAccess.Workload.*;

/**
 * A simple test to validate DSE setups.
 * <p/>
 * <h3>Running all tests against DSE</h3>
 * <p/>
 * To run tests globally against DSE, set the system property {@code dse}
 * to {@code true}.
 * <p/>
 * When this flag is provided, it is assumed a DSE version is passed under
 * the system property {@code cassandra.version}.
 * A mapping for determining C* version from DSE version is described in {@link CCMBridge}.
 * <p/>
 * Example usages:
 * <p/>
 * DSE 4.8.3:
 * <pre>
 * -Ddse -Dcassandra.version=4.8.3
 * -Ddse=true -Dcassandra.version=4.8.3
 * </pre>
 * <p/>
 * Custom local install of DSE 5.0 (using {@code cassandra.directory} instead of {@code cassandra.version}):
 * <pre>
 * -Dcassandra.version=5.0 -Ddse -Dcassandra.directory=/path/to/dse
 * </pre>
 * <p/>
 * <h3>Running a specific test against DSE</h3>
 * <p/>
 * Set the following properties on the test:
 * <pre>{@code @CCMConfig(dse = true, version = "4.8.3")}</pre>
 *
 * <h3>Supplying DSE credentials</h3>
 *
 * Rather than adding system properties for DSE credentials,
 * DSE tests rely on a recent change in CCM to support providing
 * credentials via {@code $HOME/.ccm/.dse.ini}.
 *
 * The contents of this file need to be formed in this way:
 * <pre>
 * [dse_credentials]
 * dse_username = myusername
 * dse_password = mypassword
 * </pre>
 * <p/>
 * <h3>Other requirements</h3>
 * <p/>
 * DSE requires your {@code PATH} variable to provide access
 * to super-user executables in {@code /usr/sbin}.
 * <p/>
 * A correct example is as follows: {@code /usr/bin:/usr/local/bin:/bin:/usr/sbin:$JAVA_HOME/bin:$PATH}.
 */
@Test(enabled = false)
@CCMConfig(
        dse = true,
        numberOfNodes = 3,
        version = "4.8.3",
        workloads = {
                @CCMWorkload(solr),
                @CCMWorkload({spark, solr}),
                @CCMWorkload({cassandra, spark})
        }
)
public class DseCCMClusterTest extends CCMTestsSupport {

    @Test(groups = "short")
    public void should_conenct_to_dse() throws InterruptedException {

    }

}
