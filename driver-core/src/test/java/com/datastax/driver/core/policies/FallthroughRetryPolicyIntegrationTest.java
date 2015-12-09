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
package com.datastax.driver.core.policies;

import com.datastax.driver.core.exceptions.ReadTimeoutException;
import com.datastax.driver.core.exceptions.UnavailableException;
import com.datastax.driver.core.exceptions.WriteTimeoutException;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.scassandra.http.client.PrimingRequest.Result.*;

public class FallthroughRetryPolicyIntegrationTest extends AbstractRetryPolicyIntegrationTest {
    public FallthroughRetryPolicyIntegrationTest() {
        super(FallthroughRetryPolicy.INSTANCE);
    }

    @Test(groups = "short")
    public void should_rethrow_on_read_timeout_with_0_receivedResponses() {
        simulateError(1, read_request_timeout);

        try {
            query();
            fail("expected a ReadTimeoutException");
        } catch (ReadTimeoutException e) {/*expected*/ }

        assertOnReadTimeoutWasCalled(1);
        assertThat(errors.getRetriesOnReadTimeout().getCount()).isEqualTo(0);
        assertQueried(1, 1);
        assertQueried(2, 0);
        assertQueried(3, 0);
    }

    @Test(groups = "short")
    public void should_rethrow_on_write_timeout_with_SIMPLE_write_type() {
        simulateError(1, write_request_timeout);

        try {
            query();
            fail("expected a WriteTimeoutException");
        } catch (WriteTimeoutException e) {/*expected*/}

        assertOnWriteTimeoutWasCalled(1);
        assertThat(errors.getRetriesOnWriteTimeout().getCount()).isEqualTo(0);
        assertQueried(1, 1);
        assertQueried(2, 0);
        assertQueried(3, 0);
    }

    @Test(groups = "short")
    public void should_rethrow_on_unavailable() {
        simulateError(1, unavailable);

        try {
            query();
            fail("expected an UnavailableException");
        } catch (UnavailableException e) {/*expected*/}

        assertOnUnavailableWasCalled(1);
        assertThat(errors.getRetriesOnUnavailable().getCount()).isEqualTo(0);
        assertQueried(1, 1);
        assertQueried(2, 0);
        assertQueried(3, 0);
    }
}
