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
package com.datastax.driver.core.policies;

import com.datastax.driver.core.policies.RetryPolicy.RetryDecision;
import org.testng.annotations.Test;

import static com.datastax.driver.core.ConsistencyLevel.ONE;
import static com.datastax.driver.core.policies.RetryPolicy.RetryDecision.Type.*;
import static org.assertj.core.api.Assertions.assertThat;

public class RetryDecisionTest {

    @Test(groups = "unit")
    public void should_expose_decision_properties() throws Throwable {
        RetryDecision retryAtOne = RetryDecision.retry(ONE);
        assertThat(retryAtOne.getType())
                .isEqualTo(RETRY);
        assertThat(retryAtOne.getRetryConsistencyLevel())
                .isEqualTo(ONE);
        assertThat(retryAtOne.isRetryCurrent())
                .isTrue();
        assertThat(retryAtOne.toString())
                .isEqualTo("Retry at ONE on same host.");

        RetryDecision tryNextAtOne = RetryDecision.tryNextHost(ONE);
        assertThat(tryNextAtOne.getType())
                .isEqualTo(RETRY);
        assertThat(tryNextAtOne.getRetryConsistencyLevel())
                .isEqualTo(ONE);
        assertThat(tryNextAtOne.isRetryCurrent())
                .isFalse();
        assertThat(tryNextAtOne.toString())
                .isEqualTo("Retry at ONE on next host.");

        RetryDecision rethrow = RetryDecision.rethrow();
        assertThat(rethrow.getType())
                .isEqualTo(RETHROW);
        assertThat(rethrow.toString())
                .isEqualTo("Rethrow");

        RetryDecision ignore = RetryDecision.ignore();
        assertThat(ignore.getType())
                .isEqualTo(IGNORE);
        assertThat(ignore.toString())
                .isEqualTo("Ignore");
    }
}
