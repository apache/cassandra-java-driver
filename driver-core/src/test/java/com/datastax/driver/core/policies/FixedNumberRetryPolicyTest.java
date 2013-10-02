package com.datastax.driver.core.policies;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.WriteType;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import static org.testng.Assert.*;

/**
 */
public class FixedNumberRetryPolicyTest {
    public static final int NUMBER_OF_RETRIES = 5;

    private FixedNumberRetryPolicy underTest = new FixedNumberRetryPolicy(NUMBER_OF_RETRIES);

    @Test(groups = "unit")
    public void shouldReadRetryIfNumberOfRetriesLessThanConfiguredRetries() {
        ConsistencyLevel originalConsistency = ConsistencyLevel.ALL;
        int numberOfRetries = NUMBER_OF_RETRIES - 1;

        RetryPolicy.RetryDecision decision = underTest.onReadTimeout(null, originalConsistency, 1, 1, false, numberOfRetries);

        assertEquals(decision.getType(), RetryPolicy.RetryDecision.Type.RETRY, "Should retry");
    }

    @Test(groups = "unit")
    public void shouldReadRetryAtTheSameConsistencyLevel() {
        ConsistencyLevel firstConsistency = ConsistencyLevel.ALL;
        ConsistencyLevel secondConsistency = ConsistencyLevel.ONE;
        int numberOfRetries = NUMBER_OF_RETRIES - 1;

        RetryPolicy.RetryDecision firstDecision = underTest.onReadTimeout(null, firstConsistency, 1, 1, false, numberOfRetries);
        assertEquals(firstDecision.getRetryConsistencyLevel(), firstConsistency, "Should retry at the same consistency level");

        RetryPolicy.RetryDecision secondDecision = underTest.onReadTimeout(null, secondConsistency, 1, 1, false, numberOfRetries);
        assertEquals(secondDecision.getRetryConsistencyLevel(), secondConsistency, "Should retry at the same consistency level");
    }

    @Test(groups = "unit")
    public void shouldNotReadRetryIfNumberOfRetriesGreaterThanConfiguredRetries() {
        int numberOfRetries = NUMBER_OF_RETRIES + 1;

        RetryPolicy.RetryDecision decision = underTest.onReadTimeout(null, ConsistencyLevel.ONE, 1, 1, false, numberOfRetries);

        assertEquals(decision.getType(), RetryPolicy.RetryDecision.Type.RETHROW, "Should rethrow if retried more than the configured amount");
    }

    @Test(groups = "unit")
    public void shouldNotReadRetryIfNumberOfRetriesEqualToConfiguredRetries() {
        int numberOfRetries = NUMBER_OF_RETRIES;

        RetryPolicy.RetryDecision decision = underTest.onReadTimeout(null, ConsistencyLevel.ONE, 1, 1, false, numberOfRetries);

        assertEquals(decision.getType(), RetryPolicy.RetryDecision.Type.RETHROW, "Should rethrow if already retried configured amount");
    }


    @Test(groups = "unit")
    public void shouldWriteRetryIfNumberOfRetriesLessThanConfiguredRetries() {
        ConsistencyLevel originalConsistency = ConsistencyLevel.ALL;
        int numberOfRetries = NUMBER_OF_RETRIES - 1;

        RetryPolicy.RetryDecision decision = underTest.onWriteTimeout(null, originalConsistency, WriteType.SIMPLE, 1, 0, numberOfRetries);

        assertEquals(decision.getType(), RetryPolicy.RetryDecision.Type.RETRY, "Should retry");
    }

    @Test(groups = "unit")
    public void shouldWriteRetryAtTheSameConsistencyLevel() {
        ConsistencyLevel firstConsistency = ConsistencyLevel.ALL;
        ConsistencyLevel secondConsistency = ConsistencyLevel.ONE;
        int numberOfRetries = NUMBER_OF_RETRIES - 1;

        RetryPolicy.RetryDecision firstDecision = underTest.onWriteTimeout(null, firstConsistency, WriteType.SIMPLE, 1, 0, numberOfRetries);
        assertEquals(firstDecision.getRetryConsistencyLevel(), firstConsistency, "Should retry at the same consistency level");

        RetryPolicy.RetryDecision secondDecision = underTest.onWriteTimeout(null, secondConsistency, WriteType.SIMPLE, 1, 0, numberOfRetries);
        assertEquals(secondDecision.getRetryConsistencyLevel(), secondConsistency, "Should retry at the same consistency level");
    }

    @Test(groups = "unit")
    public void shouldWNotWriteRetryIfNumberOfRetriesGreaterThanConfiguredRetries() {
        ConsistencyLevel originalConsistency = ConsistencyLevel.ALL;
        int numberOfRetries = NUMBER_OF_RETRIES + 1;

        RetryPolicy.RetryDecision decision = underTest.onWriteTimeout(null, originalConsistency, WriteType.SIMPLE, 1, 0, numberOfRetries);

        assertEquals(decision.getType(), RetryPolicy.RetryDecision.Type.RETHROW, "Should retry");
    }

    @Test(groups = "unit")
    public void shouldWNotWriteRetryIfNumberOfRetriesEqualToConfiguredRetries() {
        ConsistencyLevel originalConsistency = ConsistencyLevel.ALL;
        int numberOfRetries = NUMBER_OF_RETRIES;

        RetryPolicy.RetryDecision decision = underTest.onWriteTimeout(null, originalConsistency, WriteType.SIMPLE, 1, 0, numberOfRetries);

        assertEquals(decision.getType(), RetryPolicy.RetryDecision.Type.RETHROW, "Should retry");
    }

    @Test(groups = "unit")
    public void shouldUnavailableRetryIfNumberOfRetriesLessThanConfiguredRetries() {
        ConsistencyLevel originalConsistency = ConsistencyLevel.ALL;
        int numberOfRetries = NUMBER_OF_RETRIES - 1;

        RetryPolicy.RetryDecision decision = underTest.onUnavailable(null, originalConsistency, 1, 1, numberOfRetries);

        assertEquals(decision.getType(), RetryPolicy.RetryDecision.Type.RETRY, "Should retry");
    }

    @Test(groups = "unit")
    public void shouldUnavailableRetryAtTheSameConsistencyLevel() {
        ConsistencyLevel firstConsistency = ConsistencyLevel.ALL;
        ConsistencyLevel secondConsistency = ConsistencyLevel.ONE;
        int numberOfRetries = NUMBER_OF_RETRIES - 1;

        RetryPolicy.RetryDecision firstDecision = underTest.onUnavailable(null, firstConsistency, 1, 1, numberOfRetries);
        assertEquals(firstDecision.getRetryConsistencyLevel(), firstConsistency, "Should retry at the same consistency level");

        RetryPolicy.RetryDecision secondDecision = underTest.onUnavailable(null, secondConsistency, 1, 1, numberOfRetries);
        assertEquals(secondDecision.getRetryConsistencyLevel(), secondConsistency, "Should retry at the same consistency level");
    }

    @Test(groups = "unit")
    public void shouldNotUnavailableRetryIfNumberOfRetriesGreaterThanConfiguredRetries() {
        int numberOfRetries = NUMBER_OF_RETRIES + 1;

        RetryPolicy.RetryDecision decision = underTest.onUnavailable(null, ConsistencyLevel.ONE, 1, 1, numberOfRetries);

        assertEquals(decision.getType(), RetryPolicy.RetryDecision.Type.RETHROW, "Should rethrow if retried more than the configured amount");
    }

    @Test(groups = "unit")
    public void shouldNotUnavailableRetryIfNumberOfRetriesEqualToConfiguredRetries() {
        int numberOfRetries = NUMBER_OF_RETRIES;

        RetryPolicy.RetryDecision decision = underTest.onUnavailable(null, ConsistencyLevel.ONE, 1, 1, numberOfRetries);

        assertEquals(decision.getType(), RetryPolicy.RetryDecision.Type.RETHROW, "Should rethrow if already retried configured amount");
    }


}
