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

import com.datastax.driver.core.querybuilder.Batch;
import org.testng.annotations.Test;

import static org.testng.Assert.*;
import static org.testng.Assert.assertNull;

public abstract class AbstractIdempotencyTest {
    
    protected abstract TestBatch createBatch();

    /**
     * Unify Batch and BatchStatement to avoid duplicating all tests
     */
    protected interface TestBatch {
        //Batch only accepts RegularStatement, so we use it for common interface
        void add(RegularStatement statement);
        Boolean isIdempotent();
        void setIdempotent(boolean idempotent);
    }
    
    @Test(groups = "unit")
    public void isIdempotent_should_return_true_if_no_statements_added() {
        TestBatch batch = createBatch();
        assertTrue(batch.isIdempotent());
    }

    @Test(groups = "unit")
    public void isIdempotent_should_return_true_if_all_statements_are_idempotent() {
        TestBatch batch = createBatch();
        assertTrue(batch.isIdempotent());

        batch.add(statementWithIdempotency(true));
        assertTrue(batch.isIdempotent());

        batch.add(statementWithIdempotency(true));
        assertTrue(batch.isIdempotent());
    }

    @Test(groups = "unit")
    public void isIdempotent_should_return_false_if_any_statements_is_nonidempotent() {
        TestBatch batch = createBatch();
        assertTrue(batch.isIdempotent());

        batch.add(statementWithIdempotency(true));
        assertTrue(batch.isIdempotent());

        batch.add(statementWithIdempotency(false));
        assertFalse(batch.isIdempotent());

        batch.add(statementWithIdempotency(true));
        assertFalse(batch.isIdempotent());
    }

    @Test(groups = "unit")
    public void isIdempotent_should_return_null_if_no_nonidempotent_statements_and_some_are_nullidempotent() {
        TestBatch batch = createBatch();
        assertTrue(batch.isIdempotent());

        batch.add(statementWithIdempotency(true));
        assertTrue(batch.isIdempotent());

        batch.add(statementWithIdempotency(null));
        assertNull(batch.isIdempotent());

        batch.add(statementWithIdempotency(true));
        assertNull(batch.isIdempotent());
    }

    @Test(groups = "unit")
    public void isIdempotent_should_return_false_if_both_nonidempotent_and_nullidempotent_statements_present() {
        TestBatch batch = createBatch();
        assertTrue(batch.isIdempotent());

        batch.add(statementWithIdempotency(true));
        assertTrue(batch.isIdempotent());

        batch.add(statementWithIdempotency(null));
        assertNull(batch.isIdempotent());

        batch.add(statementWithIdempotency(false));
        assertFalse(batch.isIdempotent());

        batch.add(statementWithIdempotency(true));
        assertFalse(batch.isIdempotent());

        batch.add(statementWithIdempotency(null));
        assertFalse(batch.isIdempotent());

        batch.add(statementWithIdempotency(false));
        assertFalse(batch.isIdempotent());
    }

    @Test(groups = "unit")
    public void isIdempotent_should_return_override_flag_if_no_statements_added() {
        TestBatch batch = createBatch();
        assertTrue(batch.isIdempotent());

        batch.setIdempotent(false);
        assertFalse(batch.isIdempotent());
    }

    @Test(groups = "unit")
    public void isIdempotent_should_return_override_flag_if_calculated_idempotency_true() {
        TestBatch batch = createBatch();
        assertTrue(batch.isIdempotent());

        batch.add(statementWithIdempotency(true));
        assertTrue(batch.isIdempotent());

        batch.setIdempotent(false);
        assertFalse(batch.isIdempotent());
    }

    @Test(groups = "unit")
    public void isIdempotent_should_return_override_flag_if_calculated_idempotency_null() {
        TestBatch batch = createBatch();
        assertTrue(batch.isIdempotent());

        batch.add(statementWithIdempotency(null));
        assertNull(batch.isIdempotent());

        batch.setIdempotent(false);
        assertFalse(batch.isIdempotent());
    }

    @Test(groups = "unit")
    public void isIdempotent_should_return_override_flag_if_calculated_idempotency_false() {
        TestBatch batch = createBatch();
        assertTrue(batch.isIdempotent());

        batch.add(statementWithIdempotency(false));
        assertFalse(batch.isIdempotent());

        batch.setIdempotent(true);
        assertTrue(batch.isIdempotent());
    }

    @Test(groups = "unit")
    public void isIdempotent_should_return_override_flag_if_calculated_idempotency_equals_override_value() {
        TestBatch batch = createBatch();
        assertTrue(batch.isIdempotent());

        batch.add(statementWithIdempotency(false));
        assertFalse(batch.isIdempotent());

        batch.setIdempotent(false);
        assertFalse(batch.isIdempotent());
    }

    private RegularStatement statementWithIdempotency(Boolean idempotency) {
        RegularStatement statement = new SimpleStatement("fake statement");
        if (idempotency != null) {
            statement.setIdempotent(idempotency);
            assertEquals(statement.isIdempotent(), idempotency);
        } else {
            assertNull(statement.isIdempotent());
        }
        return statement;
    }
}
