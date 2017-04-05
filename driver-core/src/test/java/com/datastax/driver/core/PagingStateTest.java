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

import com.datastax.driver.core.exceptions.PagingStateException;
import com.datastax.driver.core.utils.CassandraVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.util.Iterator;

import static org.assertj.core.api.Assertions.assertThat;

@CassandraVersion("2.0.0")
public class PagingStateTest extends CCMTestsSupport {

    private static final Logger logger = LoggerFactory.getLogger(PagingStateTest.class);

    public static final String KEY = "paging_test";

    @Override
    public void onTestContextInitialized() {
        execute("CREATE TABLE test (k text, v int, PRIMARY KEY (k, v))");
        for (int i = 0; i < 100; i++) {
            execute(String.format("INSERT INTO test (k, v) VALUES ('%s', %d)", KEY, i));
        }
    }

    /**
     * Validates that {@link PagingState} can be reused with the same Statement.
     *
     * @test_category paging
     * @expected_result {@link ResultSet} from the query with the provided {@link PagingState} starts from the
     * subsequent row from the first query.
     */
    @Test(groups = "short")
    public void should_complete_when_using_paging_state() {
        SimpleStatement st = new SimpleStatement(String.format("SELECT v FROM test WHERE k='%s'", KEY));
        ResultSet result = session().execute(st.setFetchSize(20));
        int pageSize = result.getAvailableWithoutFetching();
        String savedPagingStateString = result.getExecutionInfo().getPagingState().toString();

        st = new SimpleStatement(String.format("SELECT v FROM test WHERE k='%s'", KEY));
        result = session().execute(st.setFetchSize(20).setPagingState(PagingState.fromString(savedPagingStateString)));

        //We have the result starting from the next page we stopped
        assertThat(result.one().getInt("v")).isEqualTo(pageSize);
    }

    /**
     * <p/>
     * Validates that if the {@link PagingState} is altered in any way that it may not be reused.
     * The paging state is altered in the following ways:
     * <p/>
     * <ol>
     * <li>Altering a byte in the paging state raw bytes.</li>
     * <li>Setting the {@link PagingState} on a different Statement. (should fail hash validation)</li>
     * </ol>
     *
     * @test_category paging
     * @expected_result {@link PagingState} refused to be reused if it is changed or used on a different statement.
     */
    @Test(groups = "short")
    public void should_fail_if_paging_state_altered() {
        boolean setWithFalseContent = false;
        boolean setWithWrongStatement = false;

        SimpleStatement st = new SimpleStatement(String.format("SELECT v FROM test WHERE k='%s'", KEY));
        ResultSet result = session().execute(st.setFetchSize(20));

        PagingState savedPagingState = result.getExecutionInfo().getPagingState();
        byte[] savedPagingStateBuffer = savedPagingState.toBytes();
        String savedPagingStateString = savedPagingState.toString();

        // corrupting the paging state
        savedPagingStateBuffer[6] = (byte) 42;

        try {
            st.setFetchSize(20).setPagingState(PagingState.fromBytes(savedPagingStateBuffer));
        } catch (PagingStateException e) {
            setWithFalseContent = true;
            logger.debug(e.getMessage());
        } finally {
            assertThat(setWithFalseContent).isTrue();
            assertThat(st.getPagingState()).isNull();
        }

        // Changing the statement
        st = new SimpleStatement(String.format("SELECT v FROM test WHERE k='%s'", "paging"));
        try {
            st.setFetchSize(20).setPagingState(PagingState.fromString(savedPagingStateString));
        } catch (PagingStateException e) {
            setWithWrongStatement = true;
            logger.debug(e.getMessage());
        } finally {
            assertThat(setWithWrongStatement).isTrue();
            assertThat(st.getPagingState()).isNull();
        }
    }

    /**
     * Validates that {@link PagingState} can be reused with a wrapped Statement.
     *
     * @test_category paging
     * @expected_result {@link ResultSet} from the query with the provided {@link PagingState} starts from the
     * subsequent row from the first query.
     */
    @Test(groups = "short")
    public void should_use_state_with_wrapped_statement() {
        Statement st = new TestWrapper(new SimpleStatement(String.format("SELECT v FROM test WHERE k='%s'", KEY)));
        ResultSet result = session().execute(st.setFetchSize(20));
        int pageSize = result.getAvailableWithoutFetching();
        String savedPagingStateString = result.getExecutionInfo().getPagingState().toString();

        st = new TestWrapper(new SimpleStatement(String.format("SELECT v FROM test WHERE k='%s'", KEY)));
        result = session().execute(st.setFetchSize(20).setPagingState(PagingState.fromString(savedPagingStateString)));

        //We have the result starting from the next page we stopped
        assertThat(result.one().getInt("v")).isEqualTo(pageSize);
    }

    /**
     * Validates that {@link PagingState} can be reused with the same {@link BoundStatement}.
     *
     * @test_category paging
     * @expected_result {@link ResultSet} from the query with the provided paging state starts from the subsequent row
     * from the first query.
     */
    @Test(groups = "short")
    @CassandraVersion("2.0.0")
    public void should_be_able_to_use_state_with_bound_statement() {
        PreparedStatement prepared = session().prepare("SELECT v from test where k=?");
        BoundStatement bs = prepared.bind(KEY);

        ResultSet result = session().execute(bs.setFetchSize(20));
        int pageSize = result.getAvailableWithoutFetching();
        PagingState pagingState = result.getExecutionInfo().getPagingState();

        result = session().execute(bs.setFetchSize(20).setPagingState(pagingState));

        //We have the result starting from the next page we stopped
        assertThat(result.one().getInt("v")).isEqualTo(pageSize);
    }

    /**
     * Validates that {@link PagingState} cannot be reused with a different {@link BoundStatement} than the original,
     * even if its source {@link PreparedStatement} was the same.
     *
     * @test_category paging
     * @expected_result A failure is thrown when setting paging state on a different {@link BoundStatement}.
     */
    @Test(groups = "short", expectedExceptions = {PagingStateException.class})
    @CassandraVersion("2.0.0")
    public void should_not_be_able_to_use_state_with_different_bound_statement() {
        PreparedStatement prepared = session().prepare("SELECT v from test where k=?");
        BoundStatement bs0 = prepared.bind(KEY);

        ResultSet result = session().execute(bs0.setFetchSize(20));
        PagingState pagingState = result.getExecutionInfo().getPagingState();

        BoundStatement bs1 = prepared.bind("different_key");
        session().execute(bs1.setFetchSize(20).setPagingState(pagingState));
    }

    /**
     * Validates if all results of a query are paged in through a queries result set that the {@link PagingState} it
     * returns will return an empty set when queried with.
     *
     * @test_category paging
     * @expected_result Query with the {@link PagingState} returns 0 rows.
     */
    @Test(groups = "short")
    public void should_return_no_rows_when_paged_to_end() {
        SimpleStatement st = new SimpleStatement(String.format("SELECT v FROM test WHERE k='%s'", KEY));
        ResultSet result = session().execute(st.setFetchSize(20));

        // Consume enough of the iterator to cause all the results to be paged in.
        Iterator<Row> rowIt = result.iterator();
        for (int i = 0; i < 83; i++) {
            rowIt.next().getInt("v");
        }

        String savedPagingStateString = result.getExecutionInfo().getPagingState().toString();

        st = new SimpleStatement(String.format("SELECT v FROM test WHERE k='%s'", KEY));
        result = session().execute(st.setFetchSize(20).setPagingState(PagingState.fromString(savedPagingStateString)));

        assertThat(result.one()).isNull();
    }

    @Test(groups = "unit", expectedExceptions = {PagingStateException.class})
    public void should_fail_when_given_invalid_string() {
        // An invalid string cannot be parsed and a PagingStateException is thrown.
        PagingState.fromString("0101");
    }

    @Test(groups = "unit", expectedExceptions = {PagingStateException.class})
    public void should_fail_when_given_invalid_byte_array() {
        // Given an expected page state of size 1 and hash of size 1, we should expect 6 bytes, but only receive 5.
        byte[] complete = {0x00, 0x01, 0x00, 0x01, 0x00};
        PagingState.fromBytes(complete);
    }

    @Test(groups = "unit", expectedExceptions = {UnsupportedOperationException.class})
    public void should_fail_when_setting_paging_state_on_batch_statement() {
        // Should not be able to set paging state on a batch statement.
        PagingState emptyStatement = PagingState.fromString("00000000");

        BatchStatement batch = new BatchStatement();
        batch.setPagingState(emptyStatement);
    }

    /**
     * Validates that the "unsafe" paging state can be reused with the same Statement.
     *
     * @test_category paging
     * @expected_result {@link ResultSet} from the query with the provided raw paging state starts from the
     * subsequent row from the first query.
     */
    @Test(groups = "short")
    public void should_complete_when_using_unsafe_paging_state() {
        SimpleStatement st = new SimpleStatement(String.format("SELECT v FROM test WHERE k='%s'", KEY));
        ResultSet result = session().execute(st.setFetchSize(20));
        int pageSize = result.getAvailableWithoutFetching();
        byte[] savedPagingState = result.getExecutionInfo().getPagingStateUnsafe();

        st = new SimpleStatement(String.format("SELECT v FROM test WHERE k='%s'", KEY));
        result = session().execute(st.setFetchSize(20).setPagingStateUnsafe(savedPagingState));

        //We have the result starting from the next page we stopped
        assertThat(result.one().getInt("v")).isEqualTo(pageSize);
    }

    static class TestWrapper extends StatementWrapper {
        TestWrapper(Statement wrapped) {
            super(wrapped);
        }
    }
}

