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

import com.datastax.driver.core.utils.CassandraVersion;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

import static org.assertj.core.api.Assertions.assertThat;

@CassandraVersion(value = "2.0.0", description = "uses paging")
public class AsyncResultSetTest extends CCMTestsSupport {

    @Override
    public void onTestContextInitialized() {
        execute(
                "create table ints (i int primary key)");
    }

    @BeforeMethod(groups = "short")
    public void cleanup() {
        session().execute("truncate ints");
    }

    @Test(groups = "short")
    public void should_iterate_single_page_result_set_asynchronously() {
        should_iterate_result_set_asynchronously(100, 500);
    }

    @Test(groups = "short")
    public void should_iterate_multi_page_result_set_asynchronously() {
        should_iterate_result_set_asynchronously(1000, 20);
    }

    private void should_iterate_result_set_asynchronously(int totalCount, int fetchSize) {
        for (int i = 0; i < totalCount; i++)
            session().execute(String.format("insert into ints (i) values (%d)", i));

        Statement statement = new SimpleStatement("select * from ints").setFetchSize(fetchSize);
        ResultsAccumulator results = new ResultsAccumulator();

        ListenableFuture<ResultSet> future = GuavaCompatibility.INSTANCE.transformAsync(
                session().executeAsync(statement),
                results);

        Futures.getUnchecked(future);

        assertThat(results.all.size()).isEqualTo(totalCount);
    }

    /**
     * Dummy transformation that accumulates all traversed results
     */
    static class ResultsAccumulator implements AsyncFunction<ResultSet, ResultSet> {
        final Set<Integer> all = new ConcurrentSkipListSet<Integer>();

        @Override
        public ListenableFuture<ResultSet> apply(ResultSet rs) throws Exception {
            int remainingInPage = rs.getAvailableWithoutFetching();
            for (Row row : rs) {
                all.add(row.getInt(0));
                if (--remainingInPage == 0)
                    break;
            }
            boolean wasLastPage = rs.getExecutionInfo().getPagingState() == null;
            if (wasLastPage)
                return Futures.immediateFuture(rs);
            else
                return GuavaCompatibility.INSTANCE.transformAsync(rs.fetchMoreResults(), this);
        }
    }
}
