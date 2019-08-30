/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.api.core.graph.statement;

import static com.datastax.dse.driver.api.core.config.DseDriverOption.CONTINUOUS_PAGING_MAX_ENQUEUED_PAGES;
import static com.datastax.dse.driver.api.core.config.DseDriverOption.CONTINUOUS_PAGING_PAGE_SIZE;
import static com.datastax.dse.driver.api.core.cql.continuous.ContinuousPagingITBase.Options;
import static com.datastax.dse.driver.internal.core.graph.GraphProtocol.GRAPH_BINARY_1_0;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.dse.driver.api.core.cql.continuous.ContinuousPagingITBase;
import com.datastax.dse.driver.api.core.graph.AsyncGraphResultSet;
import com.datastax.dse.driver.api.core.graph.GraphExecutionInfo;
import com.datastax.dse.driver.api.core.graph.GraphNode;
import com.datastax.dse.driver.api.core.graph.GraphResultSet;
import com.datastax.dse.driver.api.core.graph.GraphStatement;
import com.datastax.dse.driver.api.core.graph.PagingEnabledOptions;
import com.datastax.dse.driver.api.core.graph.ScriptGraphStatement;
import com.datastax.dse.driver.api.testinfra.session.DseSessionRule;
import com.datastax.dse.driver.api.testinfra.session.DseSessionRuleBuilder;
import com.datastax.dse.driver.internal.core.graph.MultiPageGraphResultSet;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.testinfra.DseRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.internal.core.util.CountingIterator;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;

@DseRequirement(min = "6.8.0", description = "Graph paging requires DSE 6.8+")
@RunWith(DataProviderRunner.class)
public class GraphPagingIT {

  private static CustomCcmRule ccmRule = CustomCcmRule.builder().withDseWorkloads("graph").build();

  private static DseSessionRule sessionRule =
      new DseSessionRuleBuilder(ccmRule)
          .withCreateGraph()
          .withCoreEngine()
          .withGraphProtocol(GRAPH_BINARY_1_0.toInternalCode())
          .build();

  @ClassRule public static TestRule chain = RuleChain.outerRule(ccmRule).around(sessionRule);

  @BeforeClass
  public static void setupSchema() {
    sessionRule
        .session()
        .execute(
            ScriptGraphStatement.newInstance(
                    "schema.vertexLabel('person')"
                        + ".ifNotExists()" // required otherwise we get a weird table already exists
                        // error
                        + ".partitionBy('pk', Int)"
                        + ".clusterBy('cc', Int)"
                        + ".property('name', Text)"
                        + ".create();")
                .setGraphName(sessionRule.getGraphName()));
    for (int i = 1; i <= 100; i++) {
      sessionRule
          .session()
          .execute(
              ScriptGraphStatement.newInstance(
                      String.format(
                          "g.addV('person').property('pk',0).property('cc',%d).property('name', '%s');",
                          i, "user" + i))
                  .setGraphName(sessionRule.getGraphName()));
    }
  }

  @UseDataProvider(location = ContinuousPagingITBase.class, value = "pagingOptions")
  @Test
  public void synchronous_paging_with_options(Options options) {
    // given
    DriverExecutionProfile profile = enablePaging(options, PagingEnabledOptions.ENABLED);

    if (options.sizeInBytes) {
      // Page sizes in bytes are not supported with graph queries
      return;
    }

    // when
    GraphResultSet result =
        sessionRule
            .session()
            .execute(
                ScriptGraphStatement.newInstance("g.V().hasLabel('person').values('name')")
                    .setGraphName(sessionRule.getGraphName())
                    .setTraversalSource("g")
                    .setExecutionProfile(profile));

    // then
    List<GraphNode> nodes = result.all();

    assertThat(((CountingIterator) result.iterator()).remaining()).isZero();
    assertThat(nodes).hasSize(options.expectedRows);
    for (int i = 1; i <= nodes.size(); i++) {
      GraphNode node = nodes.get(i - 1);
      assertThat(node.asString()).isEqualTo("user" + i);
    }
    assertThat(result.getExecutionInfo()).isNotNull();
    assertThat(result.getExecutionInfo().getCoordinator().getEndPoint().resolve())
        .isEqualTo(firstCcmNode());
    assertIfMultiPage(result, options.expectedPages);
  }

  @UseDataProvider(location = ContinuousPagingITBase.class, value = "pagingOptions")
  @Test
  public void synchronous_paging_with_options_when_auto(Options options) {
    // given
    DriverExecutionProfile profile = enablePaging(options, PagingEnabledOptions.AUTO);

    if (options.sizeInBytes) {
      // Page sizes in bytes are not supported with graph queries
      return;
    }

    // when
    GraphResultSet result =
        sessionRule
            .session()
            .execute(
                ScriptGraphStatement.newInstance("g.V().hasLabel('person').values('name')")
                    .setGraphName(sessionRule.getGraphName())
                    .setTraversalSource("g")
                    .setExecutionProfile(profile));

    // then
    List<GraphNode> nodes = result.all();

    assertThat(((CountingIterator) result.iterator()).remaining()).isZero();
    assertThat(nodes).hasSize(options.expectedRows);
    for (int i = 1; i <= nodes.size(); i++) {
      GraphNode node = nodes.get(i - 1);
      assertThat(node.asString()).isEqualTo("user" + i);
    }
    assertThat(result.getExecutionInfo()).isNotNull();
    assertThat(result.getExecutionInfo().getCoordinator().getEndPoint().resolve())
        .isEqualTo(firstCcmNode());

    assertIfMultiPage(result, options.expectedPages);
  }

  private void assertIfMultiPage(GraphResultSet result, int expectedPages) {
    if (result instanceof MultiPageGraphResultSet) {
      assertThat(((MultiPageGraphResultSet) result).getExecutionInfos()).hasSize(expectedPages);
      assertThat(result.getExecutionInfo())
          .isSameAs(((MultiPageGraphResultSet) result).getExecutionInfos().get(expectedPages - 1));
    }
  }

  @UseDataProvider(location = ContinuousPagingITBase.class, value = "pagingOptions")
  @Test
  public void synchronous_options_with_paging_disabled_should_fallback_to_single_page(
      Options options) {
    // given
    DriverExecutionProfile profile = enablePaging(options, PagingEnabledOptions.DISABLED);

    if (options.sizeInBytes) {
      // Page sizes in bytes are not supported with graph queries
      return;
    }

    // when
    GraphResultSet result =
        sessionRule
            .session()
            .execute(
                ScriptGraphStatement.newInstance("g.V().hasLabel('person').values('name')")
                    .setGraphName(sessionRule.getGraphName())
                    .setTraversalSource("g")
                    .setExecutionProfile(profile));

    // then
    List<GraphNode> nodes = result.all();

    assertThat(((CountingIterator) result.iterator()).remaining()).isZero();
    assertThat(nodes).hasSize(100);
    for (int i = 1; i <= nodes.size(); i++) {
      GraphNode node = nodes.get(i - 1);
      assertThat(node.asString()).isEqualTo("user" + i);
    }
    assertThat(result.getExecutionInfo()).isNotNull();
    assertThat(result.getExecutionInfo().getCoordinator().getEndPoint().resolve())
        .isEqualTo(firstCcmNode());
  }

  @UseDataProvider(location = ContinuousPagingITBase.class, value = "pagingOptions")
  @Test
  public void asynchronous_paging_with_options(Options options)
      throws ExecutionException, InterruptedException {
    // given
    DriverExecutionProfile profile = enablePaging(options, PagingEnabledOptions.ENABLED);

    if (options.sizeInBytes) {
      // Page sizes in bytes are not supported with graph queries
      return;
    }

    // when
    CompletionStage<AsyncGraphResultSet> result =
        sessionRule
            .session()
            .executeAsync(
                ScriptGraphStatement.newInstance("g.V().hasLabel('person').values('name')")
                    .setGraphName(sessionRule.getGraphName())
                    .setTraversalSource("g")
                    .setExecutionProfile(profile));

    // then
    checkAsyncResult(result, options, 0, 1, new ArrayList<>());
  }

  @UseDataProvider(location = ContinuousPagingITBase.class, value = "pagingOptions")
  @Test
  public void asynchronous_paging_with_options_when_auto(Options options)
      throws ExecutionException, InterruptedException {
    // given
    DriverExecutionProfile profile = enablePaging(options, PagingEnabledOptions.AUTO);

    if (options.sizeInBytes) {
      // Page sizes in bytes are not supported with graph queries
      return;
    }

    // when
    CompletionStage<AsyncGraphResultSet> result =
        sessionRule
            .session()
            .executeAsync(
                ScriptGraphStatement.newInstance("g.V().hasLabel('person').values('name')")
                    .setGraphName(sessionRule.getGraphName())
                    .setTraversalSource("g")
                    .setExecutionProfile(profile));

    // then
    checkAsyncResult(result, options, 0, 1, new ArrayList<>());
  }

  @UseDataProvider(location = ContinuousPagingITBase.class, value = "pagingOptions")
  @Test
  public void asynchronous_options_with_paging_disabled_should_fallback_to_single_page(
      Options options) throws ExecutionException, InterruptedException {
    // given
    DriverExecutionProfile profile = enablePaging(options, PagingEnabledOptions.DISABLED);

    if (options.sizeInBytes) {
      // Page sizes in bytes are not supported with graph queries
      return;
    }

    // when
    CompletionStage<AsyncGraphResultSet> result =
        sessionRule
            .session()
            .executeAsync(
                ScriptGraphStatement.newInstance("g.V().hasLabel('person').values('name')")
                    .setGraphName(sessionRule.getGraphName())
                    .setTraversalSource("g")
                    .setExecutionProfile(profile));

    // then
    AsyncGraphResultSet asyncGraphResultSet = result.toCompletableFuture().get();
    for (int i = 1; i <= 100; i++, asyncGraphResultSet.remaining()) {
      GraphNode node = asyncGraphResultSet.one();
      assertThat(node.asString()).isEqualTo("user" + i);
    }
    assertThat(asyncGraphResultSet.remaining()).isEqualTo(0);
  }

  private DriverExecutionProfile enablePaging(
      Options options, PagingEnabledOptions pagingEnabledOptions) {
    DriverExecutionProfile profile = options.asProfile(sessionRule.session());
    profile = profile.withString(DseDriverOption.GRAPH_PAGING_ENABLED, pagingEnabledOptions.name());
    return profile;
  }

  private void checkAsyncResult(
      CompletionStage<AsyncGraphResultSet> future,
      Options options,
      int rowsFetched,
      int pageNumber,
      List<GraphExecutionInfo> graphExecutionInfos)
      throws ExecutionException, InterruptedException {
    AsyncGraphResultSet result = future.toCompletableFuture().get();
    int remaining = result.remaining();
    rowsFetched += remaining;
    assertThat(remaining).isLessThanOrEqualTo(options.pageSize);

    if (options.expectedRows == rowsFetched) {
      assertThat(result.hasMorePages()).isFalse();
    } else {
      assertThat(result.hasMorePages()).isTrue();
    }

    int first = (pageNumber - 1) * options.pageSize + 1;
    int last = (pageNumber - 1) * options.pageSize + remaining;

    for (int i = first; i <= last; i++, remaining--) {
      GraphNode node = result.one();
      assertThat(node.asString()).isEqualTo("user" + i);
      assertThat(result.remaining()).isEqualTo(remaining - 1);
    }

    assertThat(result.remaining()).isZero();
    assertThat(result.getExecutionInfo()).isNotNull();
    assertThat(result.getExecutionInfo().getCoordinator().getEndPoint().resolve())
        .isEqualTo(firstCcmNode());

    graphExecutionInfos.add(result.getExecutionInfo());

    assertThat(graphExecutionInfos).hasSize(pageNumber);
    assertThat(result.getExecutionInfo()).isSameAs(graphExecutionInfos.get(pageNumber - 1));
    if (pageNumber == options.expectedPages) {
      assertThat(result.hasMorePages()).isFalse();
      assertThat(options.expectedRows).isEqualTo(rowsFetched);
      assertThat(options.expectedPages).isEqualTo(pageNumber);
    } else {
      assertThat(result.hasMorePages()).isTrue();
      checkAsyncResult(
          result.fetchNextPage(), options, rowsFetched, pageNumber + 1, graphExecutionInfos);
    }
  }

  @Test
  public void should_cancel_result_set() {
    // given
    DriverExecutionProfile profile = enablePaging();
    profile = profile.withInt(CONTINUOUS_PAGING_MAX_ENQUEUED_PAGES, 1);
    profile = profile.withInt(CONTINUOUS_PAGING_PAGE_SIZE, 10);

    // when
    GraphStatement statement =
        ScriptGraphStatement.newInstance("g.V().hasLabel('person').values('name')")
            .setGraphName(sessionRule.getGraphName())
            .setTraversalSource("g")
            .setExecutionProfile(profile);
    MultiPageGraphResultSet results =
        (MultiPageGraphResultSet) sessionRule.session().execute(statement);

    assertThat(((MultiPageGraphResultSet.RowIterator) results.iterator()).isCancelled()).isFalse();
    assertThat(((CountingIterator) results.iterator()).remaining()).isEqualTo(10);
    results.cancel();

    assertThat(((MultiPageGraphResultSet.RowIterator) results.iterator()).isCancelled()).isTrue();
    assertThat(((CountingIterator) results.iterator()).remaining()).isEqualTo(10);
    for (int i = 0; i < 10; i++) {
      results.one();
    }
  }

  private DriverExecutionProfile enablePaging() {
    DriverExecutionProfile profile =
        sessionRule.session().getContext().getConfig().getDefaultProfile();
    profile =
        profile.withString(
            DseDriverOption.GRAPH_PAGING_ENABLED, PagingEnabledOptions.ENABLED.name());
    return profile;
  }

  private SocketAddress firstCcmNode() {
    return ccmRule.getContactPoints().iterator().next().resolve();
  }
}
