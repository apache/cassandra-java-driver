import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.datastax.shaded.netty.channel.ChannelDuplexHandler;
import com.datastax.shaded.netty.channel.ChannelHandlerContext;
import com.datastax.shaded.netty.channel.EventLoopGroup;
import com.datastax.shaded.netty.channel.nio.NioEventLoopGroup;
import com.datastax.shaded.netty.channel.socket.SocketChannel;
import com.datastax.shaded.netty.channel.socket.nio.NioSocketChannel;
import com.datastax.shaded.netty.util.concurrent.DefaultThreadFactory;
import com.google.common.util.concurrent.*;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertFalse;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.NettyOptions;
import com.datastax.driver.integration.BaseIntegrationTest;

public class ShadedConfigurationIT extends BaseIntegrationTest {

    /**
     * <p>
     * Validates that NIO is used unconditionally when using the shaded jar.  Additionally
     * ensures that the shaded netty components are used instead of the alternative io.netty
     * classes when present in the classpath.
     *
     * @test_category packaging
     * @expected_result Shaded Netty components are used.
     * @jira_ticket JAVA-676
     * @since 2.0.10, 2.1.6
     */
    @Test(groups = "unit")
    public void should_use_nio_for_shaded_netty() {
        NettyOptions nettyOptions = new NettyOptions();
        assertThat(nettyOptions.channelClass()).isEqualTo(NioSocketChannel.class);
        EventLoopGroup eventLoopGroup = nettyOptions.eventLoopGroup(new DefaultThreadFactory("test"));
        try {
            assertThat(eventLoopGroup).isInstanceOf(NioEventLoopGroup.class);
        } finally {
            eventLoopGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS);
        }
    }

    /**
     * <p>
     * Validates that netty components can be customized via {@link NettyOptions} when using
     * the shaded jar configuration.
     *
     * <p>
     * This is not a comprehensive test as it just ensures {@link NettyOptions} can be customized,
     * but does not exhaustively test {@link NettyOptions} as
     * {@link com.datastax.driver.core.NettyOptionsTest} provides this test coverage.
     *
     * @test_category packaging
     * @expected_result custom {@link NettyOptions} is used.
     * @jira_ticket JAVA-676
     * @since 2.0.10, 2.1.6
     */
    @Test(groups = "short")
    public void should_be_able_to_customize_netty_options() throws Exception {
        // A simple configuration where all Clusters share the same EventLoopGroup.
        final EventLoopGroup eventLoop = new NioEventLoopGroup();
        final AtomicInteger readCount = new AtomicInteger(0);

        ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(3));

        NettyOptions nettyOptions = new NettyOptions() {
            @Override
            public EventLoopGroup eventLoopGroup(ThreadFactory threadFactory) {
                return eventLoop;
            }

            @Override
            public void onClusterClose(EventLoopGroup eventLoopGroup) {
                // NO-OP, we want to reuse the event loop.
            }

            @Override
            public void afterChannelInitialized(SocketChannel channel) throws Exception {
                // Add a simple handler that counts number of read calls.
                channel.pipeline().addFirst(new ChannelDuplexHandler() {
                    @Override
                    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                        readCount.incrementAndGet();
                        ctx.fireChannelRead(msg);
                    }
                });
            }
        };

        try {
            Cluster.Builder builder = Cluster.builder().withNettyOptions(nettyOptions)
                .addContactPointsWithPorts(Collections.singletonList(hostAddress));

            // Create a cluster and exercise it, then close it
            // this is mostly to ensure onClusterClose is called and the event loop is not closed.
            Cluster cluster0 = builder.build();
            try {
                ListenableFuture<Void> future = executor.submit(basicLifecycle(cluster0.connect(keyspace), "cluster0", 100, 10));
                Uninterruptibles.getUninterruptibly(future, 5, TimeUnit.SECONDS);
                cluster0.close();
                assertFalse(eventLoop.isShutdown() || eventLoop.isShuttingDown());
                assertThat(readCount.get()).isGreaterThan(0);
                readCount.set(0);
            } finally {
                cluster0.close();
            }

            // Create 3 clusters and simultaneously exercise them.
            Cluster cluster1 = builder.build();
            Cluster cluster2 = builder.build();
            Cluster cluster3 = builder.build();
            try {
                ListenableFuture[] futures = new ListenableFuture[]{
                        executor.submit(basicLifecycle(cluster1.connect(keyspace), "cluster1", 100, 10)),
                        executor.submit(basicLifecycle(cluster2.connect(keyspace), "cluster2", 100, 10)),
                        executor.submit(basicLifecycle(cluster3.connect(keyspace), "cluster3", 100, 10))
                };

                // Waits for all futures to complete, if any errors encountered this test fails.
                Uninterruptibles.getUninterruptibly(Futures.allAsList(futures), 10, TimeUnit.SECONDS);
                assertThat(readCount.get()).isGreaterThan(0);
            } finally {
                cluster1.close();
                cluster2.close();
                cluster3.close();
            }
        } finally {
            eventLoop.shutdownGracefully(0, 5, TimeUnit.SECONDS);
            executor.shutdown();
        }
    }
}
