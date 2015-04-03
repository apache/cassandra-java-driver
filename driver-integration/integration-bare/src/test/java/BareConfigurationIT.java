import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.NettyOptions;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.integration.BaseIntegrationTest;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class BareConfigurationIT extends BaseIntegrationTest {

    /**
     * <p>
     * Ensures that if LZ4 is not in the classpath that it cannot be used for compression.
     *
     * @test_category packaging, connection:compression
     * @expected_result An exception is thrown at the time the cluster is built.
     */
    @Test(groups="unit", expectedExceptions=IllegalStateException.class,
            expectedExceptionsMessageRegExp = "The requested compression is not available.*")
    public void should_not_be_able_to_use_lz4() {
        configure(Cluster.builder())
                .addContactPointsWithPorts(Collections.singletonList(hostAddress)).
                withCompression(ProtocolOptions.Compression.LZ4).build();
    }

    /**
     * <p>
     * Ensures that if Snappy is not in the classpath that it cannot be used for compression.
     *
     * @test_category packaging, connection:compression
     * @expected_result An exception is thrown at the time the cluster is built.
     */
    @Test(groups="unit", expectedExceptions=IllegalStateException.class,
            expectedExceptionsMessageRegExp = "The requested compression is not available.*")
    public void should_not_be_able_to_use_snappy() {
        configure(Cluster.builder())
                .addContactPointsWithPorts(Collections.singletonList(hostAddress)).
                withCompression(ProtocolOptions.Compression.SNAPPY).build();
    }

    /**
     * <p>
     * Ensures that by default NIO is used for I/O for the underlying netty instance used by the driver.
     *
     * @test_category packaging
     * @expected_result NioEventLoopGroup is used as the EventLoopGroup instance, NioSocketChannel is used as the
     *                  Channel instance.
     * @jira_ticket JAVA-676
     * @since 2.0.10, 2.1.6
     */
    @Test(groups="unit")
    public void should_use_nio_for_netty() {
        NettyOptions nettyOptions = new NettyOptions();
        assertThat(nettyOptions.channelClass()).isEqualTo(NioSocketChannel.class);
        EventLoopGroup eventLoopGroup = nettyOptions.eventLoopGroup(new DefaultThreadFactory("test"));
        try {
            assertThat(eventLoopGroup).isInstanceOf(NioEventLoopGroup.class);
        } finally {
            eventLoopGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS);
        }
    }
}
