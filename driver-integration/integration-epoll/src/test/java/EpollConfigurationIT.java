import com.datastax.driver.core.NettyOptions;
import com.datastax.driver.integration.BaseIntegrationTest;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class EpollConfigurationIT extends BaseIntegrationTest {

    private static final Logger logger = LoggerFactory.getLogger(EpollConfigurationIT.class);

    /**
     * <p>
     * Ensures that Epoll is used for I/O for the underlying netty instance used by the driver if
     * netty-transport-native-epoll is present in the classpath.
     *
     * @test_category packaging
     * @expected_result EpollEventLoopGroup is used as the EventLoopGroup instance, EpollSocketChannel is used as the
     *                  Channel instance.
     * @jira_ticket JAVA-676
     * @since 2.0.10, 2.1.6
     */
    @Test(groups="unit")
    public void should_use_epoll_for_netty_on_linux_only() {
        boolean isLinux = System.getProperty("os.name", "").toLowerCase(Locale.US).equals("linux");
        if(isLinux) {
            logger.debug("Detected os was Linux, expecting Epoll configuration.");
        } else {
            logger.warn("Did not detect Linux OS, so NIO configuration should be used.");
        }
        NettyOptions nettyOptions = new NettyOptions();
        Class<? extends SocketChannel> channelClass = isLinux ? EpollSocketChannel.class : NioSocketChannel.class;
        Class<? extends EventLoopGroup> eventLoopClass = isLinux ? EpollEventLoopGroup.class : NioEventLoopGroup.class;
        assertThat(nettyOptions.channelClass()).isEqualTo(channelClass);
        EventLoopGroup eventLoopGroup = nettyOptions.eventLoopGroup(new DefaultThreadFactory("test"));
        try {
            assertThat(eventLoopGroup).isInstanceOf(eventLoopClass);
        } finally {
            eventLoopGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS);
        }
    }
}
