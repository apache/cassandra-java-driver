/*
 *      Copyright (C) 2012-2014 DataStax Inc.
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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.concurrent.TimeUnit.SECONDS;

import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

import static com.datastax.driver.core.CCMBridge.ipOfNode;

public class NettyCustomizerTest {

    @Test
    public void should_invoke_netty_customizer_after_channel_initialization() throws Exception {
        //given
        CCMBridge ccm = CCMBridge.create("test", 1);
        Cluster cluster = null;
        try {
            NettyCustomizerSpy spy = new NettyCustomizerSpy();
            cluster = Cluster.builder().addContactPoint(ipOfNode(1)).withNettyCustomizer(spy).build();
            // when
            cluster.init();
            spy.waitUntilCalled();
            // then
            assertThat(spy.wasCalled()).isTrue();
        } finally {
            if (cluster != null)
                cluster.close();
            ccm.remove();
        }
    }

    /**
     * A spy that records if #afterChannelInitialized() has been called at least once.
     */
    private static class NettyCustomizerSpy extends NettyCustomizer {

        private final CountDownLatch latch = new CountDownLatch(1);

        @Override
        public void afterChannelInitialized(SocketChannel channel) throws Exception {
            ChannelPipeline pipeline = channel.pipeline();
            pipeline.addLast("test", new ChannelInboundHandlerAdapter() {
                @Override
                public void channelActive(ChannelHandlerContext ctx) throws Exception {
                    latch.countDown();
                }
            });
        }

        public void waitUntilCalled() throws InterruptedException {
            latch.await(10, SECONDS);
        }

        public boolean wasCalled() {
            return latch.getCount() == 0;
        }
    }

}