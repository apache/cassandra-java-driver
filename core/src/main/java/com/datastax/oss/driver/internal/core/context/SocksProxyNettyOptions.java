package com.datastax.oss.driver.internal.core.context;

import io.netty.channel.Channel;
import io.netty.handler.proxy.Socks5ProxyHandler;

import java.net.InetSocketAddress;

public class SocksProxyNettyOptions extends DefaultNettyOptions {
    private String socksProxyHost;
    private Integer socksProxyPort;

    public SocksProxyNettyOptions(InternalDriverContext context, String socksProxyHost, Integer socksProxyPort) {
        super(context);
        this.socksProxyHost = socksProxyHost;
        this.socksProxyPort = socksProxyPort;
    }

    @Override
    public void afterChannelInitialized(Channel channel) throws RuntimeException {
        try{
            channel.pipeline().addFirst(new Socks5ProxyHandler(new InetSocketAddress(socksProxyHost, socksProxyPort)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
