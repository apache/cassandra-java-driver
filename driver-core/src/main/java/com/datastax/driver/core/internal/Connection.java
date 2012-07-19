package com.datastax.driver.core.internal;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;

import org.apache.cassandra.transport.*;

/**
 * A connection to a Cassandra Node.
 */
public class Connection extends org.apache.cassandra.transport.Connection
{
    public final InetSocketAddress address;

    private final ClientBootstrap bootstrap;
    private final Channel channel;
    private final Manager manager;

    private volatile ChannelFuture lastWriteFuture;
    private volatile boolean shutdown;

    /**
     * Create a new connection to a Cassandra node.
     *
     * The connection is open and initialized by the constructor.
     *
     * @throws ConnectionException if the connection attempts fails.
     */
    private Connection(InetSocketAddress address, Manager manager) throws ConnectionException {
        this.manager = manager;
        this.bootstrap = manager.bootstrap();

        bootstrap.setPipelineFactory(new PipelineFactory(this));

        ChannelFuture future = bootstrap.connect(address);

        // Wait until the connection attempt succeeds or fails.
        this.channel = future.awaitUninterruptibly().getChannel();
        if (!future.isSuccess())
        {
            bootstrap.releaseExternalResources();
            throw new TransportException(address, "Cannot connect", future.getCause());
        }
    }

    public Future<Message.Response> write(Message.Request request) {

        if (shutdown)
            throw new ConnectionException(address, "Connection has been closed");

        request.attach(this);
        inFlight.incrementAndGet();
        try {

            ChannelFuture future = channel.write(request);
            future.awaitUninterruptibly();
            if (!future.isSuccess())
                throw new TransportException(address, "Error writting", future.getCause());

            Message.Response msg = responseHandler.responses.take();
            if (msg instanceof ErrorMessage)
                throw new RuntimeException(((ErrorMessage)msg).errorMsg);
            return msg;
        } finally {
            inFlight.decrementAndGet();
        }
    }

    public void close() {

        // Make sure all new writes are rejected
        shutdown = true;

        try {
            // Busy waiting, we just wait for request to be fully written, shouldn't take long
            while (inFlight.get() > 0) {
                time.sleep(10);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        channel.close().awaitUninterruptibly();
        bootstrap.releaseExternalResources();
    }

    // Cruft needed because we reuse server side classes, but we don't care about it
    public void validateNewMessage(Message.Type type) {};
    public void applyStateTransition(Message.Type requestType, Message.Type responseType) {};
    public ClientState clientState() { return null; };

    public static class Manager {

        private ClientBoostrap boostrap() {
            ClientBoostrap b = new ClientBootstrap(new NioClientSocketChannelFactory(bossExecutor, workerExecutor));

            // TODO: handle this better (use SocketChannelConfig)
            b.setOption("connectTimeoutMillis", 10000);
            b.setOption("tcpNoDelay", true);
            b.setOption("keepAlive", true);

            return b;
        }

    }

    private static class PipelineFactory implements ChannelPipelineFactory
    {
        // Stateless handlers
        private static final Message.ProtocolDecoder messageDecoder = new Message.ProtocolDecoder();
        private static final Message.ProtocolEncoder messageEncoder = new Message.ProtocolEncoder();
        private static final Frame.Decompressor frameDecompressor = new Frame.Decompressor();
        private static final Frame.Compressor frameCompressor = new Frame.Compressor();
        private static final Frame.Encoder frameEncoder = new Frame.Encoder();

        // One more fallout of using server side classes; not a big deal
        private static final org.apache.cassandra.transport.Connection.Connection.Tracker tracker;
        static {
            tracker = new org.apache.cassandra.transport.Connection.Connection.Tracker() {
                public void addConnection(Channel ch, Connection connection) {}
                public void closeAll() {}
            };
        }

        private final org.apache.cassandra.transport.Connection.Connection.Factory cfactory;

        public PipelineFactory(final Connection connection) {
            this.cfactory = new org.apache.cassandra.transport.Connection.Factory() {
                public Connection newConnection() {
                    return connection;
                }
            };
        }

        public ChannelPipeline getPipeline() throws Exception {
            ChannelPipeline pipeline = Channels.pipeline();

            //pipeline.addLast("debug", new LoggingHandler());

            pipeline.addLast("frameDecoder", new Frame.Decoder(tracker, cfactory));
            pipeline.addLast("frameEncoder", frameEncoder);

            pipeline.addLast("frameDecompressor", frameDecompressor);
            pipeline.addLast("frameCompressor", frameCompressor);

            pipeline.addLast("messageDecoder", messageDecoder);
            pipeline.addLast("messageEncoder", messageEncoder);

            pipeline.addLast("handler", responseHandler);

            return pipeline;
        }
    }
}
