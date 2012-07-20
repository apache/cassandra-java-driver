package com.datastax.driver.core.transport;

import com.datastax.driver.core.utils.SimpleFuture;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.*;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

/**
 * A connection to a Cassandra Node.
 */
public class Connection extends org.apache.cassandra.transport.Connection
{
    public final InetSocketAddress address;

    private final ClientBootstrap bootstrap;
    private final Channel channel;
    private final Factory factory;
    private final Dispatcher dispatcher = new Dispatcher();

    private AtomicInteger inFlight = new AtomicInteger(0);
    private volatile boolean shutdown;

    /**
     * Create a new connection to a Cassandra node.
     *
     * The connection is open and initialized by the constructor.
     *
     * @throws ConnectionException if the connection attempts fails.
     */
    private Connection(InetSocketAddress address, Factory factory) throws ConnectionException {
        this.address = address;
        this.factory = factory;
        this.bootstrap = factory.bootstrap();

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

    /**
     * Write a request on this connection.
     *
     * @param request the request to send
     * @return a future on the server response
     *
     * @throws ConnectionException if the connection is closed
     * @throws TransportException if an I/O error while sending the request
     */
    public Future write(Message.Request request) throws ConnectionException {
        if (shutdown)
            throw new ConnectionException(address, "Connection has been closed");

        request.attach(this);

        // We only support synchronous mode so far
        if (!inFlight.compareAndSet(0, 1))
            throw new RuntimeException("Busy connection (this should not happen, please open a bug report if you see this)");

        try {

            Future future = new Future(this);

            // TODO: This assumes the connection is used synchronously, fix that at some point
            dispatcher.setFuture(future);

            ChannelFuture writeFuture = channel.write(request);
            writeFuture.awaitUninterruptibly();
            if (!writeFuture.isSuccess())
                throw new TransportException(address, "Error writting", writeFuture.getCause());

            return future;

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
                Thread.sleep(10);
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

    public static class Factory {

        private final ExecutorService bossExecutor = Executors.newCachedThreadPool();
        private final ExecutorService workerExecutor = Executors.newCachedThreadPool();

        private final InetSocketAddress address;

        public Factory(InetSocketAddress address) {
            this.address = address;
        }

        /**
         * Opens a new connection to the node this factory points to.
         *
         * @return the newly created (and initialized) connection.
         *
         * @throws ConnectionException if connection attempt fails.
         */
        public Connection open() throws ConnectionException {
            return new Connection(address, this);
        }

        private ClientBootstrap bootstrap() {
            ClientBootstrap b = new ClientBootstrap(new NioClientSocketChannelFactory(bossExecutor, workerExecutor));

            // TODO: handle this better (use SocketChannelConfig)
            b.setOption("connectTimeoutMillis", 10000);
            b.setOption("tcpNoDelay", true);
            b.setOption("keepAlive", true);

            return b;
        }

    }

    private class Dispatcher extends SimpleChannelUpstreamHandler {

        private volatile Future future;
        private volatile Exception exception;

        public void setFuture(Future future) {
            this.future = future;
        }

        @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
            // TODO: we should do something better than just throwing an exception
            if (future == null)
                throw new RuntimeException("Not future set");

            if (!(e.getMessage() instanceof Message.Response)) {
                future.setException(new TransportException(address, "Unexpected message received: " + e.getMessage()));
            } else {
                future.set((Message.Response)e.getMessage());
            }
            future = null;
        }
    }

    public static class Future extends SimpleFuture<Message.Response> {
        private final Connection connection;

        public Future(Connection connection) {
            this.connection = connection;
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
        private static final org.apache.cassandra.transport.Connection.Tracker tracker;
        static {
            tracker = new org.apache.cassandra.transport.Connection.Tracker() {
                public void addConnection(Channel ch, org.apache.cassandra.transport.Connection connection) {}
                public void closeAll() {}
            };
        }

        private final Connection connection;
        private final org.apache.cassandra.transport.Connection.Factory cfactory;

        public PipelineFactory(final Connection connection) {
            this.connection = connection;
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

            pipeline.addLast("dispatcher", connection.dispatcher);

            return pipeline;
        }
    }
}
