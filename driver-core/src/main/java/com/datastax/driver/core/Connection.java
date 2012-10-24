package com.datastax.driver.core;

import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.utils.SimpleFuture;

import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.*;
import org.apache.cassandra.transport.messages.*;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A connection to a Cassandra Node.
 */
class Connection extends org.apache.cassandra.transport.Connection
{
    public static final int MAX_STREAM_PER_CONNECTION = 128;

    private static final Logger logger = LoggerFactory.getLogger(Connection.class);

    // TODO: that doesn't belong here
    private static final String CQL_VERSION = "3.0.0";

    private static final org.apache.cassandra.transport.Connection.Tracker EMPTY_TRACKER = new org.apache.cassandra.transport.Connection.Tracker() {
        public void addConnection(Channel ch, org.apache.cassandra.transport.Connection connection) {}
        public void closeAll() {}
    };

    public final InetSocketAddress address;
    private final String name;

    private final ClientBootstrap bootstrap;
    private final Channel channel;
    private final Factory factory;
    private final Dispatcher dispatcher = new Dispatcher();

    // Used by connnection pooling to count how many requests are "in flight" on that connection.
    public final AtomicInteger inFlight = new AtomicInteger(0);

    private final AtomicInteger writer = new AtomicInteger(0);
    private volatile boolean isClosed;
    private volatile String keyspace;

    private volatile boolean isDefunct;
    private volatile ConnectionException exception;

    /**
     * Create a new connection to a Cassandra node.
     *
     * The connection is open and initialized by the constructor.
     *
     * @throws ConnectionException if the connection attempts fails or is
     * refused by the server.
     */
    private Connection(String name, InetSocketAddress address, Factory factory) throws ConnectionException {
        super(EMPTY_TRACKER);

        this.address = address;
        this.factory = factory;
        this.name = name;
        this.bootstrap = factory.bootstrap();

        bootstrap.setPipelineFactory(new PipelineFactory(this));

        ChannelFuture future = bootstrap.connect(address);

        writer.incrementAndGet();
        try {
            // Wait until the connection attempt succeeds or fails.
            this.channel = future.awaitUninterruptibly().getChannel();
            if (!future.isSuccess())
            {
                logger.debug(String.format("[%s] Error connecting to %s%s", name, address, extractMessage(future.getCause())));
                throw new TransportException(address, "Cannot connect", future.getCause());
            }
        } finally {
            writer.decrementAndGet();
        }

        logger.trace(String.format("[%s] Connection opened successfully", name));
        initializeTransport();
        logger.trace(String.format("[%s] Transport initialized and ready", name));
    }

    private static String extractMessage(Throwable t) {
        if (t == null || t.getMessage().isEmpty())
            return "";
        return " (" + t.getMessage() + ")";
    }

    private void initializeTransport() throws ConnectionException {

        // TODO: we will need to get fancy about handling protocol version at
        // some point, but keep it simple for now.
        Map<String, String> options = new HashMap<String, String>() {{
            put(StartupMessage.CQL_VERSION, CQL_VERSION);
        }};
        ConnectionsConfiguration.ProtocolOptions.Compression compression = factory.configuration.getProtocolOptions().getCompression();
        if (compression != ConnectionsConfiguration.ProtocolOptions.Compression.NONE)
            options.put(StartupMessage.COMPRESSION, compression.toString());
        StartupMessage startup = new StartupMessage(options);
        try {
            Message.Response response = write(startup).get();
            switch (response.type) {
                case READY:
                    break;
                case ERROR:
                    throw defunct(new TransportException(address, String.format("Error initializing connection", ((ErrorMessage)response).error)));
                case AUTHENTICATE:
                    throw new TransportException(address, "Authentication required but not yet supported");
                default:
                    throw defunct(new TransportException(address, String.format("Unexpected %s response message from server to a STARTUP message", response.type)));
            }
        } catch (ExecutionException e) {
            throw defunct(new ConnectionException(address, "Unexpected error during transport initialization", e.getCause()));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean isDefunct() {
        return isDefunct;
    }

    public ConnectionException lastException() {
        return exception;
    }

    private ConnectionException defunct(ConnectionException e) {
        exception = e;
        isDefunct = true;
        dispatcher.errorOutAllHandler(e);
        return e;
    }

    public String keyspace() {
        return keyspace;
    }

    public void setKeyspace(String keyspace) throws ConnectionException {
        if (keyspace == null)
            return;

        if (this.keyspace != null && this.keyspace.equals(keyspace))
            return;

        try {
            logger.trace(String.format("[%s] Setting keyspace %s", name, keyspace));
            Message.Response response = write(new QueryMessage("USE " + keyspace, ConsistencyLevel.DEFAULT_CASSANDRA_CL)).get();
            switch (response.type) {
                case RESULT:
                    this.keyspace = keyspace;
                    break;
                default:
                    // The code set the keyspace only when a successful 'use'
                    // has been perform, so there shouldn't be any error here.
                    // It can happen however that the node we're connecting to
                    // is not up on the schema yet. In that case, defuncting
                    // the connection is not a bad choice.
                    defunct(new ConnectionException(address, String.format("Problem while setting keyspace, got %s as response", response)));
                    break;
            }
        } catch (ConnectionException e) {
            throw defunct(e);
        } catch (ExecutionException e) {
            throw defunct(new ConnectionException(address, "Error while setting keyspace", e));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
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
        Future future = new Future(request);
        write(future);
        return future;
    }

    public void write(ResponseCallback callback) throws ConnectionException {

        Message.Request request = callback.request();

        if (isDefunct)
            throw new ConnectionException(address, "Write attempt on defunct connection");

        if (isClosed)
            throw new ConnectionException(address, "Connection has been closed");

        request.attach(this);

        // We only support synchronous mode so far
        writer.incrementAndGet();
        try {

            ResponseHandler handler = new ResponseHandler(dispatcher, callback);
            dispatcher.add(handler);
            request.setStreamId(handler.streamId);

            logger.trace(String.format("[%s] writting request %s", name, request));
            ChannelFuture writeFuture = channel.write(request);
            writeFuture.awaitUninterruptibly();
            if (!writeFuture.isSuccess())
            {
                logger.debug(String.format("[%s] Error writting request %s", name, request));
                // Remove this handler from the dispatcher so it don't get notified of the error
                // twice (we will fail that method already)
                dispatcher.removeHandler(handler.streamId);

                ConnectionException ce;
                if (writeFuture.getCause() instanceof java.nio.channels.ClosedChannelException) {
                    ce = new TransportException(address, "Error writting: Closed channel");
                } else {
                    ce = new TransportException(address, "Error writting", writeFuture.getCause());
                }
                throw defunct(ce);
            }

            logger.trace(String.format("[%s] request sent successfully", name));

        } finally {
            writer.decrementAndGet();
        }
    }

    public void close() {
        if (isClosed)
            return;

        // TODO: put that to trace
        logger.debug(String.format("[%s] closing connection", name));

        // Make sure all new writes are rejected
        isClosed = true;

        if (!isDefunct) {
            try {
                // Busy waiting, we just wait for request to be fully written, shouldn't take long
                while (writer.get() > 0)
                    Thread.sleep(10);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        channel.close().awaitUninterruptibly();
        // Note: we must not call releaseExternalResources, because this shutdown the executors, which are shared
    }

    public boolean isClosed() {
        return isClosed;
    }

    // Cruft needed because we reuse server side classes, but we don't care about it
    public void validateNewMessage(Message.Type type) {};
    public void applyStateTransition(Message.Type requestType, Message.Type responseType) {};
    public ClientState clientState() { return null; };

    public static class Factory {

        private final ExecutorService bossExecutor = Executors.newCachedThreadPool();
        private final ExecutorService workerExecutor = Executors.newCachedThreadPool();

        private final ConcurrentMap<Host, AtomicInteger> idGenerators = new ConcurrentHashMap<Host, AtomicInteger>();
        private final DefaultResponseHandler defaultHandler;
        private final ConnectionsConfiguration configuration;

        public Factory(Cluster.Manager manager) {
            this(manager, manager.configuration.getConnectionsConfiguration());
        }

        private Factory(DefaultResponseHandler defaultHandler, ConnectionsConfiguration configuration) {
            this.defaultHandler = defaultHandler;
            this.configuration = configuration;
        }

        /**
         * Opens a new connection to the node this factory points to.
         *
         * @return the newly created (and initialized) connection.
         *
         * @throws ConnectionException if connection attempt fails.
         */
        public Connection open(Host host) throws ConnectionException {
            InetSocketAddress address = host.getAddress();
            String name = address.toString() + "-" + getIdGenerator(host).getAndIncrement();
            return new Connection(name, address, this);
        }

        private AtomicInteger getIdGenerator(Host host) {
            AtomicInteger g = idGenerators.get(host);
            if (g == null) {
                g = new AtomicInteger(1);
                AtomicInteger old = idGenerators.putIfAbsent(host, g);
                if (old != null)
                    g = old;
            }
            return g;
        }

        private ClientBootstrap bootstrap() {
            ClientBootstrap b = new ClientBootstrap(new NioClientSocketChannelFactory(bossExecutor, workerExecutor));

            ConnectionsConfiguration.SocketOptions options = configuration.getSocketOptions();

            b.setOption("connectTimeoutMillis", options.getConnectTimeoutMillis());
            Boolean keepAlive = options.getKeepAlive();
            if (keepAlive != null)
                b.setOption("keepAlive", keepAlive);
            Boolean reuseAddress = options.getReuseAddress();
            if (reuseAddress != null)
                b.setOption("reuseAddress", reuseAddress);
            Integer soLinger = options.getSoLinger();
            if (soLinger != null)
                b.setOption("soLinger", soLinger);
            Boolean tcpNoDelay = options.getTcpNoDelay();
            if (tcpNoDelay != null)
                b.setOption("tcpNoDelay", tcpNoDelay);
            Integer receiveBufferSize = options.getReceiveBufferSize();
            if (receiveBufferSize != null)
                b.setOption("receiveBufferSize", receiveBufferSize);
            Integer sendBufferSize = options.getSendBufferSize();
            if (sendBufferSize != null)
                b.setOption("sendBufferSize", sendBufferSize);

            return b;
        }

        public DefaultResponseHandler defaultHandler() {
            return defaultHandler;
        }
    }

    // TODO: Having a map of Integer -> ResponseHandler might be overkill if we
    // use the connection synchronously. See if we want to support lighter
    // dispatcher that assume synchronous?
    private class Dispatcher extends SimpleChannelUpstreamHandler {

        public final StreamIdGenerator streamIdHandler = new StreamIdGenerator();
        private final ConcurrentMap<Integer, ResponseHandler> pending = new ConcurrentHashMap<Integer, ResponseHandler>();

        public void add(ResponseHandler handler) {
            ResponseHandler old = pending.put(handler.streamId, handler);
            assert old == null;
        }

        public void removeHandler(int streamId) {
            pending.remove(streamId);
        }

        @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
            logger.trace(String.format("[%s] received ", e.getMessage()));

            if (!(e.getMessage() instanceof Message.Response)) {
                logger.debug(String.format("[%s] Received unexpected message: %s", name, e.getMessage()));
                defunct(new TransportException(address, "Unexpected message received: " + e.getMessage()));
                // TODO: we should allow calling some handler for such error
            } else {
                Message.Response response = (Message.Response)e.getMessage();
                int streamId = response.getStreamId();
                if (streamId < 0) {
                    factory.defaultHandler().handle(response);
                    return;
                }

                ResponseHandler handler = pending.remove(streamId);
                streamIdHandler.release(streamId);
                if (handler == null)
                    // TODO: we should handle those with a default handler
                    throw new RuntimeException("No handler set for " + streamId + ", handlers = " + pending);
                handler.callback.onSet(Connection.this, response);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
            logger.trace(String.format("[%s] connection error", name), e.getCause());

            // Ignore exception while writting, this will be handled by write() directly
            if (writer.get() > 0)
                return;

            defunct(new TransportException(address, "Unexpected exception triggered", e.getCause()));
        }

        public void errorOutAllHandler(ConnectionException ce) {
            Iterator<ResponseHandler> iter = pending.values().iterator();
            while (iter.hasNext())
            {
                iter.next().callback.onException(ce);
                iter.remove();
            }
        }
    }

    // TODO: Do we really need that after all?
    static class Future extends SimpleFuture<Message.Response> implements ResponseCallback {

        private final Message.Request request;
        private volatile InetSocketAddress address;

        public Future(Message.Request request) {
            this.request = request;
        }

        public Message.Request request() {
            return request;
        }

        public void onSet(Connection connection, Message.Response response) {
            this.address = connection.address;
            super.set(response);
        }

        public void onException(Exception exception) {
            super.setException(exception);
        }

        public InetSocketAddress getAddress() {
            return address;
        }
    }

    interface ResponseCallback {
        public Message.Request request();
        public void onSet(Connection connection, Message.Response response);
        public void onException(Exception exception);
    }

    private static class ResponseHandler {

        public final int streamId;
        public final ResponseCallback callback;

        public ResponseHandler(Dispatcher dispatcher, ResponseCallback callback) {
            this.streamId = dispatcher.streamIdHandler.next();
            this.callback = callback;
        }
    }

    public interface DefaultResponseHandler {
        public void handle(Message.Response response);
    }

    private static class PipelineFactory implements ChannelPipelineFactory {
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
                public Connection newConnection(org.apache.cassandra.transport.Connection.Tracker tracker) {
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
