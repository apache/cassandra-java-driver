/*
 *      Copyright (C) 2012 DataStax Inc.
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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.util.concurrent.Uninterruptibles;

import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.core.exceptions.DriverInternalError;

import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.*;
import org.apache.cassandra.transport.messages.*;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// For LoggingHandler
//import org.jboss.netty.handler.logging.LoggingHandler;
//import org.jboss.netty.logging.InternalLogLevel;

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

    public final InetAddress address;
    private final String name;

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
    private Connection(String name, InetAddress address, Factory factory) throws ConnectionException, InterruptedException {
        super(EMPTY_TRACKER);

        this.address = address;
        this.factory = factory;
        this.name = name;

        ClientBootstrap bootstrap = factory.newBootstrap();
        bootstrap.setPipelineFactory(new PipelineFactory(this));

        ChannelFuture future = bootstrap.connect(new InetSocketAddress(address, factory.getPort()));

        writer.incrementAndGet();
        try {
            // Wait until the connection attempt succeeds or fails.
            this.channel = future.awaitUninterruptibly().getChannel();
            this.factory.allChannels.add(this.channel);
            if (!future.isSuccess())
            {
                if (logger.isDebugEnabled())
                    logger.debug(String.format("[%s] Error connecting to %s%s", name, address, extractMessage(future.getCause())));
                throw new TransportException(address, "Cannot connect", future.getCause());
            }
        } finally {
            writer.decrementAndGet();
        }

        logger.trace("[{}] Connection opened successfully", name);
        initializeTransport();
        logger.trace("[{}] Transport initialized and ready", name);
    }

    private static String extractMessage(Throwable t) {
        if (t == null || t.getMessage().isEmpty())
            return "";
        return " (" + t.getMessage() + ")";
    }

    private void initializeTransport() throws ConnectionException, InterruptedException {

        // TODO: we will need to get fancy about handling protocol version at
        // some point, but keep it simple for now.
        Map<String, String> options = new HashMap<String, String>() {{
            put(StartupMessage.CQL_VERSION, CQL_VERSION);
        }};
        ProtocolOptions.Compression compression = factory.configuration.getProtocolOptions().getCompression();
        if (compression != ProtocolOptions.Compression.NONE)
        {
            options.put(StartupMessage.COMPRESSION, compression.toString());
            setCompressor(compression.compressor());
        }
        StartupMessage startup = new StartupMessage(options);
        try {
            Message.Response response = write(startup).get();
            switch (response.type) {
                case READY:
                    break;
                case ERROR:
                    throw defunct(new TransportException(address, String.format("Error initializing connection: %s", ((ErrorMessage)response).error.getMessage())));
                case AUTHENTICATE:
                    CredentialsMessage creds = new CredentialsMessage();
                    creds.credentials.putAll(factory.authProvider.getAuthInfo(address));
                    Message.Response authResponse = write(creds).get();
                    switch (authResponse.type) {
                        case READY:
                            break;
                        case ERROR:
                            throw new AuthenticationException(address, (((ErrorMessage)response).error).getMessage());
                        default:
                            throw defunct(new TransportException(address, String.format("Unexpected %s response message from server to a CREDENTIALS message", authResponse.type)));
                    }
                    break;
                default:
                    throw defunct(new TransportException(address, String.format("Unexpected %s response message from server to a STARTUP message", response.type)));
            }
        } catch (BusyConnectionException e) {
            throw new DriverInternalError("Newly created connection should not be busy");
        } catch (ExecutionException e) {
            throw defunct(new ConnectionException(address, "Unexpected error during transport initialization", e.getCause()));
        }
    }

    public boolean isDefunct() {
        return isDefunct;
    }

    public ConnectionException lastException() {
        return exception;
    }

    ConnectionException defunct(ConnectionException e) {
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
            logger.trace("[{}] Setting keyspace {}", name, keyspace);
            Message.Response response = Uninterruptibles.getUninterruptibly(write(new QueryMessage("USE \"" + keyspace + "\"", ConsistencyLevel.DEFAULT_CASSANDRA_CL)));
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
        } catch (BusyConnectionException e) {
            logger.error("Tried to set the keyspace on busy connection. This should not happen but is not critical");
        } catch (ExecutionException e) {
            throw defunct(new ConnectionException(address, "Error while setting keyspace", e));
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
    public Future write(Message.Request request) throws ConnectionException, BusyConnectionException {
        Future future = new Future(request);
        write(future);
        return future;
    }

    public void write(ResponseCallback callback) throws ConnectionException, BusyConnectionException {

        Message.Request request = callback.request();

        if (isDefunct)
            throw new ConnectionException(address, "Write attempt on defunct connection");

        if (isClosed)
            throw new ConnectionException(address, "Connection has been closed");

        request.attach(this);

        ResponseHandler handler = new ResponseHandler(dispatcher, callback);
        dispatcher.add(handler);
        request.setStreamId(handler.streamId);

        logger.trace("[{}] writing request {}", name, request);
        writer.incrementAndGet();
        channel.write(request).addListener(writeHandler(request, handler));
    }

    private ChannelFutureListener writeHandler(final Message.Request request, final ResponseHandler handler) {
        return new ChannelFutureListener() {
            public void operationComplete(ChannelFuture writeFuture) {

                writer.decrementAndGet();

                if (!writeFuture.isSuccess()) {

                    logger.debug("[{}] Error writing request {}", name, request);
                    // Remove this handler from the dispatcher so it don't get notified of the error
                    // twice (we will fail that method already)
                    dispatcher.removeHandler(handler.streamId);

                    ConnectionException ce;
                    if (writeFuture.getCause() instanceof java.nio.channels.ClosedChannelException) {
                        ce = new TransportException(address, "Error writing: Closed channel");
                    } else {
                        ce = new TransportException(address, "Error writing", writeFuture.getCause());
                    }
                    handler.callback.onException(Connection.this, defunct(ce));
                } else {
                    logger.trace("[{}] request sent successfully", name);
                }
            }
        };
    }

    public void close() {
        if (isClosed)
            return;

        // Note: there is no guarantee only one thread will reach that point, but executing this
        // method multiple time is harmless. If the latter change, we'll have to CAS isClosed to
        // make sure this gets executed only once.

        logger.trace("[{}] closing connection", name);

        // Make sure all new writes are rejected
        isClosed = true;

        if (!isDefunct) {
            // Busy waiting, we just wait for request to be fully written, shouldn't take long
            while (writer.get() > 0)
                Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
        }

        channel.close().awaitUninterruptibly();
        // Note: we must not call releaseExternalResources on the bootstrap, because this shutdown the executors, which are shared
    }

    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public String toString() {
        return String.format("Connection[%s, inFlight=%d, closed=%b]", name, inFlight.get(), isClosed);
    }

    // Cruft needed because we reuse server side classes, but we don't care about it
    public void validateNewMessage(Message.Type type) {};
    public void applyStateTransition(Message.Type requestType, Message.Type responseType) {};
    public ClientState clientState() { return null; };

    public static class Factory {

        private final ExecutorService bossExecutor = Executors.newCachedThreadPool();
        private final ExecutorService workerExecutor = Executors.newCachedThreadPool();

        private final ChannelFactory channelFactory = new NioClientSocketChannelFactory(bossExecutor, workerExecutor);
        private final ChannelGroup allChannels = new DefaultChannelGroup();


        private final ConcurrentMap<Host, AtomicInteger> idGenerators = new ConcurrentHashMap<Host, AtomicInteger>();
        public final DefaultResponseHandler defaultHandler;
        public final Configuration configuration;

        public final AuthInfoProvider authProvider;

        public Factory(Cluster.Manager manager, AuthInfoProvider authProvider) {
            this(manager, manager.configuration, authProvider);
        }

        private Factory(DefaultResponseHandler defaultHandler, Configuration configuration, AuthInfoProvider authProvider) {
            this.defaultHandler = defaultHandler;
            this.configuration = configuration;
            this.authProvider = authProvider;
        }

        public int getPort() {
            return configuration.getProtocolOptions().getPort();
        }

        /**
         * Opens a new connection to the node this factory points to.
         *
         * @return the newly created (and initialized) connection.
         *
         * @throws ConnectionException if connection attempt fails.
         */
        public Connection open(Host host) throws ConnectionException, InterruptedException {
            InetAddress address = host.getAddress();
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

        private ClientBootstrap newBootstrap() {
            ClientBootstrap b = new ClientBootstrap(channelFactory);

            SocketOptions options = configuration.getSocketOptions();

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

        public void shutdown() {
            allChannels.close();
            channelFactory.releaseExternalResources();
        }
    }

    private class Dispatcher extends SimpleChannelUpstreamHandler {

        public final StreamIdGenerator streamIdHandler = new StreamIdGenerator();
        private final ConcurrentMap<Integer, ResponseHandler> pending = new ConcurrentHashMap<Integer, ResponseHandler>();

        public void add(ResponseHandler handler) {
            ResponseHandler old = pending.put(handler.streamId, handler);
            assert old == null;
        }

        public void removeHandler(int streamId) {
            pending.remove(streamId);
            streamIdHandler.release(streamId);
        }

        @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
            if (!(e.getMessage() instanceof Message.Response)) {
                logger.error("[{}] Received unexpected message: {}", name, e.getMessage());
                defunct(new TransportException(address, "Unexpected message received: " + e.getMessage()));
            } else {
                Message.Response response = (Message.Response)e.getMessage();
                int streamId = response.getStreamId();

                logger.trace("[{}] received: {}", name, e.getMessage());

                if (streamId < 0) {
                    factory.defaultHandler.handle(response);
                    return;
                }

                ResponseHandler handler = pending.remove(streamId);
                streamIdHandler.release(streamId);
                if (handler == null) {
                    // Note: this is a bug, either us or cassandra. So log it, but I'm not sure it's worth breaking
                    // the connection for that.
                    logger.error("[{}] No handler set for stream {} (this is a bug, either of this driver or of Cassandra, you should report it)", name, streamId);
                    return;
                }
                handler.callback.onSet(Connection.this, response);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
            if (logger.isTraceEnabled())
                logger.trace(String.format("[%s] connection error", name), e.getCause());

            // Ignore exception while writing, this will be handled by write() directly
            if (writer.get() > 0)
                return;

            defunct(new TransportException(address, "Unexpected exception triggered", e.getCause()));
        }

        public void errorOutAllHandler(ConnectionException ce) {
            Iterator<ResponseHandler> iter = pending.values().iterator();
            while (iter.hasNext())
            {
                iter.next().callback.onException(Connection.this, ce);
                iter.remove();
            }
        }

        @Override
        public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e)
        {
            // If we've closed the channel server side then we don't really want to defunct the connection, but
            // if there is remaining thread waiting on us, we still want to wake them up
            if (isClosed)
                errorOutAllHandler(new TransportException(address, "Channel has been closed"));
            else
                defunct(new TransportException(address, "Channel has been closed"));
        }
    }

    static class Future extends SimpleFuture<Message.Response> implements RequestHandler.Callback {

        private final Message.Request request;
        private volatile InetAddress address;

        public Future(Message.Request request) {
            this.request = request;
        }

        public Message.Request request() {
            return request;
        }

        public void onSet(Connection connection, Message.Response response, ExecutionInfo info) {
            onSet(connection, response);
        }

        public void onSet(Connection connection, Message.Response response) {
            this.address = connection.address;
            super.set(response);
        }

        public void onException(Connection connection, Exception exception) {
            super.setException(exception);
        }

        public InetAddress getAddress() {
            return address;
        }
    }

    interface ResponseCallback {
        public Message.Request request();
        public void onSet(Connection connection, Message.Response response);
        public void onException(Connection connection, Exception exception);
    }

    private static class ResponseHandler {

        public final int streamId;
        public final ResponseCallback callback;

        public ResponseHandler(Dispatcher dispatcher, ResponseCallback callback) throws BusyConnectionException {
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

            //pipeline.addLast("debug", new LoggingHandler(InternalLogLevel.INFO));

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
