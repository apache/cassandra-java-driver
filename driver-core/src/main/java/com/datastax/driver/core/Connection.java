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

import javax.net.ssl.SSLEngine;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.ssl.SslHandler;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.core.exceptions.DriverInternalError;

// For LoggingHandler
//import org.jboss.netty.handler.logging.LoggingHandler;
//import org.jboss.netty.logging.InternalLogLevel;

/**
 * A connection to a Cassandra Node.
 */
class Connection {

    private static final Logger logger = LoggerFactory.getLogger(Connection.class);
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    public final InetSocketAddress address;
    private final String name;

    private final Channel channel;
    private final Factory factory;

    private final Dispatcher dispatcher = new Dispatcher();

    // Used by connection pooling to count how many requests are "in flight" on that connection.
    public final AtomicInteger inFlight = new AtomicInteger(0);

    private final AtomicInteger writer = new AtomicInteger(0);
    private volatile String keyspace;

    private volatile boolean isInitialized;
    private volatile boolean isDefunct;

    private final AtomicReference<ConnectionCloseFuture> closeFuture = new AtomicReference<ConnectionCloseFuture>();

    /**
     * Create a new connection to a Cassandra node.
     *
     * The connection is open and initialized by the constructor.
     *
     * @throws ConnectionException if the connection attempts fails or is
     * refused by the server.
     */
    protected Connection(String name, InetSocketAddress address, Factory factory) throws ConnectionException, InterruptedException, UnsupportedProtocolVersionException {
        this.address = address;
        this.factory = factory;
        this.name = name;

        ClientBootstrap bootstrap = factory.newBootstrap();
        ProtocolOptions protocolOptions = factory.configuration.getProtocolOptions();
        int protocolVersion = factory.protocolVersion == 1 ? 1 : 2;
        bootstrap.setPipelineFactory(new PipelineFactory(this, protocolVersion, protocolOptions.getCompression().compressor, protocolOptions.getSSLOptions()));

        ChannelFuture future = bootstrap.connect(address);

        writer.incrementAndGet();
        try {
            // Wait until the connection attempt succeeds or fails.
            this.channel = future.awaitUninterruptibly().getChannel();
            this.factory.allChannels.add(this.channel);
            if (!future.isSuccess())
            {
                if (logger.isDebugEnabled())
                    logger.debug(String.format("[%s] Error connecting to %s%s", name, address, extractMessage(future.getCause())));
                throw defunct(new TransportException(address, "Cannot connect", future.getCause()));
            }
        } finally {
            writer.decrementAndGet();
        }

        logger.trace("[{}] Connection opened successfully", name);
        initializeTransport(protocolVersion);
        logger.debug("[{}] Transport initialized and ready", name);
        isInitialized = true;
    }

    private static String extractMessage(Throwable t) {
        if (t == null)
            return "";
        String msg = t.getMessage() == null || t.getMessage().isEmpty()
                   ? t.toString()
                   : t.getMessage();
        return " (" + msg + ')';
    }

    private void initializeTransport(int version) throws ConnectionException, InterruptedException, UnsupportedProtocolVersionException {
        try {
            ProtocolOptions.Compression compression = factory.configuration.getProtocolOptions().getCompression();
            Message.Response response = write(new Requests.Startup(compression)).get();
            switch (response.type) {
                case READY:
                    break;
                case ERROR:
                    Responses.Error error = (Responses.Error)response;
                    // Testing for a specific string is a tad fragile but well, we don't have much choice
                    if (error.code == ExceptionCode.PROTOCOL_ERROR && error.message.contains("Invalid or unsupported protocol version"))
                        throw unsupportedProtocolVersionException(version);
                    throw defunct(new TransportException(address, String.format("Error initializing connection: %s", error.message)));
                case AUTHENTICATE:
                    Authenticator authenticator = factory.authProvider.newAuthenticator(address);
                    if (version == 1)
                    {
                        if (authenticator instanceof ProtocolV1Authenticator)
                            authenticateV1(authenticator);
                        else
                            // DSE 3.x always uses SASL authentication backported from protocol v2
                            authenticateV2(authenticator);
                    }
                    else
                        authenticateV2(authenticator);
                    break;
                default:
                    throw defunct(new TransportException(address, String.format("Unexpected %s response message from server to a STARTUP message", response.type)));
            }
        } catch (BusyConnectionException e) {
            throw defunct(new DriverInternalError("Newly created connection should not be busy"));
        } catch (ExecutionException e) {
            throw defunct(new ConnectionException(address, String.format("Unexpected error during transport initialization (%s)", e.getCause()), e.getCause()));
        }
    }

    private UnsupportedProtocolVersionException unsupportedProtocolVersionException(int triedVersion) {
        logger.debug("Got unsupported protocol version error from {} for version {}", address, triedVersion);
        UnsupportedProtocolVersionException exc = new UnsupportedProtocolVersionException(address, triedVersion);
        defunct(new TransportException(address, "Cannot initialize transport", exc));
        return exc;
    }

    private void authenticateV1(Authenticator authenticator) throws ConnectionException, BusyConnectionException, ExecutionException, InterruptedException {
        Requests.Credentials creds = new Requests.Credentials(((ProtocolV1Authenticator)authenticator).getCredentials());
        Message.Response authResponse = write(creds).get();
        switch (authResponse.type) {
            case READY:
                break;
            case ERROR:
                throw defunct(new AuthenticationException(address, ((Responses.Error)authResponse).message));
            default:
                throw defunct(new TransportException(address, String.format("Unexpected %s response message from server to a CREDENTIALS message", authResponse.type)));
        }
    }

    private void authenticateV2(Authenticator authenticator) throws ConnectionException, BusyConnectionException, ExecutionException, InterruptedException {
        byte[] initialResponse = authenticator.initialResponse();
        if (null == initialResponse)
            initialResponse = EMPTY_BYTE_ARRAY;

        Message.Response authResponse = write(new Requests.AuthResponse(initialResponse)).get();
        waitForAuthCompletion(authResponse, authenticator);
    }

    private void waitForAuthCompletion(Message.Response authResponse, Authenticator authenticator) throws ConnectionException, BusyConnectionException, ExecutionException, InterruptedException {
        switch (authResponse.type) {
            case AUTH_SUCCESS:
                logger.trace("Authentication complete");
                authenticator.onAuthenticationSuccess(((Responses.AuthSuccess)authResponse).token);
                break;
            case AUTH_CHALLENGE:
                byte[] responseToServer = authenticator.evaluateChallenge(((Responses.AuthChallenge)authResponse).token);
                if (responseToServer == null) {
                    // If we generate a null response, then authentication has completed, return without
                    // sending a further response back to the server.
                    logger.trace("Authentication complete (No response to server)");
                    return;
                } else {
                    // Otherwise, send the challenge response back to the server
                    logger.trace("Sending Auth response to challenge");
                    waitForAuthCompletion(write(new Requests.AuthResponse(responseToServer)).get(), authenticator);
                }
                break;
            case ERROR:
                // This is not very nice, but we're trying to identify if we
                // attempted v2 auth against a server which only supports v1
                // The AIOOBE indicates that the server didn't recognise the
                // initial AuthResponse message
                String message = ((Responses.Error)authResponse).message;
                if (message.startsWith("java.lang.ArrayIndexOutOfBoundsException: 15"))
                    message = String.format("Cannot use authenticator %s with protocol version 1, "
                                  + "only plain text authentication is supported with this protocol version", authenticator);
                throw defunct(new AuthenticationException(address, message));
            default:
                throw defunct(new TransportException(address, String.format("Unexpected %s response message from server to authentication message", authResponse.type)));
        }
    }

    public boolean isDefunct() {
        return isDefunct;
    }

    public int maxAvailableStreams() {
        return dispatcher.streamIdHandler.maxAvailableStreams();
    }

    <E extends Exception> E defunct(E e) {
        if (logger.isDebugEnabled())
            logger.debug("Defuncting connection to " + address, e);
        isDefunct = true;

        ConnectionException ce = e instanceof ConnectionException
                               ? (ConnectionException)e
                               : new ConnectionException(address, "Connection problem", e);

        // We need to signal the connection failure before erroring out handlers to make
        // sure the "suspected" mechanism work as expected
        Host host = factory.manager.metadata.getHost(address);
        if (host != null) {
            boolean isDown = factory.manager.signalConnectionFailure(host, ce, host.wasJustAdded(), isInitialized);
            notifyOwnerWhenDefunct(isDown);
        }

        // Force the connection to close to make sure the future completes. Otherwise force() might never get called and
        // threads will wait on the future forever.
        // (this also errors out pending handlers)
        closeAsync().force();

        return e;
    }

    protected void notifyOwnerWhenDefunct(boolean hostIsDown) {
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
            long timeout = factory.getConnectTimeoutMillis();
            // Note: we quote the keyspace below, because the name is the one coming from Cassandra, so it's in the right case already
            Future future = write(new Requests.Query("USE \"" + keyspace + '"'));
            Message.Response response = Uninterruptibles.getUninterruptibly(future, timeout, TimeUnit.MILLISECONDS);
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
        } catch (TimeoutException e) {
            logger.warn(String.format("Timeout while setting keyspace on connection to %s. This should not happen but is not critical (it will retried)", address));
        } catch (BusyConnectionException e) {
            logger.warn(String.format("Tried to set the keyspace on busy connection to %s. This should not happen but is not critical (it will retried)", address));
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

    public ResponseHandler write(ResponseCallback callback) throws ConnectionException, BusyConnectionException {

        Message.Request request = callback.request();

        ResponseHandler handler = new ResponseHandler(this, callback);
        dispatcher.add(handler);
        request.setStreamId(handler.streamId);

        /*
         * We check for close/defunct *after* having set the handler because closing/defuncting
         * will set their flag and then error out handler if need. So, by doing the check after
         * having set the handler, we guarantee that even if we race with defunct/close, we may
         * never leave a handler that won't get an answer or be errored out.
         */
        if (isDefunct) {
            dispatcher.removeHandler(handler.streamId, true);
            throw new ConnectionException(address, "Write attempt on defunct connection");
        }

        if (isClosed()) {
            dispatcher.removeHandler(handler.streamId, true);
            throw new ConnectionException(address, "Connection has been closed");
        }

        logger.trace("[{}] writing request {}", name, request);
        writer.incrementAndGet();
        channel.write(request).addListener(writeHandler(request, handler));
        return handler;
    }

    private ChannelFutureListener writeHandler(final Message.Request request, final ResponseHandler handler) {
        return new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture writeFuture) {

                writer.decrementAndGet();

                if (!writeFuture.isSuccess()) {
                    logger.debug("[{}] Error writing request {}", name, request);
                    // Remove this handler from the dispatcher so it don't get notified of the error
                    // twice (we will fail that method already)
                    dispatcher.removeHandler(handler.streamId, true);

                    ConnectionException ce;
                    if (writeFuture.getCause() instanceof java.nio.channels.ClosedChannelException) {
                        ce = new TransportException(address, "Error writing: Closed channel");
                    } else {
                        ce = new TransportException(address, "Error writing", writeFuture.getCause());
                    }
                    handler.callback.onException(Connection.this, defunct(ce), System.nanoTime() - handler.startTime);
                } else {
                    logger.trace("[{}] request sent successfully", name);
                }
            }
        };
    }

    public boolean isClosed() {
        return closeFuture.get() != null;
    }

    public CloseFuture closeAsync() {

        ConnectionCloseFuture future = new ConnectionCloseFuture();
        if (!closeFuture.compareAndSet(null, future)) {
            // close had already been called, return the existing future
            return closeFuture.get();
        }

        logger.debug("[{}] closing connection", name);

        // New writes will be refused now that the future is setup.
        // We must now wait on the last ongoing queries to return, which will in turn trigger
        // the closing of the channel. However, if there no ongoing query, signal now.
        if (dispatcher.pending.isEmpty())
            future.force();
        return future;
    }

    @Override
    public String toString() {
        return String.format("Connection[%s, inFlight=%d, closed=%b]", name, inFlight.get(), isClosed());
    }

    public static class Factory {

        private final ExecutorService bossExecutor = Executors.newCachedThreadPool();
        private final ExecutorService workerExecutor = Executors.newCachedThreadPool();
        public final HashedWheelTimer timer = new HashedWheelTimer(new ThreadFactoryBuilder().setNameFormat("Timeouter-%d").build());

        private final ChannelFactory channelFactory = new NioClientSocketChannelFactory(bossExecutor, workerExecutor);
        private final ChannelGroup allChannels = new DefaultChannelGroup();

        private final ConcurrentMap<Host, AtomicInteger> idGenerators = new ConcurrentHashMap<Host, AtomicInteger>();
        public final DefaultResponseHandler defaultHandler;
        final Cluster.Manager manager;
        public final Configuration configuration;

        public final AuthProvider authProvider;
        private volatile boolean isShutdown;

        volatile int protocolVersion;

        Factory(Cluster.Manager manager, Configuration configuration) {
            this.defaultHandler = manager;
            this.manager = manager;
            this.configuration = configuration;
            this.authProvider = configuration.getProtocolOptions().getAuthProvider();
            this.protocolVersion = configuration.getProtocolOptions().initialProtocolVersion;
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
        public Connection open(Host host) throws ConnectionException, InterruptedException, UnsupportedProtocolVersionException {
            InetSocketAddress address = host.getSocketAddress();

            if (isShutdown)
                throw new ConnectionException(address, "Connection factory is shut down");

            String name = address.toString() + '-' + getIdGenerator(host).getAndIncrement();
            return new Connection(name, address, this);
        }

        /**
         * Same as open, but associate the created connection to the provided connection pool.
         */
        public PooledConnection open(HostConnectionPool pool) throws ConnectionException, InterruptedException, UnsupportedProtocolVersionException {
            InetSocketAddress address = pool.host.getSocketAddress();

            if (isShutdown)
                throw new ConnectionException(address, "Connection factory is shut down");

            String name = address.toString() + '-' + getIdGenerator(pool.host).getAndIncrement();
            return new PooledConnection(name, address, this, pool);
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

        public long getConnectTimeoutMillis() {
            return configuration.getSocketOptions().getConnectTimeoutMillis();
        }

        public long getReadTimeoutMillis() {
            return configuration.getSocketOptions().getReadTimeoutMillis();
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
            // Make sure we skip creating connection from now on.
            isShutdown = true;

            // All channels should be closed already, we call this just to be sure. And we know
            // we're not on an I/O thread or anything, so just call await.
            allChannels.close().awaitUninterruptibly();

            // This will call shutdownNow on the boss and worker executor. Since this is called
            // only once all connection have been individually closed, it's fine.
            channelFactory.releaseExternalResources();
            timer.stop();
        }
    }

    private class Dispatcher extends SimpleChannelUpstreamHandler {

        public final StreamIdGenerator streamIdHandler = new StreamIdGenerator();
        private final ConcurrentMap<Integer, ResponseHandler> pending = new ConcurrentHashMap<Integer, ResponseHandler>();

        public void add(ResponseHandler handler) {
            ResponseHandler old = pending.put(handler.streamId, handler);
            assert old == null;
        }

        public void removeHandler(int streamId, boolean releaseStreamId) {

            // If we don't release the ID, mark first so that we can rely later on the fact that if
            // we receive a response for an ID with no handler, it's that this ID has been marked.
            if (!releaseStreamId)
                streamIdHandler.mark(streamId);

            ResponseHandler handler = pending.remove(streamId);
            if (handler != null)
                handler.cancelTimeout();

            if (releaseStreamId)
                streamIdHandler.release(streamId);
        }

        @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
            if (!(e.getMessage() instanceof Message.Response)) {
                String msg = asDebugString(e.getMessage());
                logger.error("[{}] Received unexpected message: {}", name, msg);
                defunct(new TransportException(address, "Unexpected message received: " + msg));
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
                    /**
                     * During normal operation, we should not receive responses for which we don't have a handler. There is
                     * two cases however where this can happen:
                     *   1) The connection has been defuncted due to some internal error and we've raced between removing the
                     *      handler and actually closing the connection; since the original error has been logged, we're fine
                     *      ignoring this completely.
                     *   2) This request has timed out. In that case, we've already switched to another host (or errored out
                     *      to the user). So log it for debugging purpose, but it's fine ignoring otherwise.
                     */
                    streamIdHandler.unmark(streamId);
                    if (logger.isDebugEnabled())
                        logger.debug("[{}] Response received on stream {} but no handler set anymore (either the request has "
                                   + "timed out or it was closed due to another error). Received message is {}", name, streamId, asDebugString(response));
                    return;
                }
                handler.cancelTimeout();
                handler.callback.onSet(Connection.this, response, System.nanoTime() - handler.startTime);

                // If we happen to be closed and we're the last outstanding request, we need to signal the close future
                // (note: this is racy as the signaling can be called more than once, but that's not a problem)
                if (isClosed() && pending.isEmpty())
                    closeFuture.get().force();
            }
        }

        // Make sure we don't print huge responses in debug/error logs.
        private String asDebugString(Object obj) {
            if (obj == null)
                return "null";

            String msg = obj.toString();
            if (msg.length() < 500)
                return msg;

            return msg.substring(0, 500) + "... [message of size " + msg.length() + " truncated]";
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
            if (logger.isDebugEnabled())
                logger.debug(String.format("[%s] connection error", name), e.getCause());

            // Ignore exception while writing, this will be handled by write() directly
            if (writer.get() > 0)
                return;

            defunct(new TransportException(address, String.format("Unexpected exception triggered (%s)", e.getCause()), e.getCause()));
        }

        public void errorOutAllHandler(ConnectionException ce) {
            Iterator<ResponseHandler> iter = pending.values().iterator();
            while (iter.hasNext())
            {
                ResponseHandler handler = iter.next();
                handler.callback.onException(Connection.this, ce, System.nanoTime() - handler.startTime);
                iter.remove();
            }
        }

        @Override
        public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) {
            // If we've closed the channel client side then we don't really want to defunct the connection, but
            // if there is remaining thread waiting on us, we still want to wake them up
            if (!isInitialized || isClosed())
                errorOutAllHandler(new TransportException(address, "Channel has been closed"));
            else
                defunct(new TransportException(address, "Channel has been closed"));
        }
    }

    private class ConnectionCloseFuture extends CloseFuture {

        @Override
        public ConnectionCloseFuture force() {
            // Note: we must not call releaseExternalResources on the bootstrap, because this shutdown the executors, which are shared

            // This method can be thrown during Connection ctor, at which point channel is not yet set. This is ok.
            if (channel == null) {
                set(null);
                return this;
            }

            // We're going to close this channel. If anyone is waiting on that connection, we should defunct it otherwise it'll wait
            // forever. In general this won't happen since we get there only when all ongoing query are done, but this can happen
            // if the shutdown is forced. This is a no-op if there is no handler set anymore.
            dispatcher.errorOutAllHandler(new TransportException(address, "Connection has been closed"));

            ChannelFuture future = channel.close();
            future.addListener(new ChannelFutureListener() {
                public void operationComplete(ChannelFuture future) {
                    if (future.getCause() != null)
                        ConnectionCloseFuture.this.setException(future.getCause());
                    else
                        ConnectionCloseFuture.this.set(null);
                }
            });
            return this;
        }
    }

    static class Future extends AbstractFuture<Message.Response> implements RequestHandler.Callback {

        private final Message.Request request;
        private volatile InetSocketAddress address;

        public Future(Message.Request request) {
            this.request = request;
        }

        @Override
        public void register(RequestHandler handler) {
            // noop, we don't care about the handler here so far
        }

        @Override
        public Message.Request request() {
            return request;
        }

        @Override
        public void onSet(Connection connection, Message.Response response, ExecutionInfo info, Statement statement, long latency) {
            onSet(connection, response, latency);
        }

        @Override
        public void onSet(Connection connection, Message.Response response, long latency) {
            this.address = connection.address;
            super.set(response);
        }

        @Override
        public void onException(Connection connection, Exception exception, long latency) {
            // If all nodes are down, we will get a null connection here. This is fine, if we have
            // an exception, consumers shouldn't assume the address is not null.
            if (connection != null)
                this.address = connection.address;
            super.setException(exception);
        }

        @Override
        public void onTimeout(Connection connection, long latency) {
            assert connection != null; // We always timeout on a specific connection, so this shouldn't be null
            this.address = connection.address;
            super.setException(new ConnectionException(connection.address, "Operation timed out"));
        }

        public InetSocketAddress getAddress() {
            return address;
        }
    }

    interface ResponseCallback {
        public Message.Request request();
        public void onSet(Connection connection, Message.Response response, long latency);
        public void onException(Connection connection, Exception exception, long latency);
        public void onTimeout(Connection connection, long latency);
    }

    static class ResponseHandler {

        public final Connection connection;
        public final int streamId;
        public final ResponseCallback callback;

        private final Timeout timeout;
        private final long startTime;

        public ResponseHandler(Connection connection, ResponseCallback callback) throws BusyConnectionException {
            this.connection = connection;
            this.streamId = connection.dispatcher.streamIdHandler.next();
            this.callback = callback;

            long timeoutMs = connection.factory.getReadTimeoutMillis();
            this.timeout = timeoutMs <= 0 ? null : connection.factory.timer.newTimeout(onTimeoutTask(), timeoutMs, TimeUnit.MILLISECONDS);

            this.startTime = System.nanoTime();
        }

        void cancelTimeout() {
            if (timeout != null)
                timeout.cancel();
        }

        public void cancelHandler() {
            // We haven't really received a response: we want to remove the handle because we gave up on that
            // request and there is no point in holding the handler, but we don't release the streamId. If we
            // were, a new request could reuse that ID but get the answer to the request we just gave up on instead
            // of its own answer, and we would have no way to detect that.
            connection.dispatcher.removeHandler(streamId, false);
        }

        private TimerTask onTimeoutTask() {
            return new TimerTask() {
                @Override
                public void run(Timeout timeout) {
                    callback.onTimeout(connection, System.nanoTime() - startTime);
                    cancelHandler();
                }
            };
        }
    }

    public interface DefaultResponseHandler {
        public void handle(Message.Response response);
    }

    private static class PipelineFactory implements ChannelPipelineFactory {
        // Stateless handlers
        private static final Message.ProtocolDecoder messageDecoder = new Message.ProtocolDecoder();
        private static final Message.ProtocolEncoder messageEncoderV1 = new Message.ProtocolEncoder(1);
        private static final Message.ProtocolEncoder messageEncoderV2 = new Message.ProtocolEncoder(2);
        private static final Frame.Encoder frameEncoder = new Frame.Encoder();

        private final int protocolVersion;
        private final Connection connection;
        private final FrameCompressor compressor;
        private final SSLOptions sslOptions;

        public PipelineFactory(Connection connection, int protocolVersion, FrameCompressor compressor, SSLOptions sslOptions) {
            this.connection = connection;
            this.protocolVersion = protocolVersion;
            this.compressor = compressor;
            this.sslOptions = sslOptions;
        }

        @Override
        public ChannelPipeline getPipeline() throws Exception {
            ChannelPipeline pipeline = Channels.pipeline();

            if (sslOptions != null) {
                SSLEngine engine = sslOptions.context.createSSLEngine();
                engine.setUseClientMode(true);
                engine.setEnabledCipherSuites(sslOptions.cipherSuites);
                SslHandler handler = new SslHandler(engine);
                handler.setCloseOnSSLException(true);
                pipeline.addLast("ssl", handler);
            }

            //pipeline.addLast("debug", new LoggingHandler(InternalLogLevel.INFO));

            pipeline.addLast("frameDecoder", new Frame.Decoder());
            pipeline.addLast("frameEncoder", frameEncoder);

            if (compressor != null) {
                pipeline.addLast("frameDecompressor", new Frame.Decompressor(compressor));
                pipeline.addLast("frameCompressor", new Frame.Compressor(compressor));
            }

            pipeline.addLast("messageDecoder", messageDecoder);
            pipeline.addLast("messageEncoder", protocolVersion == 1 ? messageEncoderV1 : messageEncoderV2);

            pipeline.addLast("dispatcher", connection.dispatcher);

            return pipeline;
        }
    }
}
