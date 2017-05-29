/*
 * Copyright (C) 2012-2017 DataStax Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.driver.core;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.fail;

/**
 * Fake Cassandra host that will cause a given error when the driver tries to connect to it.
 */
public class FakeHost {
    public enum Behavior {THROWING_CONNECT_TIMEOUTS, THROWING_OPERATION_TIMEOUTS}

    final String address;
    private final int port;
    private final Behavior behavior;
    private final ExecutorService executor;

    FakeHost(String address, int port, Behavior behavior) {
        this.address = address;
        this.port = port;
        this.behavior = behavior;
        this.executor = Executors.newSingleThreadExecutor();
    }

    public void start() {
        executor.execute(new AcceptClientAndWait(address, port, behavior));
    }

    public void stop() {
        executor.shutdownNow();
    }

    private static class AcceptClientAndWait implements Runnable {

        private final String address;
        private final int port;
        private final Behavior behavior;

        public AcceptClientAndWait(String address, int port, Behavior behavior) {
            this.address = address;
            this.port = port;
            this.behavior = behavior;
        }

        @Override
        public void run() {
            ServerSocket server = null;
            Socket client = null;
            try {
                InetAddress bindAddress = InetAddress.getByName(address);
                int backlog = (behavior == Behavior.THROWING_CONNECT_TIMEOUTS)
                        ? 1
                        : -1; // default
                server = new ServerSocket(port, backlog, bindAddress);

                if (behavior == Behavior.THROWING_CONNECT_TIMEOUTS) {
                    // fill backlog queue
                    client = new Socket();
                    client.connect(server.getLocalSocketAddress());
                }
                TimeUnit.MINUTES.sleep(10);
                fail("Mock host wasn't expected to live more than 10 minutes");
            } catch (IOException e) {
                fail("Unexpected I/O exception", e);
            } catch (InterruptedException e) {
                // interruption is the expected way to stop this runnable, exit
                try {
                    if (client != null)
                        client.close();
                    server.close();
                } catch (IOException e1) {
                    fail("Unexpected error while closing sockets", e);
                }

            }
        }
    }
}
