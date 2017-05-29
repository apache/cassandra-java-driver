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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;

import static com.datastax.driver.core.CreateCCM.TestMode.PER_METHOD;
import static com.datastax.driver.core.StateListenerTest.TestListener.Event.*;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;

@CreateCCM(PER_METHOD)
@CCMConfig(dirtiesContext = true, createSession = false)
public class StateListenerTest extends CCMTestsSupport {

    private static final Logger logger = LoggerFactory.getLogger(StateListenerTest.class);

    @Test(groups = "long")
    public void should_receive_events_when_node_states_change() throws InterruptedException {
        TestListener listener = new TestListener();
        cluster().register(listener);

        listener.setExpectedEvent(ADD);
        ccm().add(2);
        ccm().start(2);
        listener.waitForEvent();

        listener.setExpectedEvent(DOWN);
        ccm().forceStop(1);
        listener.waitForEvent();

        listener.setExpectedEvent(UP);
        ccm().start(1);
        listener.waitForEvent();

        listener.setExpectedEvent(REMOVE);
        ccm().decommission(2);
        listener.waitForEvent();
    }

    static class TestListener implements Host.StateListener {
        enum Event {ADD, UP, SUSPECTED, DOWN, REMOVE}

        volatile CountDownLatch latch;
        volatile Event expectedEvent;
        volatile Event actualEvent;

        void setExpectedEvent(Event expectedEvent) {
            logger.debug("Set expected event {}", expectedEvent);
            this.expectedEvent = expectedEvent;
            latch = new CountDownLatch(1);
        }

        void waitForEvent() throws InterruptedException {
            assertThat(latch.await(2, MINUTES))
                    .as("Timed out waiting for event " + expectedEvent)
                    .isTrue();
            assertThat(actualEvent).isEqualTo(expectedEvent);
        }

        private void reportActualEvent(Event event) {
            if (latch.getCount() == 0) {
                // TODO this actually happens because C* sends REMOVE/ADD/REMOVE on a remove
                logger.error("Was not waiting for an event but got {} (this should eventually be fixed by JAVA-657)", event);
                return;
            }
            logger.debug("Got event {}", event);
            actualEvent = event;
            latch.countDown();
        }

        @Override
        public void onAdd(Host host) {
            reportActualEvent(ADD);
        }

        @Override
        public void onUp(Host host) {
            reportActualEvent(UP);
        }

        @Override
        public void onDown(Host host) {
            reportActualEvent(DOWN);
        }

        @Override
        public void onRemove(Host host) {
            reportActualEvent(REMOVE);
        }

        @Override
        public void onRegister(Cluster cluster) {
        }

        @Override
        public void onUnregister(Cluster cluster) {
        }
    }
}
