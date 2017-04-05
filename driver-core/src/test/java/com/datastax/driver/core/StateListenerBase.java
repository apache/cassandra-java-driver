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

public class StateListenerBase implements Host.StateListener {
    @Override
    public void onAdd(Host host) {
    }

    @Override
    public void onUp(Host host) {
    }

    @Override
    public void onDown(Host host) {
    }

    @Override
    public void onRemove(Host host) {
    }

    @Override
    public void onRegister(Cluster cluster) {

    }

    @Override
    public void onUnregister(Cluster cluster) {

    }

}
