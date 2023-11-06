/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Allows quick manual tests from the Scala console:
 *
 *   cd core/
 *   mvn scala:console
 *
 * The script below is run at init, then you can do `val cluster = builder.build()` and play with
 * it.
 *
 * Note: on MacOS, the Scala plugin seems to break the terminal if you exit the console with `:q`.
 * Use Ctrl+C instead.
 */
import com.datastax.oss.driver.api.core._
import com.datastax.oss.driver.internal.core.metadata.TopologyEvent
import com.datastax.oss.driver.internal.core.context.InternalDriverContext
import java.net.InetSocketAddress

import CqlSession

// Heartbeat logs every 30 seconds are annoying in the console, raise the interval
System.setProperty("datastax-java-driver.advanced.heartbeat.interval", "1 hour")

val address1 = new InetSocketAddress("127.0.0.1", 9042)
val address2 = new InetSocketAddress("127.0.0.2", 9042)
val address3 = new InetSocketAddress("127.0.0.3", 9042)
val address4 = new InetSocketAddress("127.0.0.4", 9042)
val address5 = new InetSocketAddress("127.0.0.5", 9042)
val address6 = new InetSocketAddress("127.0.0.6", 9042)

val builder = CqlSession.builder().addContactPoint(address1)

println("********************************************")
println("*   To start a driver instance, run:       *")
println("*   implicit val session = builder.build   *")
println("********************************************")

def fire(event: AnyRef)(implicit session: CqlSession): Unit = {
  session.getContext.asInstanceOf[InternalDriverContext].getEventBus().fire(event)
}
