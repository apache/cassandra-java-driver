<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

## Address resolution

### Quick overview

The driver uses `system.peers.rpc-address` to connect to newly discovered nodes. For special network
topologies, an address translation component can be plugged in.

* `advanced.address-translator` in the configuration.
* none by default. Also available: EC2-specific (for deployments that span multiple regions), or
  write your own.

-----

Each node in the Cassandra cluster is uniquely identified by an IP address that the driver will use
to establish connections.

* for contact points, these are provided as part of configuring the `CqlSession` object;
* for other nodes, addresses will be discovered dynamically, either by inspecting `system.peers` on
  already connected nodes, or via push notifications received on the control connection when new
  nodes are discovered by gossip.


### Cassandra-side configuration

The address that each Cassandra node shares with clients is the **broadcast RPC address**; it is
controlled by various properties in [cassandra.yaml]:

* [rpc_address] or [rpc_interface] is the address that the Cassandra process *binds to*. You must
  set one or the other, not both (for more details, see the inline comments in the default
  `cassandra.yaml` that came with your installation);
* [broadcast_rpc_address] \(introduced in Cassandra 2.1) is the address to share with clients, if it
  is different than the previous one (the reason for having a separate property is if the bind
  address is not public to clients, because there is a router in between).

If `broadcast_rpc_address` is not set, it defaults to `rpc_address`/`rpc_interface`. If
`rpc_address`/`rpc_interface` is 0.0.0.0 (all interfaces), then `broadcast_rpc_address` *must* be
set.

If you're not sure which address a Cassandra node is broadcasting, launch cqlsh locally on the node,
execute the following query and take node of the result:

```
cqlsh> select broadcast_address from system.local;

 broadcast_address
-------------------
         172.1.2.3
```

Then connect to *another* node in the cluster and run the following query, injecting the previous
result:

```
cqlsh> select rpc_address from system.peers where peer = '172.1.2.3';

 rpc_address
-------------
     1.2.3.4
```

That last result is the broadcast RPC address. Ensure that it is accessible from the client machine
where the driver will run.


### Driver-side address translation

Sometimes it's not possible for Cassandra nodes to broadcast addresses that will work for each and
every client; for instance, they might broadcast private IPs because most clients are in the same
network, but a particular client could be on another network and go through a router.

For such cases, you can register a driver-side component that will perform additional address
translation. Write a class that implements [AddressTranslator] with the following constructor:

```java
public class MyAddressTranslator implements AddressTranslator {

  public PassThroughAddressTranslator(DriverContext context, DriverOption configRoot) {
    // retrieve any required dependency or extra configuration option, otherwise can stay empty
  }

  @Override
  public InetSocketAddress translate(InetSocketAddress address) {
    // your custom translation logic
  }

  @Override
  public void close() {
    // free any resources if needed, otherwise can stay empty
  }
}
```

Then reference this class from the [configuration](../configuration/):

```
datastax-java-driver.advanced.address-translator.class = com.mycompany.MyAddressTranslator
```

Note: the contact points provided while creating the `CqlSession` are not translated, only addresses
retrieved from or sent by Cassandra nodes are.

### EC2 multi-region

If you deploy both Cassandra and client applications on Amazon EC2, and your cluster spans multiple regions, you'll have
to configure your Cassandra nodes to broadcast public RPC addresses.

However, this is not always the most cost-effective: if a client and a node are in the same region, it would be cheaper
to connect over the private IP. Ideally, you'd want to pick the best address in each case.

The driver provides `Ec2MultiRegionAddressTranslator` which does exactly that.  To use it, specify the following in
the [configuration](../configuration/):

```
datastax-java-driver.advanced.address-translator.class = Ec2MultiRegionAddressTranslator
```

With this configuration, you keep broadcasting public RPC addresses. But each time the driver connects to a new
Cassandra node:

* if the node is *in the same EC2 region*, the public IP will be translated to the intra-region private IP;
* otherwise, it will not be translated.

(To achieve this, `Ec2MultiRegionAddressTranslator` performs a reverse DNS lookup of the origin address, to find the
domain name of the target instance. Then it performs a forward DNS lookup of the domain name; the EC2 DNS does the
private/public switch automatically based on location).

[AddressTranslator]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/addresstranslation/AddressTranslator.html

[cassandra.yaml]:        https://docs.datastax.com/en/cassandra/3.x/cassandra/configuration/configCassandra_yaml.html
[rpc_address]:           https://docs.datastax.com/en/cassandra/3.x/cassandra/configuration/configCassandra_yaml.html?scroll=configCassandra_yaml__rpc_address
[rpc_interface]:         https://docs.datastax.com/en/cassandra/3.x/cassandra/configuration/configCassandra_yaml.html?scroll=configCassandra_yaml__rpc_interface
[broadcast_rpc_address]: https://docs.datastax.com/en/cassandra/3.x/cassandra/configuration/configCassandra_yaml.html?scroll=configCassandra_yaml__broadcast_rpc_address
