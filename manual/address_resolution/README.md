## Address resolution

Each node in the Cassandra cluster is uniquely identified by an IP address that the driver will use to establish
connections.

* for contact points, these are provided as part of configuring the `Cluster` object;
* for other nodes, addresses will be discovered dynamically, either by inspecting `system.peers` on already connected
  nodes, or via push notifications received from the [control host](../control_connection) when new nodes are discovered
  by gossip.


### Cassandra-side configuration

The address that each Cassandra node shares with clients is the **broadcast RPC address**; it is controlled by various
properties in [cassandra.yaml]:

* [rpc_address] or [rpc_interface] is the address that the Cassandra process *binds to*. You must set one or the other,
  not both (for more details, see the inline comments in the default `cassandra.yaml` that came with your installation);
* [broadcast_rpc_address] \(introduced in Cassandra 2.1) is the address to share with clients, if it is different than
  the previous one (the reason for having a separate property is if the bind address is not public to clients, because
  there is a router in between).

If `broadcast_rpc_address` is not set, it defaults to `rpc_address`/`rpc_interface`. If `rpc_address`/`rpc_interface`
is 0.0.0.0 (all interfaces), then `broadcast_rpc_address` *must* be set.

If you're not sure which address a Cassandra node is broadcasting, launch cqlsh locally on the node, execute the
following query and take node of the result:

```
cqlsh> select broadcast_address from system.local;

 broadcast_address
-------------------
         172.1.2.3
```

Then connect to *another* node in the cluster and run the following query, injecting the previous result:

```
cqlsh> select rpc_address from system.peers where peer = '172.1.2.3';

 rpc_address
-------------
     1.2.3.4
```

That last result is the broadcast RPC address. Ensure that it is accessible from the client machine where the driver
will run.


### Driver-side address translation

Sometimes it's not possible for Cassandra nodes to broadcast addresses that will work for each and every client; for
instance, they might broadcast private IPs because most clients are in the same network, but a particular client could
be on another network and go through a router.

For such cases, you can register a driver-side component that will perform additional address translation. Write a class
that implements [AddressTranslater] and register an instance with your `Cluster`:

```java
public class MyAddressTranslater implements AddressTranslater {
    public InetSocketAddress translate(InetSocketAddress address) {
        ... // your custom translation logic
    }
}

Cluster cluster = Cluster.builder()
    .addContactPoint("1.2.3.4")
    .withAddressTranslater(new MyAddressTranslater())
    .build();
```

Notes:

* the contact points provided while creating the `Cluster` are not translated, only addresses
  retrieved from or sent by Cassandra nodes are;
* you might want to implement [CloseableAddressTranslater] if your
  implementation has state that should be cleaned up when the `Cluster`
  shuts down. This is provided as a separate interface for historical
  reasons, the `close()` method was merged in `AddressTranslater` in 3.0.


### EC2 multi-region

If you deploy both Cassandra and client applications on Amazon EC2, and your cluster spans multiple regions, you'll have
to configure your Cassandra nodes to broadcast public RPC addresses.

However, this is not always the most cost-effective: if a client and a node are in the same region, it would be cheaper
to connect over the private IP. Ideally, you'd want to pick the best address in each case.

The driver provides an address translator that does just that: [EC2MultiRegionAddressTranslater].

```java
Cluster cluster = Cluster.builder()
    .addContactPoint("1.2.3.4")
    .withAddressTranslater(new EC2MultiRegionAddressTranslater())
    .build();
```

With this configuration, you keep broadcasting public RPC addresses. But each time the driver connects to a new
Cassandra node:

* if the node is *in the same EC2 region*, the public IP will be translated to the intra-region private IP;
* otherwise, it will not be translated.

(To achieve this, `EC2MultiRegionAddressTranslater` performs a reverse DNS lookup of the origin address, to find the
domain name of the target instance. Then it performs a forward DNS lookup of the domain name; the EC2 DNS does the
private/public switch automatically based on location).



[AddressTranslater]:               http://docs.datastax.com/en/drivers/java/2.1/com/datastax/driver/core/policies/AddressTranslater.html
[CloseableAddressTranslater]:      http://docs.datastax.com/en/drivers/java/2.1/com/datastax/driver/core/policies/CloseableAddressTranslater.html
[EC2MultiRegionAddressTranslater]: http://docs.datastax.com/en/drivers/java/2.1/com/datastax/driver/core/policies/EC2MultiRegionAddressTranslater.html

[cassandra.yaml]:        https://docs.datastax.com/en/cassandra/2.1/cassandra/configuration/configCassandra_yaml_r.html
[rpc_address]:           https://docs.datastax.com/en/cassandra/2.1/cassandra/configuration/configCassandra_yaml_r.html?scroll=reference_ds_qfg_n1r_1k__rpc_address
[rpc_interface]:         https://docs.datastax.com/en/cassandra/2.1/cassandra/configuration/configCassandra_yaml_r.html?scroll=reference_ds_qfg_n1r_1k__rpc_interface
[broadcast_rpc_address]: https://docs.datastax.com/en/cassandra/2.1/cassandra/configuration/configCassandra_yaml_r.html#reference_ds_qfg_n1r_1k__broadcast_rpc_address
