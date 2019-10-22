## Developer docs

This section explains how driver internals work. The intended audience is:

* driver developers and contributors;
* framework authors, or architects who want to write advanced customizations and integrations. 

Most of this material will involve "internal" packages; see [API conventions](../api_conventions/)
for more explanations.

We recommend reading about the [common infrastructure](common/) first. Then the documentation goes
from lowest to highest level:

* [Native protocol layer](native_protocol/): binary encoding of the TCP payloads;
* [Netty pipeline](netty_pipeline/): networking and low-level stream management;
* [Request execution](request_execution/): higher-level handling of user requests and responses;
* [Administrative tasks](admin/): everything else (cluster state and metadata).

If you're reading this on GitHub, the `.nav` file in each directory contains a suggested order.