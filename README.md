# DECODE

This project explores building a novel object storage and content delivery
system called _DECODE_. Its initial prototype was developed for Princeton
University's COS 518 course on Advanced Computer Systems.

## Disclaimer

This project is currently in a non-usable, unfinished state. While we encourage
you to take a look, it's not something you should use for production or even
test workloads. The current state of it is at best an initial proof of concept,
so beware!

## Motivation

Current content delivery systems require a reliable primary storage from which
to serve non-cached content. These systems in turn transparently serve cached
data to clients, without contacting the primary storage system. On a global
scale, they achieve performance and resilience by creating full copies of cached
objects and distributing them throughout a network of nodes.

Fundamentally, this implies that two independent systems need to be maintained:
the primary storage system, as well as the content distribution system. A
failure in either system can impede a client's ability to fetch a requested
file. For files which are not currently cached by the CDN, requests are proxied
to the primary storage system, which incurs high tail latencies.

Making this problem worse, an ever-growing and diverse user-base on the Internet
can translate to unpredictable demand of certain objects (e.g. viral video
content), among a large pool of stored objects. This necessitates storage and
distribution systems which can cost-efficiently and reliably store vast amounts
of data, provide low-latency access to data, while also reacting to and scaling
with increasing popularity of stored objects.

We hope that our project can improve on the current situation by architecting a
system that is both resilient and efficient in storing arbitrary objects, and
can further scale with increasing demand. Specifically, DECODE uses a
centralized control plane orchestrating a fleet of storage and content-delivery
nodes. It further employs erasure coding (Reed-Solomon codes) to provide
efficient fault tolerance in light of node failures


## Authors

- Leon Schuermann <lschuermann@princeton.edu>
- Fengchen Gong <gongf@princeton.edu>
