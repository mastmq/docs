# Delivery guarantees

What mast promises a client, hop by hop, and where the promise is weaker than an MQTT user would assume. Everything here was read off the code and checked against tests rather than inferred from the design; where something is not true it says so.

**The short version.** QoS 1 and 2 hold across a cluster, not only within a node: the node a publisher is connected to acknowledges a QoS 1 or 2 message only once it is stored on a replicated JetStream stream, and every node reads that stream. A node cut off from the core for a moment catches up when it reconnects. QoS 0 is at-most-once everywhere, which is the contract, and never touches storage. Two gaps remain, both below: shared subscriptions across nodes, and a node dying after it has taken a message.

| | Guarantee |
| --- | --- |
| QoS 0, anywhere | At-most-once, which is the contract |
| QoS 1 / 2, publisher and subscriber on the same node | Full — at-least-once and exactly-once respectively |
| QoS 1 / 2, across nodes | **At-least-once to every node with a subscriber.** Stored, replicated, then acknowledged; survives a leaf reconnect or a core restart shorter than `fabric.durable_max_age` |
| QoS 2 across nodes, through a node crash | At-least-once. Exactly-once holds until a node dies holding a message it had not finished delivering |
| Shared subscriptions across nodes | **Best-effort.** Exactly one group member, local-first rather than round-robin, and still on core NATS ([below](#what-is-still-best-effort)) |
| Retained messages | Durable and cluster-wide. Stored in a JetStream KV bucket, Raft-replicated |
| Persistent sessions | Durable. Subscriptions and the QoS 1/2 backlog live in the shared key-value buckets, so a session survives a restart and follows a device to another node |

## Three hops, not one

An MQTT message in a mast cluster crosses up to three boundaries, and only the middle one is mast's own invention.

1. **Publisher to its ingress node.** Ordinary MQTT. mochi runs the QoS state machine — PUBACK for QoS 1, the PUBREC/PUBREL/PUBCOMP handshake for QoS 2.
2. **Ingress node to the node owning a subscriber.** For QoS 0, core NATS, fire-and-forget. For QoS 1 and 2, a replicated JetStream stream.
3. **Owning node to the subscriber.** Ordinary MQTT again, with its own QoS negotiated per subscription and downgraded to the minimum of what the publisher sent and what the subscriber asked for.

Hops 1 and 3 are what the MQTT specification defines. The spec says nothing about hop 2, because it does not contemplate a broker made of several machines, and hop 2 is where a clustered broker earns or loses its guarantees.

## How hop 2 works

**QoS 0** is published on core NATS and nothing else. A non-retained QoS 0 publish performs zero storage operations and adds zero round trips. Core NATS discards rather than buffers, so a QoS 0 message can be lost to a slow consumer or a reconnect window — which QoS 0 permits.

**QoS 1 and 2** are written to one stream, `mast_qos`, before the ingress node sends its PUBACK or PUBREC. The write returns once JetStream has replicated it (`core.replicas`, three in a real core tier). Every node reads the stream through **one consumer of its own** — one per node, never one per subscription or session, which is the design mast exists to avoid — and hands each message to its local subscribers.

That consumer acknowledges what it has delivered, and JetStream sends again whatever it has not heard back about within three seconds. That is what carries a message across a node being cut off: while an edge's link to the core is down, what the core sends it is lost in transit, and the edge's own connection never notices. The redelivery arrives once the link is back, and the node recognises and drops any copy it already delivered by the message id every publish carries. The regression test for this cuts an edge off, publishes twenty QoS 1 messages elsewhere, and requires exactly twenty to arrive.

The stream keeps a message for `fabric.durable_max_age`, five minutes by default. That is how long a node can be unreachable and still catch up. Nothing acknowledges messages off the stream as a whole, so age is what removes them.

**When the stream cannot store a message, the publish is refused.** An MQTT 5 client receives a failing reason code on its PUBACK or PUBREC. An MQTT 3 client has no negative acknowledgement to receive, so its connection is closed, which is what makes it reconnect and send the unacknowledged message again. Each refusal counts on `mast_durable_publish_failures_total`. Refusing is the point: the alternative is acknowledging a message the broker cannot keep.

## What it costs

Measured on one laptop, a core and two edges, every message crossing between edges to ten subscribers ([bench RESULTS.md](https://github.com/mastmq/bench/blob/main/RESULTS.md)):

| QoS 1, cross-node | p50 | p99 |
| --- | --- | --- |
| 2,000 publishes/s, core NATS only (before) | 0.53ms | 2.3ms |
| 2,000 publishes/s, durable stream | 0.64ms | 1.7ms |
| 10,000 publishes/s, core NATS only (before) | 0.94ms | 23ms |
| 10,000 publishes/s, durable stream | 12.8ms | 130ms |

At moderate rates the guarantee is close to free. At high rates it costs tens of milliseconds, because every QoS 1 or 2 message is a replicated write and every node reads all of them — a node with no interest in a message still receives it and discards it. Numbers from one machine are for comparing designs, not for sizing a cluster.

A publisher also waits for the replicated write before its PUBACK, so a single MQTT connection publishing QoS 1 is paced by that write. That is the same trade every broker that replicates before acknowledging makes.

## What is still best-effort

**Shared subscriptions across nodes.** A `$share` group is served by a NATS queue group on core NATS, so exactly one member receives each message, but the hop to a member on another node is at-most-once. Serving groups from the stream needs a consumer per group rather than per node, which is a different design and not built. Within a node, a shared subscription at QoS 1 or 2 is full QoS.

**A node dying after it has taken a message.** Once a node has handed a message to its MQTT layer and acknowledged it on the stream, delivery to the subscriber is that node's MQTT state, held in memory. If the node dies before its subscriber acknowledges, the message is gone — unless the subscriber's session is persistent and it was absent at the time, in which case the message was already in the offline queue in the shared buckets. The same property turns QoS 2 across a node crash into at-least-once.

**A node cut off for longer than `fabric.durable_max_age`.** What the stream no longer holds, it cannot resend.

**`fabric.durable_qos = false`.** Turns all of this off: QoS 1 and 2 then cross nodes on core NATS, at-most-once, exactly as mast did before. It exists for rollouts and for measurement.

## Watching it

| Metric | Means |
| --- | --- |
| `mast_durable_publish_failures_total` | QoS 1/2 publishes refused because the stream would not store them. Any increase is publishers being told to retry |
| `mast_nats_slow_consumers_total` | a node discarded core NATS messages. Now QoS 0 and shared-group copies only |
| `mast_nats_disconnects_total` / `mast_nats_reconnects_total` | a node's fabric connection dropping and coming back |

A node that cannot reach the stream at startup logs that cross-node QoS 1 and 2 are best-effort on it, and behaves exactly as mast did before this existed.

## Is this unusual?

MQTT defines quality of service between a client and a broker, one hop at a time. No broker offers end-to-end assurance from publisher to subscriber, and every broker acknowledges a QoS 2 publish before any subscriber has seen it. What differs between clustered brokers is how reliable the internal hop is. mast now replicates before acknowledging, which gives up some throughput at high rates for a guarantee that survives the fabric dropping a message.

## What to do about it

**For work that must not be lost, avoid shared subscriptions spanning nodes**, or keep the workers of a group on one node, until groups are served from the stream.

**Alert on `mast_durable_publish_failures_total`.** If it moves, publishers are being refused and are retrying, which is correct behaviour and also a sign the core's JetStream is struggling.

**If a message genuinely must not be lost, do not lean on QoS alone.** That is true of every MQTT broker. Idempotent handling keyed on an application-level identifier costs little and survives the duplicates that at-least-once delivery produces.
