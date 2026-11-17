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
| Persistent sessions | Durable. Subscriptions live in a replicated bucket and the QoS 1/2 backlog on a replicated stream, kept by the core tier, so a session follows a device to another node and survives the edge it last used going away ([below](#while-a-device-is-away)) |

## Three hops, not one

An MQTT message in a mast cluster crosses up to three boundaries, and only the middle one is mast's own invention.

1. **Publisher to its ingress node.** Ordinary MQTT. mochi runs the QoS state machine — PUBACK for QoS 1, the PUBREC/PUBREL/PUBCOMP handshake for QoS 2.
2. **Ingress node to the node owning a subscriber.** For QoS 0, core NATS, fire-and-forget. For QoS 1 and 2, a replicated JetStream stream.
3. **Owning node to the subscriber.** Ordinary MQTT again, with its own QoS negotiated per subscription and downgraded to the minimum of what the publisher sent and what the subscriber asked for.

Hops 1 and 3 are what the MQTT specification defines. The spec says nothing about hop 2, because it does not contemplate a broker made of several machines, and hop 2 is where a clustered broker earns or loses its guarantees.

## How hop 2 works

**QoS 0** is published on core NATS and nothing else. A non-retained QoS 0 publish performs zero storage operations and adds zero round trips. Core NATS discards rather than buffers, so a QoS 0 message can be lost to a slow consumer or a reconnect window — which QoS 0 permits.

**QoS 1 and 2** are written to one stream, `mast_qos`, before the ingress node sends its PUBACK or PUBREC. The write returns once JetStream has replicated it (`core.replicas`, three in a real core tier). Every node reads the stream through **one consumer of its own** — one per node, never one per subscription or session, which is the design mast exists to avoid — and hands each message to its local subscribers.

That consumer acknowledges what it has delivered, and JetStream sends again whatever it has not heard back about within three seconds. That is what carries a message across a node being cut off: while an edge's link to the core is down, what the core sends it is lost in transit, and the edge's own connection never notices. JetStream numbers every delivery to a consumer, so the first delivery to arrive after a loss shows the gap; the node then handles nothing past it and replaces its consumer from the last message it did handle, and a loss at the end of a burst shows up the same way when its redelivery arrives. The node recognises and drops any copy it already delivered by the message id every publish carries. The regression test for this cuts an edge off, publishes twenty QoS 1 messages elsewhere, and requires exactly twenty to arrive.

The stream keeps a message for `fabric.durable_max_age`, five minutes by default. That is how long a node can be unreachable and still catch up. Nothing acknowledges messages off the stream as a whole, so age is what removes them.

**When the stream cannot store a message, the publish is refused.** An MQTT 5 client receives a failing reason code on its PUBACK or PUBREC. An MQTT 3 client has no negative acknowledgement to receive, so its connection is closed, which is what makes it reconnect and send the unacknowledged message again. Each refusal counts on `mast_durable_publish_failures_total`. Refusing is the point: the alternative is acknowledging a message the broker cannot keep.

## While a device is away

A persistent session that has no connection still collects its QoS 1 and 2 messages, up to the newest hundred, for `session.expiry`. QoS 0 is never kept for an absent client, which MQTT allows, and a message is kept at the lower of the publish's QoS and the subscription's.

The core tier does the keeping. Each core runs a **queuer** that watches the sessions bucket — so it knows which sessions are away and what they subscribed to — and reads the `mast_qos` stream through one consumer of its own, writing every matching message to the away session's queue. The edge that last held the session writes the same messages at the moment it sees the client gone. Every writer names a message the same way, so the queue stream keeps one copy.

Until this existed, only the edge a device had disconnected from kept anything for it. Edges are the tier that restarts on every deploy, and once that edge was gone nothing anywhere was subscribed on the device's behalf: in a test of 2,000 sleeping devices across a rolling restart of three edges, all 10,000 messages published to them were acknowledged and none was delivered. The same test now delivers all of them, and a harder one — 5,000 sleeping devices, 100,000 messages, a core and an edge killed with SIGKILL during the publish and a second edge killed before they woke — delivered every acknowledged message in nine runs out of nine, with at most a few dozen duplicates in 100,000.

When a device comes back, its queue is read and delivered before anything else is removed, and only what was read is removed, so a node dying in between delivers the backlog again rather than losing it. The edge looks at the queue twice more, five and thirty seconds later, for anything a queuer was still writing when the device arrived.

A connection is recorded in the sessions bucket when it resumes a session, so the queuer stops keeping messages the device is now receiving live. That is one replicated write per reconnect of a persistent session.

## What it costs

Measured on one laptop, a core and two edges, every message crossing between edges to ten subscribers ([bench RESULTS.md](https://github.com/mastmq/bench/blob/main/RESULTS.md)):

| QoS 1, cross-node | p50 | p99 |
| --- | --- | --- |
| 2,000 publishes/s, core NATS only (before) | 0.53ms | 2.3ms |
| 2,000 publishes/s, durable stream | 0.64ms | 1.7ms |
| 10,000 publishes/s, core NATS only (before) | 0.94ms | 23ms |
| 10,000 publishes/s, durable stream | 12.8ms | 130ms |

Since the queuer, each core also reads every QoS 1 and 2 message. Measured on three cores capped at one CPU each with 5,000 QoS 1 messages a second crossing between edges, the cores' combined CPU went from about 0.7 of a CPU to about 1.5, and p99 latency from 1.4ms to 4.1ms. A core whose queuer finds no away session for a message does nothing more with it, but it still has to read it.

At moderate rates the guarantee is close to free. At high rates it costs tens of milliseconds, because every QoS 1 or 2 message is a replicated write and every node reads all of them — a node with no interest in a message still receives it and discards it. Numbers from one machine are for comparing designs, not for sizing a cluster.

A publisher also waits for the replicated write before its PUBACK, so a single MQTT connection publishing QoS 1 is paced by that write. That is the same trade every broker that replicates before acknowledging makes.

## What is still best-effort

**Shared subscriptions across nodes.** A `$share` group is served by a NATS queue group on core NATS, so exactly one member receives each message, but the hop to a member on another node is at-most-once. Serving groups from the stream needs a consumer per group rather than per node, which is a different design and not built. Within a node, a shared subscription at QoS 1 or 2 is full QoS.

**A node dying after it has taken a message.** Once a node has handed a message to its MQTT layer and acknowledged it on the stream, delivery to the subscriber is that node's MQTT state, held in memory. If the node dies before its subscriber acknowledges, the message is gone — unless the subscriber's session is persistent and it was absent at the time, in which case the message was already in the offline queue. The same property turns QoS 2 across a node crash into at-least-once.

**A device connected to an edge that is killed.** Its session is still recorded as held, so no queuer keeps its messages until it reconnects somewhere else — about a second for a device that retries promptly. What was published to it in that second is lost. With 3,000 connected devices taking 2,000 QoS 1 messages a second, a SIGKILL of one edge and of one core lost about eighty messages, on this version and the one before it alike. A graceful restart loses none, because each disconnect is recorded. Closing this needs the core tier to notice an edge's leaf connection dropping and treat its sessions as away, which is not built.

**A node cut off for longer than `fabric.durable_max_age`.** What the stream no longer holds, it cannot resend.

**`fabric.durable_qos = false`.** Turns all of this off: QoS 1 and 2 then cross nodes on core NATS, at-most-once, exactly as mast did before. It exists for rollouts and for measurement.

## Watching it

| Metric | Means |
| --- | --- |
| `mast_durable_publish_failures_total` | QoS 1/2 publishes refused because the stream would not store them. Any increase is publishers being told to retry |
| `mast_nats_slow_consumers_total` | a node discarded core NATS messages. Now QoS 0 and shared-group copies only |
| `mast_nats_disconnects_total` / `mast_nats_reconnects_total` | a node's fabric connection dropping and coming back |
| `mast_durable_read_total{reader}` | messages a node read off the stream, `bridge` on an edge and `queuer` on a core. Every reader should climb with the stream; one that stops has lost its consumer |
| `mast_queuer_away_sessions` | sessions a core's queuer keeps messages for. It should match on every core |
| `mast_offline_queued_total` / `mast_offline_delivered_total` | messages kept for an absent client, and replayed when it came back |

A node that cannot reach the stream at startup logs that cross-node QoS 1 and 2 are best-effort on it, and behaves exactly as mast did before this existed.

## Is this unusual?

MQTT defines quality of service between a client and a broker, one hop at a time. No broker offers end-to-end assurance from publisher to subscriber, and every broker acknowledges a QoS 2 publish before any subscriber has seen it. What differs between clustered brokers is how reliable the internal hop is. mast now replicates before acknowledging, which gives up some throughput at high rates for a guarantee that survives the fabric dropping a message.

## What to do about it

**For work that must not be lost, avoid shared subscriptions spanning nodes**, or keep the workers of a group on one node, until groups are served from the stream.

**Alert on `mast_durable_publish_failures_total`.** If it moves, publishers are being refused and are retrying, which is correct behaviour and also a sign the core's JetStream is struggling.

**If a message genuinely must not be lost, do not lean on QoS alone.** That is true of every MQTT broker. Idempotent handling keyed on an application-level identifier costs little and survives the duplicates that at-least-once delivery produces.
