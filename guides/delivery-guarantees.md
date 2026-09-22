# Delivery guarantees

What mast promises a client, hop by hop, and where the promise is weaker than an MQTT user would assume. Everything here was read off the code rather than inferred from the design; where something is not yet true it says so and links to the issue.

**The short version.** Within a single node, mast honours QoS 0, 1 and 2 in full. Across a cluster, QoS 1 and 2 are best-effort: the node a publisher is connected to acknowledges the message before it has crossed the fabric, and the fabric is at-most-once. If you run standalone, none of this applies to you.

| | Guarantee |
| --- | --- |
| QoS 0, anywhere | At-most-once, which is the contract |
| QoS 1 / 2, publisher and subscriber on the same node | Full — at-least-once and exactly-once respectively |
| QoS 1 / 2, across nodes | **Best-effort.** The publisher is told delivered; the message can still be dropped in the fabric ([#11](https://github.com/mastmq/mast/issues/11)) |
| Retained messages | Durable and cluster-wide. Stored in a JetStream KV bucket, Raft-replicated, not subject to the above |
| Shared subscriptions | Exactly-once to exactly one group member, but local-first rather than round-robin |
| Persistent sessions | Resume for the lifetime of the node holding them; not durable across a restart or a move ([#8](https://github.com/mastmq/mast/issues/8)) |

## Three hops, not one

An MQTT message in a mast cluster crosses up to three boundaries, and only the middle one is mast's own invention.

1. **Publisher to its ingress node.** Ordinary MQTT. mochi runs the QoS state machine against in-memory inflight state — PUBACK for QoS 1, the PUBREC/PUBREL/PUBCOMP handshake for QoS 2.
2. **Ingress node to the node owning a subscriber.** Core NATS, fire-and-forget. **At-most-once.**
3. **Owning node to the subscriber.** Ordinary MQTT again, with its own QoS negotiated per subscription and downgraded to the minimum of what the publisher sent and what the subscriber asked for.

Hops 1 and 3 are precisely what the MQTT specification defines, and mast implements them the way any single-node broker does. The spec says nothing about hop 2, because the spec does not contemplate a broker made of several machines. That silence is where the gap lives.

## What hop 2 costs, and what it does not

It is worth being concrete, because the cheapness is the whole reason the gap exists.

A non-retained publish performs **zero key-value operations** and adds **zero round trips**. The only KV writes in the bridge are `PutRetained` on a retained publish and `MatchRetained` on subscribe; the publish path itself is a `nats.Msg` and a fire-and-forget `PublishMsg`. The QoS 2 handshake never touches the fabric at all — it is terminated locally, against memory, on the node the client is connected to.

That is the trade. Hop 2 is nearly free because nothing is durable about it.

## When hop 2 actually drops

Core NATS discards rather than buffers. In practice:

- **Slow consumer** — a subscription exceeds its pending limits and the server drops for it
- **No interest at the instant of publish** — core NATS delivers to current subscribers and to nobody else
- **A leaf-node reconnect window** — an edge node re-establishing its link to the core
- **The ingress process dying** after it has acknowledged the client and before the bytes leave it

All of these are now visible. The bridge registers a NATS asynchronous error handler, so a drop produces both a log line naming the subject and the count, and a metric:

| Metric | Means |
| --- | --- |
| `mast_nats_slow_consumers_total` | **this node discarded messages it had already acknowledged.** Above zero is the alarm |
| `mast_nats_async_errors_total` | every asynchronous error on the fabric connection, slow consumers included |
| `mast_nats_disconnects_total` / `mast_nats_reconnects_total` | the fabric connection dropping and coming back |

No other metric can show the loss on its own: `mast_messages_in_total` counts a dropped message as accepted and `mast_messages_out_total` never counts it at all, so before these existed the gap was only visible to somebody subtracting two numbers nobody was subtracting.

## Is this unusual?

Partly. MQTT defines quality of service between a client and a broker, one hop at a time; it does not define end-to-end assurance from publisher to subscriber, and no broker offers it. Every broker acknowledges a QoS 2 publish before any subscriber has seen it. On that count mast behaves exactly like the rest.

What differs between clustered brokers is how reliable the internal hop is. Brokers that replicate a message to the node owning a session before acknowledging give up throughput for a guarantee that survives a node loss. mast currently does not, and that is a deliberate position rather than an oversight — but it is a position that should be chosen by whoever operates it, not discovered.

## The fix

Request-reply on the downlink hop: the owning node acknowledges acceptance before the ingress node acknowledges the client. The cost is one round trip on QoS 1 and 2 publishes and nothing at all on QoS 0.

The part still undecided is what "the owning node" means when several nodes hold interest. NATS request-reply hands back the first responder, and the ingress node has no idea how many nodes are listening. Acknowledging on the first reply is cheap but only proves one node took it; waiting for a count requires a count nobody has. The most promising shape is to scope the guarantee to session-owning nodes, because a connected subscriber that misses a message is a client that can notice, and an offline one is not.

Tracked as [#11](https://github.com/mastmq/mast/issues/11).

## What to do about it today

**Run standalone and the question disappears.** One process, no fabric hop, full QoS 1 and 2. This is the common case for an edge box or a single-tenant deployment, and it is why mast ships as one binary that clusters rather than a cluster you shrink.

**In a cluster, measure the gap before you worry about it.** It costs a message only when the fabric drops one, which is bounded by slow consumers and reconnects rather than being a steady rate. Scrape `mast_nats_slow_consumers_total` and alert on any increase: if it never moves under your traffic, the gap is theoretical for you, and if it does move you have the subject and the count in the log line next to it.

**If a message genuinely must not be lost, do not lean on QoS alone.** That is true of every MQTT broker and doubly true here. Idempotent handling keyed on an application-level identifier costs little and survives duplicates, drops and the redelivery that [#11](https://github.com/mastmq/mast/issues/11) would introduce.

**Retained messages are not affected.** They go through JetStream KV, which is Raft-replicated and at-least-once, so last-known-value survives a node loss even though live delivery might not.
