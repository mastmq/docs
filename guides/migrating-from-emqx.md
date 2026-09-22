# Migrating from EMQX

Written while putting mast beside a live EMQX v5 deployment on OpenShift. Everything here was observed rather than inferred; where something is untested it says so.

## Decide first whether this is a migration at all

MQTT protocol parity is maybe half of what an EMQX deployment uses. The other half is the platform around it: the dashboard, the REST API, the rule engine, data bridges into Kafka or Postgres, delayed publish, banned clients, exclusive subscriptions, auto-subscribe, MQTT over QUIC. mast has no answer for any of that and is not trying to.

So the question is not "can mast replace EMQX" but "does this deployment use anything outside the protocol". If the answer is the rule engine or data bridges, stop here. If it is authentication, authorization, QoS, retained messages and sessions, read on.

## Configuration mapping

| EMQX | mast |
| --- | --- |
| `EMQX_AUTHENTICATION__1__BACKEND: http` | `auth.mode = "http"` with `auth.http.wire = "emqx"` |
| `EMQX_AUTHENTICATION__1__URL` | `auth.http.authn_url` |
| `EMQX_AUTHORIZATION__SOURCES__1__URL` | `auth.http.authz_url` |
| `EMQX_AUTHORIZATION__NO_MATCH: deny` | `auth.http.on_error = "deny"` |
| `EMQX_LISTENERS__TCP__DEFAULT__BIND` | `mqtt.addr` |
| `EMQX_LISTENERS__TCP__INTERNAL__BIND` | `mqtt.internal_addr` |
| `EMQX_LISTENERS__TCP__INTERNAL__ENABLE_AUTHN: false` | implied: the internal listener never authenticates |
| `EMQX_LISTENERS__TCP__DEFAULT__MAX_CONN_RATE` | no equivalent yet |
| `EMQX_CLUSTER__DISCOVERY_STRATEGY: k8s` | not needed; NATS routes or leaf nodes |

`auth.http.wire = "emqx"` makes mast post the shapes EMQX's http backends use — `{token, username, password, client_id}` for authentication and `{token, username, password, topic, action}` for authorization — and read `{"result":"allow"|"deny"}` back, with HTTP 200 either way. An auth service written for EMQX needs no change.

Topics reach the authorization endpoint **unmounted**, as `snapp/driver/+/location` rather than `tenant/snapp/driver/+/location`. The tenant mount is internal to mast and never appears in the auth protocol, so existing ACL rules keep working.

## The four traps

**`is_superuser` is the one that will break you.** EMQX never consults authorization for a connection whose authentication returned `is_superuser: true`. Any ACL rule that would deny such a connection has therefore never run. A replacement that starts enforcing those rules is not more correct, it is differently behaved, and the difference surfaces as broken services. mast honours the flag and skips the policy for those connections — but check, before you switch, which of your services authenticate as superusers, because those are the ones whose ACL behaviour has never been exercised.

**EMQX has no tenants, so pick one.** Under the emqx wire the auth service cannot name a tenant, so everything lands in `tenant.default`. That value becomes a NATS subject token, so keep it to `[A-Za-z0-9_-]`. Multi-tenancy is available later by moving to the mast wire and having the service name a tenant per connection; nothing about the migration forecloses it.

**Go 1.24+ enables multipath TCP on listeners, and the OpenShift dataplane does not handle it.** Connections are accepted and then every read fails with `permission denied`, which looks like a broker fault and is not. Set `GODEBUG=multipathtcp=0`. The Helm chart exposes `extraEnv` for exactly this.

**OpenShift's restricted SCC assigns its own uid.** A pod that asks for a specific `runAsUser` or `fsGroup` is rejected outright. Null those fields — and note that setting them to `{}` is not enough, because Helm deep-merges maps and only an explicit `null` removes a default value.

## Suggested sequence

Run mast beside the existing broker first, on its own service, pointed at the same auth service. Nothing about this touches the EMQX release, and it costs a few hundred megabytes.

Exercise it with the credentials your services actually use, not test ones. Authentication, a denied token, QoS 1 both directions, a retained value reaching a late subscriber, a persistent session resuming, and the internal listener accepting an unauthenticated client are the checks worth automating; they are the ones that caught real differences here.

Point one low-stakes consumer at the mast service and leave it. Then widen.

## Known differences at the time of writing

Will messages are not delivered ([#6](https://github.com/mastmq/mast/issues/6)). If anything relies on last-will for presence detection, that is a blocker.

There is no connection rate limit, so a reconnect storm reaches the auth service unthrottled where EMQX would have capped it.

Sessions survive a client reconnecting, a broker restart, and a move to another node: subscriptions and the QoS 1 and 2 backlog live in the shared key-value buckets. At a single replica this is already more than EMQX gives you without persistent session storage configured.

There is no dashboard and no management API.
