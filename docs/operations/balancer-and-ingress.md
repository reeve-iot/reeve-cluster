# Balancer and ingress

What the balancer, ingress or proxy in front of the replicas must and must not do, the configuration that satisfies
that for common controllers, and how clients should retry the library's HTTP answers. The behaviour described here was
observed with nginx 1.28, ingress-nginx 4.15.1 (controller 1.15.1), Cilium 1.20.2 Gateway API (Envoy 1.37) and HAProxy
in TCP mode.

## The requirement

The balancer must:

- resend a request to another replica only when the connection failed (while connecting, sending the request, or
  reading the response header) or the answer is a `503`, and only for idempotent methods;
- never resend after a read timeout, because a request that timed out may have committed (the HTTP bound already makes
  a response start before `ProxyReadTimeout`, see [configuration.md](configuration.md#the-http-bound));
- never resend a non-idempotent method (`POST`, `PATCH`) after any byte of it was sent, and never resend a `202`.

Balancers match the status only. None tells the library's `503`s from any other `503`, and none reads `Retry-After`.
Resending any `503` of an idempotent method is harmless. A `503 write_refused` follows a handler that ran, and a
balancer resends it for `PUT` and `DELETE`, so what those handlers do outside configuration must be idempotent too.

A connection that failed after the request was sent may still have committed. That is the residual of a replica that
dies after a commit ([architecture overview](../architecture.md)). A balancer that routes only to ready
replicas still sees `503`s from replicas that left service between its readiness checks.

## What a client retries

The library answers a request or a hub invocation with these codes. Every `503` carries `Retry-After: 1`.

| Answer | Meaning | Retry |
|---|---|---|
| `503 replica_not_serving` | The replica is starting, loading, catching up, activated but not yet serving, or draining. Nothing ran. | Any method. |
| `503 replica_fenced` | The replica is fencing or fenced, or its serving check found fence start passed. Nothing ran. | Any method. |
| `503 replica_behind` | The read index was not reached within `ReadIndexTimeout`. The request did not run. | Any method. |
| `503 write_refused` | A commit of the request was refused by a fenced replica, with no configuration entry appended. No configuration write took effect, but the handler ran, and what it did outside configuration may have landed. | Only an idempotent method, or an action whose other effects are idempotent (an idempotency key). A client that cannot tell does not resend. |
| `202 write_unconfirmed`, `Reeve-Seq` | The write was appended but not acknowledged by every member in time, or the replica fenced or drained while waiting. It may be committed. | Never blindly: read before writing again. |
| the action's status, `Reeve-Replication: unconfirmed` | A `4xx` or `5xx` from the action after a commit whose acknowledgement is unconfirmed. | Per the action's status. |
| `503 status_unavailable` | The status endpoint's read failed or took longer than `ActionTimeBudget`. Not a refused request. | Any time. |

A hub invocation answers with the same codes as the message of a `HubException`, and the method's return value is
lost with it. The `errorCode` is in the body of every admission refusal; for `write_refused` and `write_unconfirmed` it
is there when the acknowledgement filter answered (the MVC result filter, `AcknowledgeWrites()` on a minimal-API
endpoint or group, or an exception after a refused commit). An endpoint without the filter gets the status and the
headers from the fence middleware and keeps its own body, so mark minimal-API writes with `AcknowledgeWrites()`.
`ReeveClusterHttpOptions.WriteError` puts the code in the application's own error envelope.

`AddReeveClusterAspNetCore` registers an `IExceptionHandler` ahead of every handler of the application, whichever is
registered first: it answers an exception that follows a refused commit by the rules above and declines every other
exception, which the application's handlers then see.

## nginx and ingress-nginx

Plain nginx, in the `server` or `location` that proxies to the replicas:

```nginx
proxy_next_upstream error http_503;
```

ingress-nginx, on every Ingress that routes to the replicas:

```yaml
metadata:
  annotations:
    nginx.ingress.kubernetes.io/proxy-next-upstream: "error http_503"
```

Do not list `timeout`, `http_500`, `http_502`, `http_504` or `non_idempotent`, and do not set the ConfigMap option
`retry-non-idempotent: "true"`. The controller's default renders `proxy_next_upstream error timeout;
proxy_next_upstream_tries 3` for every location, which resends an idempotent request after a read timeout and violates
the requirement.

What nginx and ingress-nginx resent (the two behaved identically in every case):

| `proxy_next_upstream` | Resent for GET, HEAD, OPTIONS, PUT, DELETE | Resent for POST, PATCH |
|---|---|---|
| `error http_503` (required) | any `503`; FIN, RST or a cut-off header after the request was sent; accepted then dropped; connection refused. Not: `500`, `502`, `504`, `429`, `202`, a read timeout, an invalid response, a stalled or reset body | connection refused only (nothing was sent) |
| `error timeout` (the ingress-nginx default) | no `503`; the connection failures; **a read timeout** | connection failures before anything was sent |
| `error timeout http_503` | as required, **plus a read timeout** | as required |
| `error http_503 non_idempotent` | as required | **`503` and FIN/RST/cut-off after the request was sent** |
| `error http_500 http_502 http_503 http_504` | as required, plus `500`, `502`, `504` | as required |
| `off` | nothing | nothing |

Once a response has started, nothing is resent. A `POST` whose connection failed before any byte was sent can be
resent, which is the "failure to connect" case and safe.

## Cilium Gateway API

Give the `HTTPRoute` rules that route to the replicas **no `retry` stanza**, and remove the client's Envoy retry
headers, unless every client is trusted not to send them:

```yaml
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
spec:                     # metadata, parentRefs and hostnames as the deployment has them
  rules:
    - backendRefs:
        - name: app
          port: 8080
      filters:
        - type: RequestHeaderModifier
          requestHeaderModifier:
            remove:
              - x-envoy-retry-on
              - x-envoy-retry-grpc-on
              - x-envoy-max-retries
```

- With no `retry` stanza nothing is resent, for any method and any failure: every `503`, reset and refusal reaches the
  client, which then retries from `Retry-After: 1`. That satisfies the requirement, which bounds what a balancer resends
  from above.
- A `retry` stanza (`retry: {codes: [503], attempts: N}`) resends **every method, `POST` and `PATCH` included**, on any
  `503`, a refused connection, FIN or RST after the request was sent, a cut-off header and an invalid response, and may
  resend to the same replica. Cilium 1.20 sets Envoy's `retry_on` to
  `retriable-status-codes,connect-failure,reset,refused-stream` for any route with a retry stanza, and the route has no
  field for the method or the conditions. This violates the requirement and cannot be narrowed.
- Without the header filter, a client that sends `x-envoy-retry-on: 5xx,reset` with `x-envoy-max-retries: 3` switches
  retries on by itself; in the test every `POST` that reached a `503` replica first was sent twice.
- The retry behaviour differs between Cilium versions (1.19 sets no `retry_on` at all). Check it again after a Cilium
  upgrade.
- There is no route timeout unless the route sets one; the listener's `stream_idle_timeout` is 300 s. A
  `timeouts.request` ends a request with `504`, which is not resent either.
- Cilium's default outlier detection (eject after 5 consecutive `5xx`, at most 10 % of hosts) does not eject a replica in
  a two-replica service: a replica answering `503` to every request kept receiving about half of them.

## A TCP edge

A TCP-mode load balancer (HAProxy `mode tcp`, for example) never sends a request twice: it has no notion of a request,
and its `retries` cover connection establishment only. It needs nothing for resending. Its server inactivity timeout
(`timeout server`) cuts a connection with no response, so it is a hop of `ProxyReadTimeout`.

## ProxyReadTimeout

`ProxyReadTimeout` is the shortest wait for a response to start over every hop. A request still unanswered at the
shortest of them is cut by that hop, not by the library. Typical defaults:

| Hop | Setting | Default |
|---|---|---|
| ingress-nginx | `proxy-read-timeout` | 60 s |
| nginx | `proxy_read_timeout` | 60 s |
| Cilium Gateway API (Envoy) | `stream_idle_timeout` of the listener; `timeouts.request` of the route when set | 300 s; none |
| HAProxy | `timeout server` | as configured |
| The client | its own request timeout | the client's |

Set `ProxyReadTimeout` to the smallest of these, and raise an edge whose timeout is below
`ReadIndexTimeout + ActionTimeBudget + AckTimeout`.

## Readiness and routing

Controllers route only to Ready endpoints. When a replica turns not ready, requests still reach it until the endpoint
update propagates. With a 1 s readiness probe, ingress-nginx sent requests to the replica for 2.4 s after it turned not
ready, and Cilium for about 0.05 s. Those requests get a library `503`, which the required nginx configuration resends
for an idempotent method. The `preStop` sleep covers this window on a graceful stop
([deployment.md](deployment.md#stopping-prestop-and-termination-grace)).

When no replica is ready, the controller answers with its own `503` and no `errorCode`. Reach the skip and status
endpoints then with `kubectl port-forward` to a pod, which bypasses the controller and reaches a replica that is not
ready.

## Long-lived connections

The read index and the acknowledgement cover a request and a hub invocation, not a connection that stays open. A fence
and a drain abort the connections the package tracks (hub connections, HTTP/1.1 WebSocket upgrades, and anything the
application registers with `ClusterConnections.Track`), and their clients reconnect to a replica that serves; give
SignalR clients `WithAutomaticReconnect()`. HTTP/2 WebSockets are not recognised, and an endpoint marked
`ShortCircuit()` bypasses the fence middleware ([ASP.NET Core](../guide/aspnetcore.md)).
