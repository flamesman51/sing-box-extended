### Structure

`smart` is an automatic outbound group. It chooses a reachable member first, then favors long-term stability, sufficient throughput, and finally latency. It supports stream connections, packet connections, nested groups, and the TUN flow fast path.

```json
{
  "type": "smart",
  "tag": "auto",
  "outbounds": ["primary-a", "primary-b", "backup"],
  "member_options": {
    "primary-a": {"tier": 1, "country": "NL"},
    "primary-b": {"tier": 1, "country": "KZ"},
    "backup": {"tier": 2}
  },
  "probe": {
    "heartbeat_interval": "15s",
    "standby_interval": "60s",
    "background_interval": "5m"
  },
  "udp": {
    "enabled": true,
    "quic_policy": "auto"
  },
  "throughput": {
    "enabled": true,
    "target_mbps": 25,
    "download_url": "https://speed.cloudflare.com/__down?bytes={bytes}",
    "upload_url": "https://speed.cloudflare.com/__up",
    "wifi_daily_budget_mb": 300,
    "cellular_daily_budget_mb": 50,
    "other_daily_budget_mb": 300
  },
  "interrupt_on_failover": true,
  "notify_all_dead": true
}
```

The values shown are defaults except for the example member tags and metadata. An omitted `probe.timeout` selects the adaptive timeout described below.

### Fields

#### outbounds

List of member outbound tags. Members can also come from the group fields `providers` and `use_all_providers`; `include` and `exclude` filter provider members by tag. Configuration order is the final tie-breaker.

#### member_options

An object keyed by member tag. Each entry accepts:

- `tier`: integer priority, starting at `1`; the default is `1`. A healthy lower-numbered tier takes priority over every higher-numbered tier, even if it is slower. An unavailable member is not made eligible by its tier.
- `country`: optional country code used only as a late tie-breaker. It is not an IP-geolocation lookup and does not override reachability, tier, stability, or throughput.

#### probe

- `heartbeat_interval`: primary check interval while there is demand; default `15s`.
- `standby_interval`: standby check interval while there is demand; default `60s`.
- `background_interval`: other-member check interval when there has been demand in the last ten minutes; default `5m`. With no recent demand, background checks run every `30m`.
- `timeout`: optional positive per-member probe timeout. When omitted, the timeout is `3 × RTT p90 + 1s`, clamped to `2.5s`–`8s`. A member with no successful check in the current network generation has a `6s` minimum to allow a cold WireGuard handshake.

Configured intervals must be positive. Demand means attached traffic within the last `30s`. Probe targets and direct-connectivity targets are built in, not configurable URLs.

#### udp

- `enabled`: enables independent UDP reachability probes; default `true`.
- `quic_policy`: `auto` (default), `route_only`, or `off`.

Generic UDP and UDP/443 are tested separately: a server can carry DNS or STUN successfully while dropping QUIC traffic. See [UDP and QUIC](#udp-and-quic).

#### throughput

- `enabled`: enables throughput measurement; default `true`.
- `target_mbps`: sufficient download rate in Mbit/s; default `25`, and must be positive. Smart does not continuously maximize speed after the target is met.
- `download_url`: default `https://speed.cloudflare.com/__down?bytes={bytes}`. It must be a valid URL containing the literal `{bytes}` placeholder.
- `upload_url`: default `https://speed.cloudflare.com/__up`; it must be a valid URL.
- `wifi_daily_budget_mb`: default `300`.
- `cellular_daily_budget_mb`: default `50`.
- `other_daily_budget_mb`: default `300`, including Ethernet.

Budgets use decimal MB (`1 MB = 1,000,000 bytes`) and must be nonnegative. An explicit `0` disables active throughput tests for that network type; it is not replaced by the default. See [Budgets and cache](#budgets-and-cache).

#### interrupt_on_failover

Default `true`. When a member is confirmed dead, close its attached connections and flows, even if no replacement is available. Other members are not interrupted. A switch for a better tier, stability, or throughput affects only new connections.

#### notify_all_dead

Default `true`. Notify only after every member has corroborated failures, the interface is up, and a fresh direct HTTPS probe confirms that direct Internet access works, continuously for at least `45s`. Recovery cancels the notification. Paused, offline, captive, and unknown-direct states do not produce this notification.

### Selection and recovery

Smart compares eligible members in this order:

1. Lower tier.
2. Health state: healthy, emergency, then the current suspect primary.
3. Stability class: A (`≥ 98%`), B (`≥ 93%`), C (`≥ 80%`), D (below `80%`). Low-confidence local history lowers the class by one step, but not below C.
4. Throughput class: meets the target, unknown, `75–100%`, `50–75%`, `25–50%`, below `25%` of the target.
5. Median latency plus jitter.
6. Same country as the previous primary.
7. Configuration order.

Health is proven by successful probe transactions, not by connection counters. In particular, SYN, ACK, RST, and packet byte counts are accounting and stall signals, never proof that a server works. HTTPS probes use independent operators and valid TLS; a negative TCP result requires failures against two different operators. UDP probes use STUN. UDP/443 probes validate QUIC version-negotiation responses. Probe name resolution uses DNS-over-TCP through the member rather than the profile's DNS router.

A single operator failure marks a healthy primary suspect and starts a confirmation check in parallel with standby validation. Confirmed failure quarantines the member, initially for `30s`, with exponential backoff capped at `15m`. Recovery requires three successful half-open checks, at least `20s` apart. If no primary is eligible, emergency checks can recover a member without waiting for its quarantine timer; retries back off through `5s`, `10s`, `20s`, and `30s` while there is demand or a network/wake event.

At startup or on a new network generation, the two best-ranked members are checked first. The first successful member can carry new traffic immediately. For up to `10s`, a previous primary that is not down, or the best unknown member, can be used while validation is pending.

A recovered lower-numbered tier must stay healthy for `60s` before tier restoration. An ordinary stability or throughput improvement needs two distinct observations at least `30s` apart and a primary dwell of at least `10m`; healthy primaries are not replaced merely for a lower ping. Sustained demand on an actively measured slow primary can trigger a challenger test in a light-traffic window within `5m` and waive the dwell when the challenger meets the target.

#### Failure bound and reconnect time

With demand, the default heartbeat and adaptive timeout remove a dead primary from selection within:

```text
15s heartbeat + 8s first check + 8s confirmation = 31s worst case
```

The typical removal time is about `20s`. Standby validation runs in parallel with confirmation. If the standby was checked in the last `60s`, a replacement is validated by that moment; otherwise validation can require one more member timeout. Custom heartbeat intervals or timeouts change this bound.

This is a selection-removal bound, not an application-recovery guarantee. Applications control whether and when they reconnect. Existing flows are not replayed. Ordinary `DialContext` can retry one eligible standby for a timeout or handshake error returned before the connection is handed off, within the remaining dial budget. With no caller deadline, its total budget is twice the member timeout. Destination refusals and resets are neutral and are not retried; lazy handshakes after handoff and TUN fast-path flows are not retried.

### UDP and QUIC

For UDP ports other than `443`, Smart uses the TCP primary unless it has fresh negative generic-UDP evidence. It then prefers a member with fresh positive UDP evidence, falling back to the primary if none is known.

For UDP/443 with `quic_policy: "auto"`:

- Positive or unknown UDP/443 evidence on the TCP primary keeps traffic on that primary. Unknown evidence never rejects a flow.
- Fresh negative evidence on the primary permits a member with fresh positive UDP/443 evidence only at the same tier and no more than one throughput class worse.
- If there is no suitable member, fresh negative evidence rejects the new UDP/443 session. This may encourage an application to fall back to TCP, but cannot force it.

`route_only` uses this routing preference without rejection. `off` disables the UDP/443 special case. Generic UDP success is never substituted for UDP/443 success. Generic UDP positive evidence lasts `30m`; UDP/443 positive and corroborated negative evidence last `10m`.

The first UDP/443 flow in a network generation triggers discovery across healthy members, repeated every `10m` while there is UDP/443 demand. Before results arrive, there is no health-based rejection. The TUN fast path declines health-dependent rejections and lets normal packet routing reevaluate each new session; it does not install a durable dispatcher rejection. Member selection remains destination-aware through nested groups. The group's `Now()` value is only the TCP/display projection.

### Interfaces, pause, and direct connectivity

Default-interface, network-type, carrier, and BSSID changes start a new network generation. Stale checks from the old generation are discarded, in-flight work is cancelled, and new evidence is collected. Pause, cancellation, resolver failure, and link loss are neutral: they do not count as member failures. No probe timers run while paused; waking coalesces primary and standby checks.

Wi-Fi and cellular metadata improve per-network learning but are optional. Denied permissions, redacted SSIDs/BSSIDs, and stale cellular observations fall back to coarser keys. Smart does not turn on routing's `NeedWIFIState()` requirement. Direct probes use a separate bootstrap resolver and the platform-bound default-network path, not the profile DNS router or a proxy member. Built-in direct target lists depend on the platform country setting; there is no IP-geolocation request.

If direct checks also fail and no member has succeeded in this generation, ambiguous member failures are deferred for up to `60s`, then discarded as offline rather than blamed on every member. Invalid captive-portal certificates do not count as working direct Internet. A network change cancels active speed tests; exact underlay pinning is not guaranteed, and bytes with uncertain network type are charged to cellular.

### Budgets and cache

**Active throughput tests require an enabled `experimental.cache_file`.** Without persistent cache storage they are disabled, even when `throughput.enabled` is `true`; reachability checks and selection still work.

The daily ledger is shared by every Smart group in the process. If groups configure different limits, the effective limit for each network type is the minimum of their limits. Tests are serial, at most four per hour, and are not started while paused or in power-save mode. They wait for light user traffic. Primary calibration comes first, then standby and challengers, with at most one challenger per hour and no repeat until eligible members have had a turn.

A test reserves `min(25 MB, 20% of remaining daily budget)` before it starts. If the cap is below `1.1 MB`, it is skipped. Counted member-connection bytes include a `10%` estimate for outer overhead; at most `10%` reservation overshoot is allowed. Download uses at most two connections and ramps from `1 MB` until a steady measurement or the cap. The `1 MB` upload test runs only on Wi-Fi, or on cellular with a configured daily budget of at least `500 MB`.

Reservations are persisted before testing, reconciled afterward, and charged in full after a crash if left orphaned. Daily accounting uses platform local time. If the clock moves to an earlier date, the latest date already seen keeps accumulating usage; a clock rollback does not grant another budget. These budgets cover active throughput tests, not ordinary user traffic or small health probes.

Active speed results last `24h`. A passive result needs a busy window of at least `10s`, proves only a lower bound on achieved speed, and lasts `6h`; a newer active result supersedes older passive observations. Expired results become unknown.

### History, privacy, and backup

Learned history is scoped to the cache ID. Installation metadata is **not**: the installation ID, HMAC seed, direct-list seed, budget ledger, reservations, and import lineage share one installation-wide metadata bucket across profiles and cache IDs. Changing cache ID or resetting history does not reset daily usage or create a new installation identity. **A different `cache_file.path` is treated as a different installation**, which is a storage-path limitation.

Network keys always store Wi-Fi SSIDs/BSSIDs and detailed location components as per-install HMAC-SHA256 hashes, using the first `16` hexadecimal characters. MCC, MNC, and radio technology are not hashed. Clear labels are stored separately for local display and explicitly requested raw exports. The default export is redacted. Raw exports can reveal network names and location-related identifiers; share them only with people you trust. Hashing is pseudonymization, not removal of every identifying fact: carrier, radio, interface, and server statistics remain.

Local member identities include the outbound configuration and local public-key identity; credential or configuration changes start fresh live evidence. Private keys are never persisted in history or exported. Imported statistics match server identities and provide bounded priors only. Imports never restore live health, quarantine, the budget ledger, or `last_ok`. Reimporting the same source replaces that source's prior rather than adding it again. Foreign-install hashed network keys cannot be matched and are ignored; only matching coarse keys are imported, without folding child records into parents.

Configured fingerprints belong to each outbound or endpoint instance. Reloading or checking configurations does not keep closed instances alive in the constructor registries.

Each network also learns six local-time, four-hour buckets. A bucket influences stability only after at least `24` weight units of its own observations and can shift the stability class by at most one step. One weight unit is five minutes of observed time; pauses do not contribute. Statistics decay with a three-day half-life and shrink toward broader network levels until enough local evidence is available.

Fresh cellular location data adds a hashed tower key below carrier and radio technology. Tower changes affect history attribution only, not network generation or live health. Tower data older than `120s`, or missing after permission revocation, falls back to a coarser key.

History reset clears learned statistics, events, timeline, and imported priors for the selected group while keeping live health and installation metadata. Android backup does not restore live Smart health; export/import is a statistics exchange, not a running VPN-state backup.

Stored history is bounded: at most `48` network keys, `32` identities per key, `500` events per group, and `1 MiB` serialized history. Writes are debounced to at most once per minute. The cache uses bbolt, which reuses freed pages but does not shrink its file after eviction or reset. Expect a high-water file size of a few MiB; automatic compaction is not performed.
