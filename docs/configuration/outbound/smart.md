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
- `disable_direct`: disables direct-connectivity probes; default `false`. Use it on networks where direct foreign HTTPS is blocked or must not be touched. Member failures are attributed normally without the ambiguous-offline deferral, and `notify_all_dead` cannot fire because it requires a fresh direct success.

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

Attachment tokens identify the exact member instance. Connections completing after a provider replaces that member, or after the group begins shutdown, are closed instead of being attached to a replacement.

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

Stall-triggered checks are coalesced until the attachment receives read progress, with at most one admitted primary check per heartbeat interval.

A single operator failure marks a healthy primary suspect and starts a confirmation check in parallel with standby validation. Confirmed failure quarantines the member, initially for `30s`, with exponential backoff capped at `15m`. Recovery requires three successful half-open checks, at least `20s` apart. If no primary is eligible, emergency checks can recover a member without waiting for its quarantine timer; retries back off through `5s`, `10s`, `20s`, and `30s` while there is demand or a network/wake event. Wake-triggered emergency rounds retain the minimum `2s` gap.

At startup or on a new network generation, the two best-ranked members are checked first. The first successful member can carry new traffic immediately. For up to `10s`, a previous primary that is not down, or the best unknown member, can be used while validation is pending.

A recovered lower-numbered tier must stay healthy for `60s` before tier restoration. An ordinary stability or throughput improvement needs two distinct observations at least `30s` apart and a primary dwell of at least `10m`; healthy primaries are not replaced merely for a lower ping. Sustained low-rate demand can trigger a bounded fresh primary confirmation and challenger pair, including during busy traffic. A freshly target-meeting challenger can waive the dwell; this waiver cannot override tier, health state, or stability priority. See [Degradation checks](#degradation-checks) for admission limits and cooldowns.

### Internal panic containment

Smart contains its own probe, throughput, scheduler, persistence, callback, and synchronous-entry panics. A contained probe is **no data**, not a successful check or a member failure: queued and in-flight state is cleared without suspecting or quarantining the member. URL-test callers receive a containment error rather than a fabricated latency. Throughput reservations settle at the known consumed bytes, or the full reserved cap when consumption is unknown; work that never started settles at zero.

The scheduler releases its state locks before recovery, cancels and clears interrupted work, and restarts after `1s`, doubling up to `5m`. One supervisor owns shutdown completion. After ten interrupted loop attempts within an hour, scheduling stops and selection freezes on the current primary, or the first reachable member in declared tier order when no primary exists. Status marks containment degradation separately from ordinary tier or health degradation.

If contained probes prevent validation at startup or after a network change, the `10s` startup deadline no longer rejects unknown members. Smart uses unknown members in declared tier and configuration order, with `degraded: true`, without recording successful health evidence. Known failures remain failures. Normal selection resumes when a real successful result arrives.

Containment contracts for caller-owned execution:

- `SelectMember` returns the current primary, or the first member in declared tier and configuration order, and marks the group degraded.
- Dial and packet-listen wrappers return a containment error when their own execution panics. They do not retry a panic as a member-health failure.
- An interrupted `Attach` leaves the connection running untracked and returns an inert lease whose `Close` succeeds. Passive accounting skips that attachment.
- Status, networks, export, import, reset, and URL-test APIs return a containment error.
- Cancellation callbacks, ledger admission/accounting, and passive counter callbacks drop the interrupted sample. Durable throughput reservations remain reserved until reconciliation succeeds.

Panic diagnostics are logged at error level and exposed as additive **version 1** status fields:

- `panics`: `{ "total": N, "recent": [{ "at": "RFC3339 UTC", "site": "...", "message": "...", "stack": "..." }] }`. The ring holds at most `20` entries; messages are at most `512` UTF-8 bytes and stacks at most `4096` bytes.
- `degraded`: boolean indicating containment fallback or a stopped scheduler.
- `budgets`: current ledger snapshots for **all three** keys, `wifi`, `cellular`, and `other`, each containing `day`, `day_ends_at`, `used_mb`, `reserved_mb`, and `limit_mb` (plus the existing budget metadata). `day_ends_at` is RFC3339 UTC for the end of that accounting day, calculated using the device's local offset; clock rollback retains the end of the latest accounting day already seen. Remaining budget is `max(0, limit_mb - used_mb - reserved_mb)`. The existing current-network `budget` field remains available.

Messages replace URLs, IP addresses, and host/domain names with `<url>`, `<ip>`, and `<host>` before logging or status storage. Stacks retain only function names and source basenames with line numbers, not arguments, memory addresses, or build paths. Containment is a last-resort availability guard, not proof that an interrupted operation completed.


#### Failure bound and reconnect time

With demand, the default heartbeat and adaptive timeout remove a dead primary from selection within:

```text
15s heartbeat + 8s first check + 8s confirmation = 31s worst case
```

The typical removal time is about `20s`. Standby validation runs in parallel with confirmation. If the standby was checked in the last `60s`, a replacement is validated by that moment; otherwise validation can require one more member timeout. Primary and standby checks have a reserved worker that background confirmations cannot occupy. When a primary check becomes due, background work is cancelled to free capacity even if that worker is checking the standby. Custom heartbeat intervals or timeouts change this bound.

This is a selection-removal bound, not an application-recovery guarantee. Applications control whether and when they reconnect. Existing flows are not replayed. Ordinary `DialContext` can retry one eligible standby for a timeout or handshake error returned before the connection is handed off, within the remaining dial budget. With no caller deadline, its total budget is twice the member timeout. Destination refusals and resets are neutral and are not retried; lazy handshakes after handoff and TUN fast-path flows are not retried.

### UDP and QUIC

For UDP ports other than `443`, Smart uses the TCP primary unless it has fresh negative generic-UDP evidence. It then prefers a member with fresh positive UDP evidence, falling back to the primary if none is known.

For UDP/443 with `quic_policy: "auto"`:

- Positive or unknown UDP/443 evidence on the TCP primary keeps traffic on that primary. Unknown evidence never rejects a flow.
- Fresh negative evidence on the primary permits a member with fresh positive UDP/443 evidence only at the same tier and no more than one throughput class worse.
- If there is no suitable member, fresh negative evidence rejects the new UDP/443 session. This may encourage an application to fall back to TCP, but cannot force it.

`route_only` uses this routing preference without rejection. `off` disables the UDP/443 special case. Generic UDP success is never substituted for UDP/443 success. Generic UDP positive evidence lasts `30m`; UDP/443 positive and corroborated negative evidence last `10m`.

The first UDP/443 flow in a network generation triggers discovery across healthy members, repeated every `10m` while there is UDP/443 demand, including when the current flow is rejected. Before results arrive, there is no health-based rejection. The TUN fast path declines health-dependent rejections and lets normal packet routing reevaluate each new session; it does not install a durable dispatcher rejection. Member selection remains destination-aware through nested groups. The group's `Now()` value is only the TCP/display projection.

### Interfaces, pause, and direct connectivity

Default-interface, network-type, carrier, and BSSID changes start a new network generation. Stale checks from the old generation are discarded, in-flight work is cancelled, and new evidence is collected. Pause, cancellation, resolver failure, and link loss are neutral: they do not count as member failures. No probe timers run while paused; waking coalesces primary and standby checks. Pause and shutdown cancel any active `all_dead` notification.

Wi-Fi and cellular metadata improve per-network learning but are optional. Denied permissions, redacted SSIDs/BSSIDs, and stale cellular observations fall back to coarser keys. Smart does not turn on routing's `NeedWIFIState()` requirement. Direct probes use a separate bootstrap resolver and the platform-bound default-network path, not the profile DNS router or a proxy member. Built-in direct target lists depend on the platform country setting; there is no IP-geolocation request.

If direct checks also fail and no member has succeeded in this generation, ambiguous member failures are deferred for up to `60s`, then discarded as offline rather than blamed on every member. Invalid captive-portal certificates do not count as working direct Internet. A network change cancels active speed tests; exact underlay pinning is not guaranteed, and bytes with uncertain network type are charged to cellular.

### Budgets and cache

**Active throughput tests require an enabled `experimental.cache_file`.** Without persistent cache storage they are disabled, even when `throughput.enabled` is `true`; reachability checks and selection still work.

The daily ledger is shared by every Smart group in the process. If groups configure different limits, the effective limit for each network type is the minimum of their limits. Tests are serial, at most four per hour, and are not started while paused or in power-save mode. Ordinary calibration waits for light user traffic, with primary calibration before standby and other members. Degradation checks have a separate, bounded busy-traffic exception described below.

A test reserves `min(25 MB, 20% of remaining daily budget)` before it starts. If the cap is below `1.1 MB`, it is skipped. Counted member-connection bytes include a `10%` estimate for outer overhead; at most `10%` reservation overshoot is allowed. Download uses at most two connections and ramps from `1 MB` until a steady measurement or the cap. The `1 MB` upload test runs only on Wi-Fi, or on cellular with a configured daily budget of at least `500 MB`.

Budget reservation and reconciliation use cancellable background admission work, not the health owner or selection locks. A throughput measurement starts only after its reservation is durable and current power, pause, network, health, and traffic gates are rechecked. Cancelled tests that never start are reconciled at zero bytes, including jobs waiting in worker queues. Disabled throughput, absent cache, and a zero network-type budget never enter admission. Denied admissions back off: `5s` while another test runs, `1m` for the hourly limit or a temporary disabled state, until the next local day for exhausted budget, and `30s` for storage errors.

Reservations are persisted before testing, reconciled afterward, and charged in full after a crash if left orphaned. Daily accounting uses platform local time. If the clock moves to an earlier date, the latest date already seen keeps accumulating usage; a clock rollback does not grant another budget. These budgets cover active throughput tests, not ordinary user traffic or small health probes.

Active speed results last `24h`. A passive result needs a busy window of at least `10s`, proves only a lower bound on achieved speed, and lasts `6h`; a newer active result supersedes older passive observations. Expired results become unknown.

#### Degradation checks

Smart can recheck a primary before its active estimate expires. A rolling 20-second window must stay below half `target_mbps` (12.5 Mbit/s at the default 25 Mbit/s target), transfer at least 256 KiB, and keep at least one current-generation flow open and at least ten seconds old. Coverage measures elapsed time with an open flow, including zero-byte samples as slow samples. A contiguous read stall of up to eight seconds is allowed: TCP loss recovery can back off through roughly 1, 2, and 4 seconds without the application becoming idle. A longer stall or no eligible open flows resets the window. Sampling still tolerates intervals of 0.5–3 seconds, uses actual elapsed time, and resets after a sampling gap longer than three seconds. Partial oldest samples are weighted by their overlap with the window. Rates aggregate concurrent primary flows; passive observations never demote the server.

Each sampled rate must remain below half-target; a recovered sample resets the low-rate window rather than being averaged away by later slowness.

Intermittent traffic such as 375 KB every other second can deliberately raise suspicion. Paced video or an application pause shorter than the stall bound can resemble a throttled bulk download; passive counters cannot prove saturation. False suspicion costs a bounded active confirmation, while rejecting loss-stalled downloads would miss the throttling this feature is meant to detect. Active confirmation and the degradation allowance, not a high progress-coverage threshold, guard against that trade-off.

One episode performs a fresh primary confirmation and, only when it confirms below half-target, one fresh challenger. These download-only jobs may run during user traffic, survive ordinary heartbeat preemption, and still obey health, power, pause, network, cache and budget gates. Primary confirmation adds the user's achieved primary download bytes over exactly the same interval to active-test bytes. Fast proof excludes the first 1 MB and requires four target-meeting intervals of at least 500 ms each. Low results remain provisional: stale fast estimates cannot defeat the fresh-challenger requirement. A same-tier, healthy challenger with no worse stability must freshly meet the target before the demand waiver switches selection.

After warmup, slow proof is checked incrementally as bytes arrive over intervals of at least two seconds. It does not wait for the larger quota required to prove fast throughput, so a 0.5 Mbit/s primary can confirm before the 30-second test deadline. If that deadline expires after the connected probe receives any payload over a body interval of at least two seconds, its total average, including warmup and identical-interval same-primary passive bytes, can still prove slowness when strictly below half-target. Dialing and response-header latency do not count toward that minimum; shorter body intervals remain `no_data`. Including warmup can only overstate speed, so this fallback cannot manufacture slow evidence from a startup burst. A 0.145 MB payload over 30 seconds therefore confirms slow even without finishing warmup. Zero payload, dial or handshake failure, cancellation, and incomplete non-deadline responses remain failures or `no_data`, not slow proof.

A fast confirmation receives a one-hour cooldown for that primary and network: repeated suspicion spends at most one confirmation during that cooldown and does not consume the hourly challenger cooldown. Other completed episodes receive 30 minutes. Degradation challengers are limited to one per hour, including standby members, and rotate through untried or least-recently-tried eligible members. Two slow results suggest a shared bottleneck or two slow servers; they do not prove a local problem or trigger a switch.

Degradation admissions share the ordinary durable ledger, four-tests-per-hour limit and daily budget. Their separate admission allowance is 10% of the effective daily network-type budget. At target 25 Mbit/s, confirmation reserves 10 MB nominal, and a full confirmation/challenger pair allows at most 38.5 MB including overshoot headroom. Before another confirmation, at least 49.5 MB of degradation allowance must remain, preserving one full episode after a harmless test. With 5000 MB Wi-Fi and 1000 MB cellular budgets, allowances are 500 MB and 100 MB; five worst-case harmless cellular confirmations leave 45 MB and further confirmations wait. Small budgets may prevent any adequately sized confirmation. Exhausting this allowance does not disable ordinary calibration. Uncertain attribution after a network handover charges all consumed bytes to cellular, even when the final cellular allowance is exceeded: this is an admission allowance, not a hard accounting cap.

Before confirmation, Smart also checks that the ordinary budget remaining after its worst-case charge can still fund a target-sized challenger. Challenger admission requires at least the target-sized confirmation cap (10 MB at target 25), including framing headroom; an undersized challenger is rejected before recording an attempt or starting its hourly cooldown.

Additive version 1 status includes `throughput_degradation` (state, observed/provisional rate, continuous flow count, last check, retry time and block reason) and each budget's `degradation` usage, reservations and limit. Containment `degraded` retains its separate meaning. Budget, cooldown or eligibility blocks mean there is no universal switching deadline.

**Switching helps new connections only. An established slow download stays on its current server.**

If a completed test's accounting write or callback fails, its durable reservation remains charged as reserved space, with its settlement queued before the callback runs. Smart retries the pending reconciliation before admitting another test; a temporary storage failure or contained accounting panic does not require a restart.

### History, privacy, and backup

Learned history is scoped to the cache ID. Installation metadata is **not**: the installation ID, HMAC seed, direct-list seed, budget ledger, reservations, and import lineage share one installation-wide metadata bucket across profiles and cache IDs. Changing cache ID or resetting history does not reset daily usage or create a new installation identity. **A different `cache_file.path` is treated as a different installation**, which is a storage-path limitation.

An absent Smart metadata or history document initializes empty first-run state. Other cache read errors still fail startup rather than silently discarding stored data.

Network keys always store Wi-Fi SSIDs/BSSIDs and detailed location components as per-install HMAC-SHA256 hashes, using the first `16` hexadecimal characters. MCC, MNC, and radio technology are not hashed. Clear labels are stored separately for local display and explicitly requested raw exports. The default export is redacted. Raw exports can reveal network names and location-related identifiers; share them only with people you trust. Hashing is pseudonymization, not removal of every identifying fact: carrier, radio, interface, and server statistics remain.

Local member identities include the outbound configuration and local public-key identity; credential or configuration changes start fresh live evidence. Private keys are never persisted in history or exported. Imported statistics match server identities and provide bounded priors only. Imports never restore live health, quarantine, the budget ledger, or `last_ok`. Reimporting the same source replaces that source's prior rather than adding it again. Foreign-install hashed network keys cannot be matched and are ignored; only matching coarse keys are imported, without folding child records into parents.

Configured fingerprints belong to each outbound or endpoint instance. Reloading or checking configurations does not keep closed instances alive in the constructor registries. A `parser` member uses the parsed concrete outbound's identity rather than the link wrapper's identity.

Each network also learns six local-time, four-hour buckets. A bucket influences stability only after at least `24` weight units of its own observations and can shift the stability class by at most one step. One weight unit is five minutes of observed time; pauses do not contribute. Pause exclusion keeps a separate baseline for every network key and member identity, including each hierarchy level, so changing networks cannot consume another key's paused time. Statistics decay with a three-day half-life and shrink toward broader network levels until enough local evidence is available.

Fresh cellular location data adds a hashed tower key below carrier and radio technology. Tower changes affect history attribution only, not network generation or live health. Tower data older than `120s`, or missing after permission revocation, falls back to a coarser key.

History reset clears learned statistics, events, timeline, and imported priors for the selected group while keeping live health and installation metadata. Reset checks the caller's current history token inside the cache transaction and returns its new token atomically; an old group cannot reclaim a replacement's ownership. Replacement startup serializes history loading and claiming with reset and claims ownership only after validation succeeds. Ledger retirement watchers stop when an unused owner is pruned, even while the cache remains open. Android backup does not restore live Smart health; export/import is a statistics exchange, not a running VPN-state backup.

Stored history is bounded: at most `48` network keys, `32` identities per key, `500` events per group, and `1 MiB` serialized history. Live identity-to-server metadata is removed when its last retained statistic is evicted; unused provider configurations do not accumulate metadata. A dedicated persistence worker serializes consistent snapshots and writes at most once per minute without waiting in the health owner loop or holding its state lock during serialization or disk writes. Reset is ordered against pending writes, and cache-scoped generation tokens prevent detached writes from overwriting replacement or reset history. Shutdown gives the group's own final history and accounting wait a shared `5s` deadline, then logs and abandons it; recent learned history may be lost, and unfinished durable reservations remain conservatively charged on restart. This deadline does not bound a hung storage layer: upstream cache-file cleanup can still wait in bbolt `DB.Close` for an outstanding transaction. Ledger owners are retired when their cache lifetime ends. The cache uses bbolt, which reuses freed pages but does not shrink its file after eviction or reset. Expect a high-water file size of a few MiB; automatic compaction is not performed.

### Development

The Linux real-component harness uses in-process client and server boxes, user-space WireGuard/gVisor endpoints, per-member UDP fault relays, and protocol-correct HTTPS, TCP DNS, STUN, and QUIC version-negotiation fixtures. It requires no Internet access, TUN device, or root privileges. It lives in the separate `protocol/group/smartharness` test package, so its temporary CA trust through `SSL_CERT_FILE` and `SSL_CERT_DIR` affects only the harness binary, not other group tests. Direct-connectivity probes are disabled with `probe.disable_direct`.

Server startup retries bind collisions at most five times, closing each failed box before choosing a fresh port. The startup regression test forces both IPv4 and IPv6 wildcard-port collisions.

The harness checks initial selection after 20/5/1.5 Mbit/s calibration at a 10 Mbit/s target, UDP reachability, budget accounting, cache restart persistence, blackhole failover, and panic counts. The rates are scaled down so race-instrumented packet processing does not become the bottleneck; exactly one member must still meet the target. WireGuard retains the phone's `1280`-byte MTU rather than using jumbo packets to hide packet-size failures. The harness does not assert ordinary speed-improvement switching, which has a ten-minute dwell, or mid-session throughput degradation detection.

The harness uses a `30s` primary heartbeat so each calibration can finish before primary validation preempts background work. A heartbeat shorter than the built-in throughput sample duration can repeatedly cancel throughput tests, consume the hourly test allowance, and leave throughput unknown. Configured outer link rates include WireGuard framing; measured application throughput is lower. The CI test workflow runs three consecutive race-enabled passes:

```sh
GOTOOLCHAIN=go1.26.8 go test -race -count=3 -timeout=9m -ldflags=-checklinkname=0 \
  -tags with_gvisor,with_quic,with_dhcp,with_wireguard,with_utls,with_acme,with_clash_api,with_tailscale,with_ccm,with_ocm,badlinkname,tfogo_checklinkname0 \
  ./protocol/group/smartharness/
```
