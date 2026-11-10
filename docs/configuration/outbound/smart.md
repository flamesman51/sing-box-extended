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
- `timeout`: optional per-member probe timeout. When omitted, the timeout is `3 × RTT p90 + 1s`, clamped to `2.5s`–`8s`. A member with no successful check in the current network generation has a `6s` minimum to allow a cold WireGuard handshake. The legacy policy also accepts `"0s"` to select this adaptive timeout; the profile engine requires an explicitly configured timeout to be positive.
- `disable_direct`: disables direct-connectivity probes; default `false`. Use it on networks where direct foreign HTTPS is blocked or must not be touched. Member failures are attributed normally without the ambiguous-offline deferral, and `notify_all_dead` cannot fire because it requires a fresh direct success.

Configured intervals must be positive on the profile engine. On the legacy policy, `"0s"` selects the built-in default for that interval. Negative intervals and timeouts are invalid on both engines. Demand means attached traffic within the last `30s`. Probe targets and direct-connectivity targets are built in, not configurable URLs.

#### udp

- `enabled`: enables independent UDP reachability probes; default `true`.
- `quic_policy`: `auto` (default), `route_only`, or `off`. An explicit empty string selects `auto` on the legacy policy and is invalid on the profile engine.

Generic UDP and UDP/443 are tested separately: a server can carry DNS or STUN successfully while dropping QUIC traffic. See [UDP and QUIC](#udp-and-quic).

#### throughput

- `enabled`: enables throughput measurement; default `true`.
- `target_mbps`: sufficient download rate in Mbit/s; default `25`. An explicitly configured target must be positive and finite on the profile engine; on the legacy policy, `0` selects the built-in default `25`. Negative and non-finite targets are invalid on both engines. Profiles can supply a different default. The legacy policy stops favoring throughput improvements after this target is met; profile criteria can rank measured download speed separately.
- `download_url`: default `https://speed.cloudflare.com/__down?bytes={bytes}`. It must be a valid URL containing the literal `{bytes}` placeholder.
- `upload_url`: default `https://speed.cloudflare.com/__up`; it must be a valid URL.
- `wifi_daily_budget_mb`: default `300`.
- `cellular_daily_budget_mb`: default `50`.
- `other_daily_budget_mb`: default `300`, including Ethernet.

Budgets use decimal MB (`1 MB = 1,000,000 bytes`) and must be nonnegative. An explicit `0` disables active throughput tests for that network type; it is not replaced by the default. See [Budgets and cache](#budgets-and-cache).

Without a platform interface, Smart classifies the network as `other`, regardless of the default interface's type. This makes `other_daily_budget_mb` apply to server and desktop environments without platform-provided network classification.

#### interrupt_on_failover

Default `true`. When a member is confirmed dead, close its attached connections and flows, even if no replacement is available. Other members are not interrupted. A switch for a better tier, stability, or throughput affects only new connections.

Attachment tokens identify the exact member instance. Connections completing after a provider replaces that member, or after the group begins shutdown, are closed instead of being attached to a replacement.

#### notify_all_dead

Default `true`. Notify only after every member has corroborated failures, the interface is up, and a fresh direct HTTPS probe confirms that direct Internet access works, continuously for at least `45s`. Recovery cancels the notification. Paused, offline, captive, and unknown-direct states do not produce this notification.

### Profiles

`preference` selects a built-in policy and optionally overrides its raw values:

```json
{
  "preference": {
    "profile": "balance-speed-1080p",
    "rank": ["speed_floor", "stability", "download", "latency"],
    "min_stability": "C",
    "speed_floor_hysteresis": 0.2,
    "switch": {
      "improvement": true,
      "dwell": "10m",
      "confirmations": 2,
      "confirmation_interval": "30s",
      "tier_restore": "60s",
      "speed_margin": 1.25
    }
  }
}
```

On the profile engine, precedence is **explicit field, then profile value, then built-in default**. This applies to every preference leaf and to the profile-owned existing fields: `throughput.target_mbps`, `throughput.enabled`, `probe.heartbeat_interval`, `probe.standby_interval`, `probe.background_interval`, `interrupt_on_failover`, and `udp.quic_policy`. Explicit `false`, `0`, `"0s"`, and `"none"` are never replaced by a profile default; they must still satisfy that field's validation rules. In particular, an explicit zero target, probe interval, or probe timeout, or an empty QUIC policy is invalid on this engine, not an omitted value. Profiles never set or increase throughput budgets.

No `preference`, an empty `preference` object, or only `profile: "default"` retains the unchanged legacy policy, including the meaning of explicitly configured existing fields: zero probe intervals select their built-in defaults, `probe.timeout: "0s"` selects adaptive timing, `throughput.target_mbps: 0` selects `25`, and `udp.quic_policy: ""` selects `auto`. Engine selection depends only on new preference leaves: for example, `throughput.target_mbps: 8` without `preference` still uses the legacy engine. Empty `switch` objects add no override. `default` keeps the legacy tier/state/stability/throughput/p90 comparison, improvement signature, dwell, confirmations, and degradation evidence.

**Default plus overrides uses the profile engine; latency is p50-bucketed and confirmations are per decisive criterion.** Any new preference leaf with the default base, including a rank without a profile or `{"switch":{"improvement":false}}`, selects this engine and reports `profile: "custom"` in status. Its base is rank `stability, throughput, latency`, no minimum stability, target `25`, and the switch defaults below. `custom` is a resolved status value, not a built-in profile name.

#### Built-in policies

Unless a row says otherwise, defaults are: heartbeat `15s`, standby `1m`, background `5m`, `interrupt_on_failover: true`, `udp.quic_policy: "auto"`, throughput enabled, improvement enabled, dwell `10m`, two confirmations at least `30s` apart, tier restoration hold `60s`, speed-floor hysteresis `0.2`, and download speed margin `1.25`.

| Profile | Intent | Rank | Minimum stability | Target Mbit/s | Other defaults |
| --- | --- | --- | --- | --- | --- |
| `default` | Unchanged legacy behavior | Legacy order | none | 25 | Legacy behavior |
| `stability-downtime` | Stay online with fast failover | stability, outages, latency | none | 4 | Heartbeat `10s`, standby `30s` |
| `stability-connections` | Preserve running connections; choose steadier servers for new connections | stability, outages, latency | none | 4 | Interrupt `false`, dwell `30m`, confirmations `3` |
| `balance-speed-720p` | Most stable server sufficient for 720p, otherwise the fastest acceptable server | speed_floor, stability, download, latency | C | 4 | |
| `balance-speed-1080p` | Most stable server sufficient for 1080p, otherwise the fastest acceptable server | speed_floor, stability, download, latency | C | 8 | |
| `balance-speed-1440p` | Most stable server sufficient for 1440p, otherwise the fastest acceptable server | speed_floor, stability, download, latency | C | 15 | |
| `balance-speed-4k` | Most stable server sufficient for 4K, otherwise the fastest acceptable server | speed_floor, stability, download, latency | C | 25 | |
| `speed-max` | Raw measured download speed | download, stability, latency | C | 25 | Dwell `5m`; target drives degradation detection, not a speed ceiling |
| `responsive` | Responsive browsing | latency, stability, download | C | 4 | |
| `realtime-calls` | Voice and video calls | udp, stability, rtt_spread, latency, speed_floor | C | 4 | Heartbeat `10s`, standby `30s`, interrupt `false` |
| `realtime-gaming` | Games | udp, latency, rtt_spread, stability | C | 2 | Heartbeat `10s`, standby `30s`, interrupt `false` |
| `data-saver` | Metered plans and minimal probing | stability, latency | none | 4 | Throughput disabled, heartbeat `30s`, standby `2m`, background `15m` |
| `phone` | Phone default; same policy as balance-speed-1080p | speed_floor, stability, download, latency | C | 8 | |
| `desktop` | Desktop default; same policy as balance-speed-4k | speed_floor, stability, download, latency | C | 25 | |
| `server` | Shared proxy with bulk traffic and availability | speed_floor, stability, download, latency | C | 25 | Heartbeat `10s`, standby `30s` |

For `stability-connections`: **The selector never closes running connections itself, and new connections move only to steadier servers. Connections can still end if their server fails, the network changes or the VPN restarts.**

That guarantee describes the unmodified profile. Explicit overrides, including `interrupt_on_failover: true`, take precedence. Switching by itself never closes established connections; `interrupt_on_failover` alone controls closing on quarantine. No upload profile is provided: upload is recorded from a `1 MB` sample but is not ranked.

`speed-max` ranks raw download speed without stopping at its target. Its `25 Mbit/s` default target drives degradation detection and stays within the existing `25 MB` proof cap.

#### Raw preference fields

- `profile`: one of the built-in names above; omitted means `default`.
- `rank`: a nonempty list of known, unique criteria. An explicit list replaces the profile's entire rank, rather than extending it.
- `min_stability`: `A`, `B`, `C`, `D`, or `none`; `none` clears a profile's floor. Within the same tier and health state, members meeting the floor sort before those below it. This gate does not make an unavailable member reachable.
- `speed_floor_hysteresis`: a finite number in `[0, 0.5]`, default `0.2`. A challenger must meet the full target; the current primary retains floor credit while its latest fresh measurement is at least `(1 - hysteresis) × target`. Expired evidence always loses credit.
- `switch.improvement`: default `true`. When `false`, no improvement or degradation switch occurs; failover and enabled tier restoration still work.
- `switch.dwell`: positive duration, default `10m`; minimum primary selection time before an ordinary improvement switch.
- `switch.confirmations`: integer at least `1`, default `2`.
- `switch.confirmation_interval`: positive duration, default `30s`; minimum spacing between confirmations.
- `switch.tier_restore`: nonnegative duration, default `60s`. Explicit `"0s"` disables tier restoration; it does not restore immediately.
- `switch.speed_margin`: finite number at least `1`, default `1.25`; a download improvement requires at least this multiple of the primary's measured download rate.

#### Profile criteria and evidence

All profile policies compare **tier first, then health state**. Reachability is never traded for speed, latency, or stability. The minimum-stability gate follows, then the resolved rank, previous-primary country stickiness, and configuration order.

| Criterion | Comparison and evidence |
| --- | --- |
| `stability` | Existing A–D stability classes, including local-hour history buckets. |
| `speed_floor` | At this criterion's position in the rank, fresh measured download meeting the target sorts first. Active evidence lasts `24h`; passive evidence lasts `6h`. The primary gets the hysteresis allowance described above. When both members miss the floor, their download buckets decide before later criteria; unknown follows every measured result. Earlier rank criteria always retain priority. |
| `download` | Fresh measured download in logarithmic buckets: `floor(log(Mbit/s) / log(1.25))`, higher first. Unknown follows every measured result. This is separate from the legacy throughput class. |
| `throughput` | Existing throughput class: meets target, unknown, then progressively slower target fractions. |
| `latency` | Nearest-rank p50 probe time in `25ms` buckets; unknown with fewer than three samples. The profile estimator guarantees p50 is no greater than p90. |
| `rtt_spread` | Nearest-rank `(p90 - p50)` in `25ms` buckets; unknown with fewer than eight samples. This measures HTTP/TLS probe-time spread, **not packet jitter**. |
| `udp` | Generic UDP evidence: working, unknown, failed. |
| `quic` | Separate UDP/443 evidence: working, unknown, failed. Generic UDP success does not prove QUIC reachability. |
| `outages` | Outage episodes per effective at-risk hour: class 0 below `0.05/h`, class 1 below `0.25/h`, class 2 below `1/h`, otherwise class 3. Evidence is unknown until effective at-risk exposure reaches `2h`, even when episodes have already occurred. |

Unknown reachability evidence sits after known-good and before known-bad evidence. Measured-download ordering has an explicit exception: any measured result precedes an unknown result. Thus, with target `8 Mbit/s`, a measured `3 Mbit/s` member ranks ahead of an unmeasured member in the below-floor download fallback. The unmeasured member is still eligible for safe exploration. In balance profiles, where `speed_floor` comes first, the fastest acceptable measured member wins when no member meets the target; for example, `20 Mbit/s` beats `3 Mbit/s` at a `25 Mbit/s` target even if the latter is more stable. A custom rank or `realtime-calls` still honors every criterion before `speed_floor`.

An outage episode opens when a previously usable healthy or suspect member becomes quarantined. It closes after three successful TCP probes spaced at least `20s` apart, with no failure between them. A failure resets this recovery count, not the episode count. Both the open episode and its recovery count persist across restarts; the health state machine remains unchanged, so a member can become usable before its episode closes. Failed probation and repeated quarantines before confirmed recovery remain one episode. Episodes and exposure are recorded at every distinct network-path level. Ranking uses only the most specific level with at least `2h` of effective exposure, falling back to a parent if necessary; overlapping levels are never added together. Matching imported evidence contributes before this exposure check. Decay uses a `72h` half-life; decay and imported-history trust scale episode count and exposure equally, preserving their ratio while reducing evidence mass. Stall signals are not a ranking criterion.

Exposure, the outage-rate denominator, accrues only while the member is healthy or suspect and usable, the group is running and unpaused, and it is on that path. Quarantine, probation, paused, offline, stopped, and other-path time does not count. Total observed time is tracked separately for diagnostics only. If an open episode's member and path remain unobserved for more than `30m`, the next observation censors that episode: it ends without recovery success, its episode count remains, and a later loss starts a new episode. A short restart with at most `30m` unobserved preserves both the open episode and its recovery count.

When `udp` or `quic` appears in the rank, the new engine refreshes a bounded fixed pool: the standby, the next ranked eligible member, and the pinned pending improvement candidate until its confirmation finishes or is discarded. Generic UDP refresh runs every `20m` against a `30m` TTL; QUIC refresh runs every `8m` against a `10m` TTL. These fixed members refresh below TTL with slack. A cancelled, preempted, or dropped fixed-pool refresh retries after `30s`, then `60s`, then `2m`, until it succeeds or the evidence deadline passes. Queuing a job does not count as refreshing evidence.

One additional exploration slot rotates through the other healthy same-tier members in configuration order, first those meeting `min_stability`. Meeting-floor exploration runs normally; below-floor members join the rotation only when no existing fallback is accepted by the forwarding policy. For QUIC, an accepted fallback is healthy, at the primary's tier, has positive UDP/443 evidence, and is no more than one throughput class worse than the primary; for generic UDP, it is healthy with positive generic-UDP evidence. This keeps forwarding-compatible fallbacks discoverable below the soft stability floor. Exploration is best effort: its evidence may expire to unknown between visits, and maximum revisit is the rotation-member count times the refresh interval, subject to the bounded queue and priorities.

Demand-triggered UDP/443 discovery uses the same pool, probes due fixed members, and advances the rotating slot at most once per `10m`. Legacy discovery is unchanged.

#### Profile switching and degradation

An improvement compares the bucketed criteria in order. The first differing rank entry is decisive. A download comparison, including the embedded download bucket when both members miss `speed_floor`, also requires the challenger to meet `speed_margin`. A decision made by that embedded download comparison reports decisive criterion `speed_floor`, not `download`. Confirmations count only while the same challenger remains better on the same decisive criterion, with new relevant evidence and at least `confirmation_interval` between observations. A switch needs the configured confirmation count and primary dwell.

Download, speed-floor, and throughput-class confirmations are scheduled explicitly: up to `confirmations - 1` additional challenger throughput tests, spaced by the confirmation interval. They bypass ordinary per-member `24h` test spacing but still obey ledger budgets, admission gates, and the four-starts-per-hour limit. Budget, rate, or busy denials leave confirmations pending rather than resetting them; pending confirmations expire with their measurement TTL. Probe-fed criteria use normal probe cadence.

Confirmation jobs measure download only, with no upload phase. Routine heartbeat preemption does not cancel them. Pause, a network-generation change, the candidate leaving healthy state, shutdown, or their existing bounded `30s` admission/job timeout does cancel them. Reserved health-worker priority is unchanged.

Pending confirmations exist only in memory and are never persisted or restored. They are scoped to the network generation, configuration/provider membership generation, candidate identity, and primary epoch. A change to any of these discards pending confirmations; existing job tokens reject late results.

For profiles ranking `speed_floor`, `download`, or `throughput`, degradation starts below `(1 - hysteresis) × target`, not the legacy half-target threshold. Confirmed below-threshold results remove the primary's floor credit. Exploration requires a healthy same-tier member meeting the minimum-stability floor, but no existing speed measurement; unmeasured members can therefore be tested. After paired fresh measurement, a degradation switch requires both `switch.improvement: true` and a better challenger under the resolved criteria. It waives dwell, but never bypasses tier, health, or ranking. A below-floor challenger can win through the download fallback rather than having to reach the target.

A confirmed throughput result must be positive and satisfy the measurement's existing duration and data minimums. Zero or failed measurements are inconclusive: they do not remove floor credit and never count as a confirmation; health probes handle reachability failures.

Degradation pairs run only when the existing ledger admits them; profiles never raise budgets. With the default `300 MB` Wi-Fi and `50 MB` cellular budgets, the pair flow is effectively unavailable on cellular and on Wi-Fi becomes possible only after regular spending lowers the ordinary test cap. Balance profiles converge opportunistically through regular tests and scheduled confirmations in admitted idle windows, with every lease idle for at least `2s`. Continuous traffic leaves a better member pending until an idle window; convergence is not guaranteed while that traffic continues.

Failover chooses the first eligible member in profile order. Tier restoration retains its configured healthy hold and is skipped when disabled. Existing connections remain on their selected member.

Status includes `preference: {profile, rank, min_stability, target_mbps, improvement}`. Relevant member fields include `download_bucket`, `meets_floor`, and `outages: {episodes, exposure_hours, class}`.

#### App policy overrides and status

The gomobile API exposes `OverrideOptions.SmartPolicy string`, `SmartProfiles() (string, error)`, and `CheckSmartPolicy(json string) error`.

An empty `SmartPolicy` preserves the subscription configuration. A nonempty value is an object containing only `preference`, `throughput.target_mbps`, `throughput.enabled`, the three profile-owned probe intervals, `interrupt_on_failover`, and `udp.quic_policy`. Before parsing the subscription, the service removes `preference` and all seven profile-owned fields from every `type: "smart"` outbound, then deep-merges this object. Other settings, including budgets, URLs, probe timeouts, member lists, and non-Smart outbounds, are unchanged. For example, `{"preference":{"profile":"balance-speed-4k"}}` replaces a subscription's explicit `target_mbps: 8` with the profile's target of `25`. Configuration comments use the normal parser. Invalid JSON, unknown fields, null values, and invalid policy values fail with `smart policy override: <reason>`; `CheckSmartPolicy` applies the same validation.

`SmartProfiles` returns a JSON array in the built-in table order. Each object contains `name`, `rank`, `min_stability`, `target_mbps`, `throughput_enabled`, `heartbeat_interval`, `standby_interval`, `background_interval`, `interrupt_on_failover`, `quic_policy`, `improvement`, `dwell`, `confirmations`, `confirmation_interval`, `tier_restore`, `hysteresis`, and `speed_margin`. Values come from the core resolver; durations are Go duration strings and numeric values are JSON numbers.

The status `preference` object also includes:

- `engine`: `legacy` or `profile`.
- `base`: the built-in profile name, or `default` for a Classic-based custom policy.
- `custom`: true only on the profile engine when a preference leaf other than `profile`, or a profile-owned existing field, was explicitly supplied. Presence counts even when the value equals its default; an empty `switch` object does not count. Legacy policies always report false.
- `source`: `override` when a nonempty app override was applied, otherwise `config`.
- `override_hash`: lowercase SHA-256 hex of the exact applied `SmartPolicy` string, including whitespace; empty when none was applied. This metadata is internal and cannot be supplied in configuration JSON.
- `resolved`: the full effective policy in the override object shape, with every value explicit, suitable for `CheckSmartPolicy` and for initializing an editor. It includes `preference.profile`, `rank`, `min_stability`, `speed_floor_hysteresis`, all six `preference.switch` fields, both profile-owned throughput fields, all three probe intervals, `interrupt_on_failover`, and `udp.quic_policy`.

Saving a fully explicit Classic-based `resolved` policy selects the profile engine, as described above for default plus overrides; the legacy comparator itself is not representable as a custom rank.

### Selection and recovery

The unchanged legacy policy (`default`, without preference overrides) compares eligible members in this order:

1. Lower tier.
2. Health state: healthy, emergency, then the current suspect primary.
3. Stability class: A (`≥ 98%`), B (`≥ 93%`), C (`≥ 80%`), D (below `80%`). Low-confidence local history lowers the class by one step, but not below C.
4. Throughput class: meets the target, unknown, `75–100%`, `50–75%`, `25–50%`, below `25%` of the target.
5. p90 probe latency, computed by the legacy RTT estimator.
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

This subsection describes the legacy `default` path. Profile thresholds and eligibility differences are described under [Profile switching and degradation](#profile-switching-and-degradation); budgets, accounting, and admission limits still apply.

Smart can recheck a primary before its active estimate expires. A rolling 20-second window must stay below half `target_mbps` (12.5 Mbit/s at the default 25 Mbit/s target), transfer at least 128 KiB (about 52.4 kbit/s), and keep at least one current-generation flow open and at least ten seconds old. This byte floor admits extreme throttling near 50 kbit/s while rejecting a 16 kbit/s trickle. Coverage measures elapsed time with an open flow, including zero-byte samples as slow samples. A contiguous read stall of up to eight seconds is allowed: TCP loss recovery can back off through roughly 1, 2, and 4 seconds without the application becoming idle. A longer stall or no eligible open flows resets the window. Sampling still tolerates intervals of 0.5–3 seconds, uses actual elapsed time, and resets after a sampling gap longer than three seconds. Partial oldest samples are weighted by their overlap with the window. Rates aggregate concurrent primary flows; passive observations never demote the server.

Each sampled rate must remain below half-target; a recovered sample resets the low-rate window rather than being averaged away by later slowness.

Intermittent traffic such as 375 KB every other second can deliberately raise suspicion. Paced video or an application pause shorter than the stall bound can resemble a throttled bulk download; passive counters cannot prove saturation. False suspicion costs a bounded active confirmation, while rejecting loss-stalled downloads would miss the throttling this feature is meant to detect. Active confirmation and the degradation allowance, not a high progress-coverage threshold, guard against that trade-off.

One episode performs a fresh primary confirmation and, only when it confirms below half-target, one fresh challenger. These download-only jobs may run during user traffic, survive ordinary heartbeat preemption, and still obey health, power, pause, network, cache and budget gates. Primary confirmation adds the user's achieved primary download bytes over exactly the same interval to active-test bytes. Fast proof excludes the first 1 MB and requires four target-meeting intervals of at least 500 ms each. Low results remain provisional: stale fast estimates cannot defeat the fresh-challenger requirement. A same-tier, healthy challenger with no worse stability must freshly meet the target before the demand waiver switches selection.

After warmup, slow proof is checked incrementally as bytes arrive over intervals of at least two seconds. It does not wait for the larger quota required to prove fast throughput, so a 0.5 Mbit/s primary can confirm before the 30-second test deadline. If that deadline expires after the connected probe receives any payload over a body interval of at least two seconds, its total average, including warmup and identical-interval same-primary passive bytes, can still prove slowness when strictly below half-target. Dialing and response-header latency do not count toward that minimum; shorter body intervals remain `no_data`. Including warmup can only overstate speed, so this fallback cannot manufacture slow evidence from a startup burst. A 0.145 MB payload over 30 seconds therefore confirms slow even without finishing warmup. Zero payload, dial or handshake failure, cancellation, and incomplete non-deadline responses remain failures or `no_data`, not slow proof.

A fast confirmation receives a one-hour cooldown for that primary and network: repeated suspicion spends at most one confirmation during that cooldown and does not consume the hourly challenger cooldown. Other completed episodes receive 30 minutes. Degradation challengers are limited to one per hour, including standby members, and rotate through untried or least-recently-tried eligible members. Two slow results suggest a shared bottleneck or two slow servers; they do not prove a local problem or trigger a switch.

Degradation admissions share the ordinary durable ledger, four-tests-per-hour limit and daily budget. Their separate admission allowance is 10% of the effective daily network-type budget. At target 25 Mbit/s, confirmation reserves 10 MB nominal, and a full confirmation/challenger pair allows at most 38.5 MB including overshoot headroom. Before another confirmation, at least 49.5 MB of degradation allowance must remain, preserving one full episode after a harmless test. With 5000 MB Wi-Fi and 1000 MB cellular budgets, allowances are 500 MB and 100 MB; five worst-case harmless cellular confirmations leave 45 MB and further confirmations wait. Small budgets may prevent any adequately sized confirmation. Exhausting this allowance does not disable ordinary calibration. Uncertain attribution after a network handover charges all consumed bytes to cellular, even when the final cellular allowance is exceeded: this is an admission allowance, not a hard accounting cap.

The lower byte floor can suspect application-limited trickle flows above that floor. A fast primary still costs at most one confirmation per primary/network per hour, at most 11 MB per confirmation at target 25 Mbit/s. With a 1000 MB cellular daily budget, five full-cost harmless confirmations spend 55 MB and leave 45 MB, below the 49.5 MB admission requirement. Smaller actual charges can permit more confirmations; the worst-case total daily degradation admission allowance, including challengers, remains 100 MB, subject to the uncertain-attribution exception above. The default 50 MB cellular budget provides only 5 MB of allowance, so it admits no target-25 confirmation.

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

The harness checks initial selection after 20/5/1.5 Mbit/s calibration at a 10 Mbit/s target, UDP reachability, budget accounting, cache restart persistence, blackhole failover, and panic counts. Slice-D scenarios cover a primary falling from 20 to 1.5 Mbit/s, a single downstream limiter shared by multiple member relays, two slow servers with an untried fast alternative, same-interval passive-byte correction, and a startup burst followed by slow delivery. The degradation switch retains a strict 120-second bound. Tests require a fresh degradation episode, a healthy target-meeting challenger, and the explicit throughput-driven switch event rather than a health failover. Bufferbloat may temporarily make the slow primary suspect. The relay uses a bounded 32-packet FIFO with drops; realistic TCP retransmission stalls are not treated as flow termination. Tests also check original-flow attachment, new-connection routing, provisional-result isolation, cooldowns, the 10% admission allowance, and completed scenario teardown.

The two-slow-server scenario checks the first unsuccessful pair and its exposed retry state. Status does not expose attempted challenger identities or the next eligible challenger, so fixture request counts prove B was tested and C was not. The fixed one-hour challenger cooldown has no configuration override; the real-component suite does not wait an hour. `TestSmartDegradationRotationAcrossCooldown` in `protocol/group` covers the complete A/B/C rotation with the fake clock.

The rates are scaled down so race-instrumented packet processing does not become the bottleneck. Degraded links use an optional bounded 32-packet policer; startup calibration and the existing harness retain their 256-packet queue. The passive-correction scenario alone uses 40 Mbit/s member and shared-link headroom, with a paced 20 Mbit/s user stream and an active probe capped below 3 Mbit/s, so shared capacity can meet the unchanged 10 Mbit/s target. Its strict fast-confirmation assertion requires same-interval passive-byte correction. WireGuard retains the phone's `1280`-byte MTU rather than using jumbo packets to hide packet-size failures. Ordinary speed-improvement switching still has a ten-minute dwell and is not timed by this harness.

The shared downstream limiter serializes packets from their relay arrival timestamps. Late timer wakeups consume elapsed link time rather than adding another per-packet delay, so timer granularity does not silently reduce the configured capacity.

The harness uses a `30s` primary heartbeat so each calibration can finish before primary validation preempts background work. The degradation scenarios leave `probe.timeout` unset and use the production adaptive timeout. A heartbeat shorter than the built-in throughput sample duration can repeatedly cancel throughput tests, consume the hourly test allowance, and leave throughput unknown. Configured outer link rates include WireGuard framing; measured application throughput is lower. Scenarios run serially to avoid race-instrumented packet-processing contention. CI runs one pass with `-count=1 -timeout=15m` without the race detector, because two-core runners cannot carry the throughput scenarios under it; the smart unit tests run there with `-race`. Local acceptance requires three consecutive race-enabled passes:

```sh
GOTOOLCHAIN=go1.26.8 go test -race -count=3 -timeout=40m -ldflags=-checklinkname=0 \
  -tags with_gvisor,with_quic,with_dhcp,with_wireguard,with_utls,with_acme,with_clash_api,with_tailscale,with_ccm,with_ocm,badlinkname,tfogo_checklinkname0 \
  ./protocol/group/smartharness/
```
