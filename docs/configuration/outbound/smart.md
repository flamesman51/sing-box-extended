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
    "streams": 1,
    "window": "7s",
    "warmup": "1s",
    "upload": true,
    "upload_window": "7s",
    "upload_warmup": "1s",
    "download_url": "https://speed.cloudflare.com/__down?bytes={bytes}",
    "upload_url": "https://speed.cloudflare.com/__up?bytes={bytes}",
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
- `streams`: parallel HTTPS connections per direction; default `1`, or `4` for `desktop`, `server`, and `speed-max`. On the profile engine an explicitly configured value must be an integer at least `1`; legacy `0` selects `1`.
- `window`: download measurement window, default `7s`; at least `5s`.
- `warmup`: download warmup, default `1s`; at least `1s`.
- `upload`: enables active upload measurement, default `true`. Explicit `false` disables upload phases without disabling downloads.
- `upload_window`: upload measurement window, default `7s`; at least `5s`.
- `upload_warmup`: upload warmup, default `1s`; at least `1s`.
- `download_url`: default `https://speed.cloudflare.com/__down?bytes={bytes}`. It must be a valid URL containing the literal `{bytes}` placeholder.
- `upload_url`: default `https://speed.cloudflare.com/__up?bytes={bytes}`; it must be a valid URL. A literal `{bytes}` placeholder, when present, is replaced with the POST body size. Upload is usable only when the final 2xx response carries `cf-meta-upload-bytes` equal to that body size. Missing receipts mark upload `unverified`; early, mismatched, or incomplete receipts never produce ranked upload evidence.
- `wifi_daily_budget_mb`: default `300`.
- `cellular_daily_budget_mb`: default `50`.
- `other_daily_budget_mb`: default `300`, including Ethernet.

Budgets use decimal MB (`1 MB = 1,000,000 bytes`) and must be nonnegative. An explicit `0` disables active throughput tests for that network type; it is not replaced by the default. See [Budgets and cache](#budgets-and-cache).

The four measurement durations preserve field presence: an explicit zero is invalid on the profile engine and selects the built-in default on the legacy engine. Nonzero values below the stated minimum are invalid on both engines. Omitted values select defaults. Measurement settings apply to every engine; they do not select the profile engine.

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

On the profile engine, precedence is **explicit field, then profile value, then built-in default**. This applies to every preference leaf and to the profile-owned fields: `throughput.target_mbps`, `throughput.enabled`, `throughput.streams`, `probe.heartbeat_interval`, `probe.standby_interval`, `probe.background_interval`, `interrupt_on_failover`, and `udp.quic_policy`. Explicit `false`, `0`, `"0s"`, and `"none"` are never replaced by a profile default; they must still satisfy that field's validation rules. In particular, explicit zero target, streams, probe or measurement durations, or an empty QUIC policy is invalid on this engine, not an omitted value. Profiles never set or increase throughput budgets.

No `preference`, an empty `preference` object, or only `profile: "default"` retains the legacy decision policy. Zero probe intervals select their built-in defaults, `probe.timeout: "0s"` selects adaptive timing, `throughput.target_mbps: 0` selects `25`, zero measurement fields select their defaults, and `udp.quic_policy: ""` selects `auto`. Engine selection depends only on preference leaves: for example, `throughput.target_mbps: 8` without `preference` still uses the legacy engine. Empty `switch` objects add no override. `default` keeps the legacy tier/state/stability/throughput/p90 comparison, improvement signature, dwell, and confirmations, but uses the same active-only measurement evidence and accounting as every other engine.

**Default plus overrides uses the profile engine; latency is p50-bucketed and confirmations are per decisive criterion.** Any new preference leaf with the default base, including a rank without a profile or `{"switch":{"improvement":false}}`, selects this engine and reports `profile: "custom"` in status. Its base is rank `stability, throughput, latency`, no minimum stability, target `25`, and the switch defaults below. `custom` is a resolved status value, not a built-in profile name.

#### Built-in policies

Unless a row says otherwise, defaults are: heartbeat `15s`, standby `1m`, background `5m`, `interrupt_on_failover: true`, `udp.quic_policy: "auto"`, throughput and upload enabled, `streams: 1`, download and upload windows `7s`, warmups `1s`, improvement enabled, dwell `10m`, two confirmations at least `30s` apart, tier restoration hold `60s`, speed-floor hysteresis `0.2`, and speed margin `1.25`.

| Profile | Intent | Rank | Minimum stability | Target Mbit/s | Other defaults |
| --- | --- | --- | --- | --- | --- |
| `default` | Unchanged legacy behavior | Legacy order | none | 25 | Legacy behavior |
| `stability-downtime` | Stay online with fast failover | stability, outages, latency | none | 4 | Heartbeat `10s`, standby `30s` |
| `stability-connections` | Preserve running connections; choose steadier servers for new connections | stability, outages, latency | none | 4 | Interrupt `false`, dwell `30m`, confirmations `3` |
| `balance-speed-720p` | Most stable server sufficient for 720p, otherwise the fastest acceptable server | speed_floor, stability, download, latency | C | 4 | |
| `balance-speed-1080p` | Most stable server sufficient for 1080p, otherwise the fastest acceptable server | speed_floor, stability, download, latency | C | 8 | |
| `balance-speed-1440p` | Most stable server sufficient for 1440p, otherwise the fastest acceptable server | speed_floor, stability, download, latency | C | 15 | |
| `balance-speed-4k` | Most stable server sufficient for 4K, otherwise the fastest acceptable server | speed_floor, stability, download, latency | C | 25 | |
| `speed-max` | Raw measured download speed | download, stability, latency | C | 25 | Streams `4`, dwell `5m`; target drives degradation detection, not a speed ceiling |
| `speed-upload` | Verified upload capacity | upload, stability, rtt | C | 25 | Target drives degradation detection only |
| `responsive` | Responsive browsing | rtt, stability, download | C | 4 | |
| `realtime-calls` | Voice and video calls | udp, stability, jitter, rtt, speed_floor | C | 4 | Heartbeat `10s`, standby `30s`, interrupt `false` |
| `realtime-gaming` | Games | udp, rtt, jitter, stability | C | 2 | Heartbeat `10s`, standby `30s`, interrupt `false` |
| `data-saver` | Metered plans and minimal probing | stability, latency | none | 4 | Throughput disabled, heartbeat `30s`, standby `2m`, background `15m` |
| `phone` | Phone default; same policy as balance-speed-1080p | speed_floor, stability, download, latency | C | 8 | |
| `desktop` | Desktop default; balance-speed-4k with four streams | speed_floor, stability, download, latency | C | 25 | Streams `4` |
| `server` | Shared proxy with bulk traffic and availability | speed_floor, stability, download, latency | C | 25 | Streams `4`, heartbeat `10s`, standby `30s` |

For `stability-connections`: **The selector never closes running connections itself, and new connections move only to steadier servers. Connections can still end if their server fails, the network changes or the VPN restarts.**

That guarantee describes the unmodified profile. Explicit overrides, including `interrupt_on_failover: true`, take precedence. Switching by itself never closes established connections; `interrupt_on_failover` alone controls closing on quarantine.

`speed-max` ranks raw download speed without stopping at its target. Any positive finite target is valid; neither target nor profile imposes a test-byte cap.

#### Raw preference fields

- `profile`: one of the built-in names above; omitted means `default`.
- `rank`: a nonempty list of known, unique criteria. An explicit list replaces the profile's entire rank, rather than extending it.
- `min_stability`: `A`, `B`, `C`, `D`, or `none`; `none` clears a profile's floor. Within the same tier and health state, members meeting the floor sort before those below it. This gate does not make an unavailable member reachable.
- `speed_floor_hysteresis`: a finite number in `[0, 0.5]`, default `0.2`. Normally a challenger must meet the full target; the current primary retains floor credit while its fresh regular download is at least `(1 - hysteresis) × target`. A completed degradation episode can revoke that credit until a newer idle download reaches the full target; see below.
- `switch.improvement`: default `true`. When `false`, no improvement or degradation switch occurs; failover and enabled tier restoration still work.
- `switch.dwell`: positive duration, default `10m`; minimum primary selection time before an ordinary improvement switch.
- `switch.confirmations`: integer at least `1`, default `2`.
- `switch.confirmation_interval`: positive duration, default `30s`; minimum spacing between confirmations.
- `switch.tier_restore`: nonnegative duration, default `60s`. Explicit `"0s"` disables tier restoration; it does not restore immediately.
- `switch.speed_margin`: finite number at least `1`, default `1.25`; a download or upload improvement requires at least this multiple of the primary's measured rate in that direction.

#### Profile criteria and evidence

All profile policies compare **tier first, then health state**. Reachability is never traded for speed, latency, or stability. The minimum-stability gate follows, then the resolved rank, previous-primary country stickiness, and configuration order.

| Criterion | Comparison and evidence |
| --- | --- |
| `stability` | Existing A–D stability classes, including local-hour history buckets. |
| `speed_floor` | At this criterion's position in the rank, fresh active download meeting the target sorts first. Active evidence lasts `24h`; passive traffic never grants floor credit. The primary normally gets the hysteresis allowance described above, subject to persisted floor revocation or proof. When both members miss the floor, their regular download buckets decide before later criteria; unknown follows every measured result. Earlier rank criteria always retain priority. |
| `download` | Fresh valid active download in logarithmic buckets: `floor(log(Mbit/s) / log(1.25))`, higher first. Unknown follows every measured result. This is separate from the legacy throughput class. |
| `upload` | Fresh valid receipt-verified active upload in logarithmic buckets, higher first; `speed_margin` applies. Unknown follows every measured result. Download and upload expire independently after `24h`. |
| `throughput` | Existing throughput class: meets target, unknown, then progressively slower target fractions. Only active evidence contributes. |
| `rtt` | Median true UDP round-trip time in `10ms` buckets; unknown with fewer than five valid STUN samples less than `6h` old. |
| `jitter` | Nearest-rank UDP RTT `(p90 - p50)` in `5ms` buckets; unknown with fewer than ten valid samples less than `6h` old. |
| `latency` | Nearest-rank p50 health-check time in `25ms` buckets; unknown with fewer than three samples. The profile estimator guarantees p50 is no greater than p90. This includes connection and TLS work, not only packet RTT. |
| `rtt_spread` | Nearest-rank health-check-time `(p90 - p50)` in `25ms` buckets; unknown with fewer than eight samples. This is check-time spread, **not packet jitter**. |
| `udp` | Generic UDP evidence: working, unknown, failed. |
| `quic` | Separate UDP/443 evidence: working, unknown, failed. Generic UDP success does not prove QUIC reachability. |
| `outages` | Outage episodes per effective at-risk hour: class 0 below `0.05/h`, class 1 below `0.25/h`, class 2 below `1/h`, otherwise class 3. Evidence is unknown until effective at-risk exposure reaches `2h`, even when episodes have already occurred. |

Unknown reachability evidence sits after known-good and before known-bad evidence. Measured-download ordering has an explicit exception: any measured result precedes an unknown result. Thus, with target `8 Mbit/s`, a measured `3 Mbit/s` member ranks ahead of an unmeasured member in the below-floor download fallback. The unmeasured member is still eligible for safe exploration. In balance profiles, where `speed_floor` comes first, the fastest acceptable measured member wins when no member meets the target; for example, `20 Mbit/s` beats `3 Mbit/s` at a `25 Mbit/s` target even if the latter is more stable. A custom rank or `realtime-calls` still honors every criterion before `speed_floor`.

An outage episode opens when a previously usable healthy or suspect member becomes quarantined. It closes after three successful TCP probes spaced at least `20s` apart, with no failure between them. A failure resets this recovery count, not the episode count. Both the open episode and its recovery count persist across restarts; the health state machine remains unchanged, so a member can become usable before its episode closes. Failed probation and repeated quarantines before confirmed recovery remain one episode. Episodes and exposure are recorded at every distinct network-path level. Ranking uses only the most specific level with at least `2h` of effective exposure, falling back to a parent if necessary; overlapping levels are never added together. Matching imported evidence contributes before this exposure check. Decay uses a `72h` half-life; decay and imported-history trust scale episode count and exposure equally, preserving their ratio while reducing evidence mass. Stall signals are not a ranking criterion.

Exposure, the outage-rate denominator, accrues only while the member is healthy or suspect and usable, the group is running and unpaused, and it is on that path. Quarantine, probation, paused, offline, stopped, and other-path time does not count. Total observed time is tracked separately for diagnostics only. If an open episode's member and path remain unobserved for more than `30m`, the next observation censors that episode: it ends without recovery success, its episode count remains, and a later loss starts a new episode. A short restart with at most `30m` unobserved preserves both the open episode and its recovery count.

When `udp`, `rtt`, or `jitter` appears in the rank, the profile engine refreshes a bounded generic-STUN pool: the standby, the next ranked eligible member, and the pinned pending improvement candidate until its confirmation finishes or is discarded. The same fixed-pool and rotating exploration rules apply when `quic` appears, using separate UDP/443 probes. Generic UDP refresh runs every `20m` against a `30m` TTL; QUIC refresh runs every `8m` against a `10m` TTL. Fixed members refresh below TTL with slack. A cancelled, preempted, or dropped fixed-pool refresh retries after `30s`, then `60s`, then `2m`, until it succeeds or the evidence deadline passes. Queuing a job does not count as refreshing evidence. With `rtt` or `jitter` ranked, the primary also receives a STUN burst every `2m` under demand.

One additional exploration slot rotates through the other healthy same-tier members in configuration order, first those meeting `min_stability`. Meeting-floor exploration runs normally; below-floor members join the rotation only when no existing fallback is accepted by the forwarding policy. For QUIC, an accepted fallback is healthy, at the primary's tier, has positive UDP/443 evidence, and is no more than one throughput class worse than the primary; for generic UDP, it is healthy with positive generic-UDP evidence. This keeps forwarding-compatible fallbacks discoverable below the soft stability floor. Exploration is best effort: its evidence may expire to unknown between visits, and maximum revisit is the rotation-member count times the refresh interval, subject to the bounded queue and priorities.

Demand-triggered UDP/443 discovery uses the same pool, probes due fixed members, and advances the rotating slot at most once per `10m`. Legacy discovery is unchanged.

Each generic STUN burst sends five Binding requests `200ms` apart and retains valid reply RTTs, up to `64` per member and in per-path history. Every sample keeps its own timestamp. Ranking uses only samples less than `6h` old, with the same filtering after restore; samples without a timestamp are unknown. A completed burst with at least three valid replies supplies one RTT confirmation observation, using that burst's median. A jitter confirmation schedules two consecutive bursts, starting the second `1s` after the first completes, and requires at least ten valid RTTs from that fresh cohort; its value is cohort p90 minus p50. An RTT or jitter confirmation observation must itself be less than `30m` old. A later partial burst does not refresh an earlier observation's timestamp. QUIC version-negotiation RTTs remain separate diagnostics.

#### Profile switching and degradation

An improvement compares bucketed criteria in order. The first differing rank entry is decisive. A download comparison, including the embedded download bucket when both members miss `speed_floor`, or an upload comparison, also requires `speed_margin`. An embedded download comparison reports decisive criterion `speed_floor`. Confirmations require the same challenger to remain better on the same criterion, new relevant evidence, and at least `confirmation_interval` between observations. A switch needs the configured confirmation count and primary dwell.

Confirmations schedule evidence for their decisive criterion: a download phase for `download`, `speed_floor`, or `throughput`; an upload phase for `upload`; one STUN burst for `rtt`; and a two-burst cohort for `jitter`. Direct-measurement observations must independently prove that criterion's improvement against the primary's current effective value. TCP heartbeats never advance upload, RTT, or jitter confirmations. History-fed criteria (`stability`, `outages`, `latency`, `rtt_spread`, `udp`, and `quic`) require a new relevant probe and a freshly recomputed estimate that still supports the improvement.

Download and upload confirmation plans obey the daily ledger and the `30` starts/hour bug guard, bypass regular per-member spacing, and require every lease idle for at least `2s`. STUN-only confirmations need neither a data reservation nor an idle window. Budget, rate, or busy denials leave confirmations pending rather than resetting them; pending evidence must remain fresh. All admitted measurement plans survive routine heartbeat preemption. Pause, power-save activation, network-generation or provider membership changes, shutdown, the member leaving healthy state, or the plan deadline cancel them. One deadline covers admission, queueing, and execution: `30s` plus each phase's `10s` dial allowance, warmup, window, and `5s` tail, plus `5s` per STUN burst and the `1s` gap between cohort bursts. Health-worker priority is unchanged.

Pending confirmations exist only in memory and are never persisted or restored. They are scoped to the network generation, configuration/provider membership generation, candidate identity, and primary epoch. A change to any of these discards pending confirmations; existing job tokens reject late results.

Degradation v2 uses real-traffic read bins only to trigger active tests. For profile policies, read rate below `(1 - hysteresis) × target` for at least `20s`, with at least `128 KiB` transferred during demand, triggers a two-phase plan: A1 measures the primary, then C measures a preselected challenger. Only test bytes count; user bytes are never added. These measurements under load prove lower bounds, not standalone speed ordering.

A1 at or above `(1 - hysteresis) × target` ends with `primary_fast`. Otherwise the primary is underdelivering now. C at or above the full target proves that challenger meets the floor despite competition. A profile switch requires `switch.improvement: true`, `speed_floor` in the rank, and a comparison decided at `speed_floor`; every earlier criterion must be equal or better for the challenger. Dwell is waived, but tier, health, and ranking are not. Without `speed_floor` (including `speed-max` and `speed-upload`), an episode never switches by speed magnitude. Outcomes are `switched`, `primary_fast`, `challenger_slow`, `no_floor_criterion`, or `inconclusive`.

Only a completed plan commits floor changes: slow A1 revokes the primary's floor, and target-meeting C proves the challenger's floor. The newest per-network/member record replaces the previous one and survives restart. Revocation remains until a newer regular idle download meets the **full** target; a below-target result inside the hysteresis band does not clear it. Proof lasts less than `24h`, requires a proven target at least as high as the current target, and yields to newer regular download or floor state. Budget aborts and cancellations write no floor state. While the primary is revoked, pre-revocation candidate downloads are unknown for floor and speed comparisons, and all older downloads become stale for scheduling.

A1 and C are stored separately as `download_under_load`, never replacing regular download magnitudes or counting as ordinary confirmations. Ordinary improvement switches are suppressed from A1 admission until the episode ends. The profile engine makes no shared-bottleneck diagnosis. The whole A1+C plan must fit admission before either phase starts; no separate degradation allowance or target-derived cap exists. Regular idle measurements still require every lease idle for at least `2s`.

Failover chooses the first eligible member in profile order. Tier restoration retains its configured healthy hold and is skipped when disabled. Existing connections remain on their selected member.

Status includes `preference: {profile, rank, min_stability, target_mbps, streams, improvement}`. Relevant member fields include `download_bucket`, `meets_floor`, and `outages: {episodes, exposure_hours, class}`.

#### App policy overrides and status

The gomobile API exposes `OverrideOptions.SmartPolicy string`, `SmartProfiles() (string, error)`, and `CheckSmartPolicy(json string) error`.

An empty `SmartPolicy` preserves the subscription configuration. A nonempty value is an object containing only `preference`, `throughput.target_mbps`, `throughput.enabled`, `throughput.streams`, the three profile-owned probe intervals, `interrupt_on_failover`, and `udp.quic_policy`. After parsing the subscription normally, the service resets `preference` and all eight profile-owned fields, including their presence metadata, on every `type: "smart"` outbound, then applies the override's explicitly supplied values. Other settings, including budgets, URLs, measurement windows and warmups, upload enablement, probe timeouts, member lists, and non-Smart outbounds, remain as parsed. For example, `{"preference":{"profile":"balance-speed-4k"}}` replaces a subscription's explicit `target_mbps: 8` with `25` and resets its stream count to `1`. Invalid override JSON, unknown fields, null values, and invalid policy values fail with `smart policy override: <reason>`; `CheckSmartPolicy` applies the same validation.

The subscription JSON is never rewritten. Its normal parser alone handles comments, case-insensitive field aliases, duplicate keys, and object merging, so applying an override cannot change the effective values of unrelated settings.

The override string itself must be a plain RFC 8259 JSON object: comments, trailing commas, duplicate keys at any depth, and noncanonical key spellings are rejected. Override keys are exact-case, including all `preference` and `preference.switch` leaves. Both `CheckSmartPolicy` and service startup use the same validator; errors begin with `smart policy override:` and name the invalid key path. These strict rules do not change the subscription parser's support for comments or case-insensitive fields.

`SmartProfiles` returns a JSON array in the built-in table order, with `speed-upload` immediately after `speed-max`. Each object contains `name`, `rank`, `min_stability`, `target_mbps`, `throughput_enabled`, `streams`, `heartbeat_interval`, `standby_interval`, `background_interval`, `interrupt_on_failover`, `quic_policy`, `improvement`, `dwell`, `confirmations`, `confirmation_interval`, `tier_restore`, `hysteresis`, and `speed_margin`. Values come from the core resolver; durations are Go duration strings and numeric values are JSON numbers.

The status `preference` object also includes:

- `engine`: `legacy` or `profile`.
- `base`: the built-in profile name, or `default` for a Classic-based custom policy.
- `custom`: true only on the profile engine when a preference leaf other than `profile`, or a profile-owned existing field, was explicitly supplied. Presence counts even when the value equals its default; an empty `switch` object does not count. Legacy policies always report false.
- `source`: `override` when a nonempty app override was applied, otherwise `config`.
- `override_hash`: lowercase SHA-256 hex of the exact applied `SmartPolicy` string, including whitespace; empty when none was applied. This metadata is internal and cannot be supplied in configuration JSON.
- `resolved`: the full effective policy in the override object shape, with every value explicit, suitable for `CheckSmartPolicy` and for initializing an editor. It includes `preference.profile`, `rank`, `min_stability`, `speed_floor_hysteresis`, all six `preference.switch` fields, all three profile-owned throughput fields (including `streams`), all three probe intervals, `interrupt_on_failover`, and `udp.quic_policy`.

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

Smart contains its own probe, throughput, scheduler, persistence, callback, and synchronous-entry panics. A contained probe is **no data**, not a successful check or a member failure: queued and in-flight state is cleared without suspecting or quarantining the member. URL-test callers receive a containment error rather than a fabricated latency. A contained measurement panic conservatively charges its entire persisted reservation.

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

The daily ledger is shared by every Smart group in the process. If groups configure different limits, the effective limit for each network type is the minimum of their limits. Tests are serial. A `30` starts/hour fuse guards against scheduler bugs; regular plans have at least `10m` spacing per member, while confirmations follow `confirmation_interval`. No tests start while paused or in power-save mode. Ordinary calibration requires every lease idle for at least `2s`; degradation plans may run under user traffic.

Before admission, an immutable plan fixes streams, warmup, window, and required directions. Its estimated charged usage is the sum over planned phases of `rate × (warmup + window) / 8 × 1.25 × 1.1`. Download rate is the greater of the last valid active download and target, or at least `25 Mbit/s` without history. Upload uses its last valid upload, or `1 Mbit/s` without history. Only planned phases count: upload-disabled and download-confirmation plans omit upload, upload-only plans omit download, and a degradation plan estimates both sequential downloads. Admission requires remaining charged budget at least equal to that estimate.

An admitted plan reserves and persists the **entire remaining daily charged budget**, not merely its estimate. This single reservation spans every phase and is reconciled once to actual metered bytes plus `10%` outer-overhead accounting. A shared atomic allowance of `floor(remaining / 1.1)` raw bytes is allocated before reads and writes across all streams. If exhausted, the test aborts with `speedtest aborted: budget`, discards all results, and charges completed operations. There is no per-test byte cap, target-derived quota, overshoot allowance, or separate degradation budget. Adaptive individual HTTP request sizes do not limit total test traffic.

**After a crash or contained measurement panic, the persisted reservation is charged in full. This can consume the day's entire remaining budget even when the test used less.** Conservative recovery prevents unaccounted traffic; older reservation records retain their original meaning. Normal cancellation reconciles known usage. Reservations become durable before execution, and queued plans recheck pause, power, network, health, and traffic gates before starting. Disabled throughput, absent cache, and zero daily budget do not enter admission. A member whose estimate does not fit backs off until the next local midnight or a budget/evidence change; the scheduler continues searching other members. Group-wide exhaustion is reported only when no eligible member plan fits.

Daily accounting uses local time. Clock rollback retains the latest accounting day already seen and grants no new budget. These budgets cover active throughput tests, not ordinary user traffic or small health probes. A network handover with uncertain attribution charges consumed bytes to cellular.

Download and upload results carry independent timestamps, validity, source, streams, and windows. Each active direction expires after `24h` and restores using its own age; a download-only result never updates upload age or vice versa. Regular scheduling includes unknown or stale download and, when `upload` is enabled, unknown, stale, or previously inconclusive upload. Thus fresh download cannot block an upload-only plan. An inconclusive upload keeps its previous valid result.

Old `download_mbps` cache data remains active download at its original timestamp. Every pre-v2 upload is unknown for ranking because it lacks receipt provenance, including positive values; a positive old value is kept only as `legacy_upload`, and a verified upload is scheduled when enabled.

Measurement status retains `download_mbps` and `upload_mbps`, with independent `download_at` and `upload_at`. Additive fields are `download_p10_mbps`, `download_mean_mbps`, and `upload_post_min_mbps`. The `download` and `upload` objects expose `value`, `streams`, `window` (seconds), `source`, `at`, `valid`, and `fresh`; download additionally has `p10` and `mean`, and upload has `post_min`. Status timestamps are Unix seconds. `upload_status` is `unknown`, `verified`, `unverified`, or `inconclusive`; group-level `upload_verified: false` identifies an endpoint lacking receipts.

`download_under_load` uses the download object shape. A recorded `floor_state` exposes `kind`, `target`, `at`, `value`, and `effective`, where `effective` describes the record for the current target and time. `legacy_episode_obs` exposes `value` and `at`; `throughput_source: "under_load"` identifies when the legacy class uses it rather than the regular download. The removed passive source never appears. Known packet estimates appear as `udp_rtt_ms` and `jitter_ms`; `quic_rtt_ms` is a separate diagnostic. `observed_peak_down` and `offered_up` are diagnostic Mbit/s values. Group-level `budget_exhausted` is true only when no eligible regular plan fits.

Real-traffic observations are **diagnostic and trigger-only**, never ranking credit, floor proof, improvement confirmation, or a reason to skip an active test, on any engine. A per-member `1s` read-byte ring drives degradation triggers and `observed_peak_down`, the best `5s` read mean in the last `6h`. The analogous write diagnostic is `offered_up`. Status and insights label these as “observed during use”; they are never compared with targets. Buffered reads and locally accepted writes do not prove delivered capacity.
These diagnostics describe traffic observed since the VPN started on this network. Their in-memory ring resets after a restart or network change; it is not persisted.

Download uses parallel HTTP/1.1 GET streams. Each begins with `1,000,000` bytes and adapts subsequent request sizes toward `2s`, within `1 MB` to `25 MB` per request. Completed requests reach EOF for connection reuse. Warmup begins at the first response headers; measurement uses aligned `1s` bins after warmup, excluding a partial final bin. `download` is the median bin, with `download_p10` and `download_mean` diagnostics. At least five complete bins and bytes from every stream are required. At window end, requests stop and in-flight work is cancelled; trailing bytes are charged but not ranked.

Upload uses parallel POST streams, bootstrapping at `65,536` bytes from the uplink itself and adapting toward `1s` per verified POST, between `64 KiB` and `25 MB`. Each observation runs from the first written body byte through the final matching receipt; delayed responses can only lower the reported rate. The measured set contains POSTs started after upload warmup and before window end. `upload` is their total verified bytes divided by the span from earliest start to latest final response, aggregated across streams. At least three POSTs must qualify, and every POST in the set must complete with its matching receipt. A `5s` tail permits in-flight completion; any unverified, cancelled, or mismatched member makes the phase inconclusive. `upload_post_min` is a diagnostic minimum per-POST rate multiplied by streams; no synthetic upload median or p10 is reported.

#### Degradation checks

The same measurement and accounting rules apply to legacy `default` and profile engines; profile floor semantics are described under [Profile switching and degradation](#profile-switching-and-degradation).

Demand read bins below the threshold for at least `20s`, with at least `128 KiB` transferred, trigger a plan. Legacy keeps its half-target threshold and decision predicates; profile policies use `(1 - hysteresis) × target`. Passive bins cannot prove saturation or capacity. A1 tests only the primary's test bytes, and C tests only the challenger's test bytes while user traffic continues.

The challenger is selected before admission, and the complete two-download plan must fit the estimate. No A1 runs and no floor state changes when the plan cannot be admitted. There are no target-derived proof quotas, separate degradation allowances, or per-test data caps. Admission, accounting, lifecycle cancellation, and the plan-derived deadline are the same as other measurement plans.

For legacy decisions, each valid A1 or C is also persisted separately as a per-network/member episode observation, including `primary_fast` outcomes. The legacy throughput class uses the newer of that observation and regular active download, each with its own `24h` age. This preserves legacy class decisions without overwriting regular download. Episode observations never feed profile speed-magnitude comparisons or replace profile floor state.

Budget, cooldown, or eligibility blocks mean there is no universal switching deadline. Containment `degraded` remains separate from throughput degradation.

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

The harness covers selection, UDP reachability, budget accounting, restart persistence, blackhole failover, and panic containment with local protocol fixtures. Measurement-v2 unit fixtures use paced local servers and fake clocks for receipt validation, directional age, budget aborts, and floor decisions. No Internet speed test is required.

Configured outer link rates include WireGuard framing; measured application throughput is lower. Scenarios run serially to avoid race-instrumented packet-processing contention. WireGuard keeps a `1280`-byte MTU.

The shared downstream limiter serializes packets from their relay arrival timestamps. Late timer wakeups consume elapsed link time rather than adding another per-packet delay, so timer granularity does not silently reduce the configured capacity.

Measurement plans are not cancelled by routine heartbeat preemption; health-worker priority remains reserved. The degradation scenarios use the production adaptive probe timeout. The real-component suite can be run locally with:

```sh
GOTOOLCHAIN=go1.26.8 go test -race -count=3 -timeout=40m -ldflags=-checklinkname=0 \
  -tags with_gvisor,with_quic,with_dhcp,with_wireguard,with_utls,with_acme,with_clash_api,with_tailscale,with_ccm,with_ocm,badlinkname,tfogo_checklinkname0 \
  ./protocol/group/smartharness/
```
