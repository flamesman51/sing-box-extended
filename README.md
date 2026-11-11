# sing-box-extended

[![license](https://img.shields.io/badge/license-GPLv3-blue.svg)](LICENSE)
[![go](https://img.shields.io/badge/go-1.26-00ADD8.svg)](go.mod)
[![codeberg](https://img.shields.io/badge/mirror-codeberg-2185D0.svg)](https://codeberg.org/shtorm-7/sing-box-extended)
[![telegram](https://img.shields.io/badge/telegram-chat-26A5E4.svg?logo=telegram)](https://t.me/sing_box_extended)

Sing-box with extended features.

## Off-phone goroutine debugging

The validation regression runs on Linux without Android, root, a TUN interface,
credentials, or a real relay. It generates temporary WireGuard keys and uses
loopback blackholes. The optional lifecycle lab requires an ignored private
configuration and exercises libbox gRPC status/group subscriptions, Smart status
queries, reconnects, and box shutdown. It rejects TUN, system WireGuard devices,
non-loopback listeners, and services.

```sh
export GOTOOLCHAIN=go1.26.8
tags=with_gvisor,with_quic,with_dhcp,with_wireguard,with_utls,with_acme,with_clash_api,with_tailscale,with_ccm,with_ocm,badlinkname,tfogo_checklinkname0
GOROUTINE_LAB_OUTPUT=/tmp/sing-box-goroutines \
  go test -count=1 -v -ldflags=-checklinkname=0 -tags "$tags" \
  ./experimental/libbox -run '^TestGoroutine(CheckConfig|Lifecycle)Lab$'
```

`TestGoroutineCheckConfigLab` repeats successful and failed configuration
validation with 13 WireGuard endpoints. Before the fix, each successful
validation left 78 gVisor TCP processor goroutines on a six-CPU runtime; VPN
restart could not release them because validation never starts the endpoint's
cleanup scope. Remote profile refresh validates even unchanged content. Device
allocation now happens in `Start`, while constructor validation remains
resource-free. Twenty successful validations previously grew from 4 to 1,564
goroutines on a six-CPU runtime; the fixed validation loop stays at its baseline.
Device access is published only after startup completes. Dials and packet
listeners before startup or after shutdown return `WireGuard device is not ready`;
invalid non-IP destinations retain their existing validation error. Local-address
validation and missing-gVisor errors still fail construction, while operating
system device creation errors occur during startup.

For a longer lifecycle experiment, set `GOROUTINE_LAB_CONFIG`,
`GOROUTINE_LAB_CYCLES`, `GOROUTINE_LAB_DWELL` (a Go duration), and
`GOROUTINE_LAB_EVENTS=1` to exercise device pause/wake and network resets. The
output directory receives active profiles per cycle, a final stopped profile,
and the validation profile. Shutdown waits up to five seconds for asynchronous
workers to return within five goroutines of baseline.

`GOROUTINE_LAB_CONFIG` points to an ignored private JSON file, for example under
`build/`. An unset variable or missing file skips the lifecycle test; the
synthetic validation regression still runs.
WireGuard tests require a separate device/key, never a phone's or another
active client's key. A loopback mixed inbound plus
`GOROUTINE_LAB_PROXY=http://127.0.0.1:21880` enables repeated HTTPS traffic;
`experimental.debug.listen` on loopback exposes `/debug/pprof/goroutine?debug=1`
during the run. Do not reuse a production TUN configuration.

```sh
GOROUTINE_LAB_CONFIG="$PWD/build/leak-lab.json" \
GOROUTINE_LAB_OUTPUT=/tmp/sing-box-goroutines \
GOROUTINE_LAB_CYCLES=2 GOROUTINE_LAB_DWELL=30s GOROUTINE_LAB_EVENTS=1 \
  go test -count=1 -v -ldflags=-checklinkname=0 -tags "$tags" \
  ./experimental/libbox -run '^TestGoroutineLifecycleLab$'
```

Compare stack counts, not only the process total: a running WireGuard endpoint
normally owns several gVisor and crypto workers. Profiles can contain private
paths and connection metadata; inspect locally and redact before sharing.

## Integration tests

The `test/` directory is a separate Go module. It uses this checkout of the core
and the same published dependency forks as the main module; sibling checkouts
are not required. From the repository root:

```sh
export GOTOOLCHAIN=go1.26.8
tags=with_gvisor,with_quic,with_dhcp,with_wireguard,with_utls,with_acme,with_clash_api,with_tailscale,with_ccm,with_ocm,with_trusttunnel,with_openvpn,with_openconnect,badlinkname,tfogo_checklinkname0
(cd test && go test -count=1 -v -timeout=15m -ldflags=-checklinkname=0 -tags "$tags" .)
```

This tag set covers the tested protocols without generated admin-panel assets.
The optional `with_naive_outbound` tests additionally require the
[Cronet build toolchain](https://github.com/sagernet/cronet-go#linux-build-instructions)
on Linux; they are not required for the portable integration run.

Use `-run '^$'` to compile without running tests. The package initializer still
checks Docker and pulls missing interoperability images when a daemon is
available. Docker interoperability tests require a daemon and image downloads;
OpenVPN/OpenConnect container tests also need container TUN/NET_ADMIN support.
Do not run them against a production VPN or grant the test process root access.
The self-to-self and endpoint UDP NAT tests use loopback and userspace network
stacks, not host TUN devices.

Brutal tests require the `tcp-brutal` kernel module; kTLS tests require kernel
TLS support to be loaded. Missing kernel support is an environment failure,
not a reason to load modules or change the workstation's network setup.

For an offline run, add
`-skip '^(TestTUICDomainUDP|TestReality|TestKTLSReality|TestBrutalVLESS)$'`:
these tests contact external QUIC or Reality handshake servers. Without Docker,
legacy interoperability tests report daemon connection failures, while newer
OpenVPN/OpenConnect tests skip missing prerequisites. OpenConnect Docker
interoperability additionally requires `OPENCONNECT_IT=1`.
Keep these environment failures separate from self-to-self protocol failures.

## Formatting

Run `golangci-lint fmt ./...` from the repository root to apply the gci and
gofumpt settings in `.golangci.yml`. Check formatting without changing files
with `golangci-lint fmt --diff ./...`.

## 🔥 Features

### Protocols
- **WARP** — Cloudflare WARP integration through WireGuard
- **MASQUE** — Cloudflare MASQUE proxy over QUIC / HTTP-2
- **MTProxy** — Telegram MTProxy server with FakeTLS and domain fronting
- **Mieru** — Secure, hard to classify, hard to probe network protocol
- **TrustTunnel** — AdGuard's obfuscated VPN protocol, indistinguishable from HTTPS traffic
- **Sudoku** — Traffic obfuscation protocol based on 4×4 Sudoku puzzles with low-entropy fingerprints
- **SSH** — SSH client and server with certificate authentication and upstream fallback
- **Call** — Traffic tunneling through video-call platforms (VK, Dion, Telemost, WBStream)
- **VPN** — Routed tunnel over any sing-box protocol
- **Bond** — Link aggregation for increasing throughput
- **Fallback** — Outbound group with priority-based switching
- **Failover** — Automatic outbound switching with session recovery for high availability

### DNS
- **SDNS (DNSCrypt)** — Encrypted DNS queries for enhanced privacy
- **DNS Fallback** — Sequential / parallel queries across upstream resolvers

### Limiters
- **Bandwidth Limiter** — Upload / download / bidirectional rate limiting
- **Connection Limiter** — Concurrent connection control
- **Traffic Limiter** — Per-user traffic quotas
- **Rate Limiter** — Request rate limiting

### Encryption & Obfuscation
- **Amnezia 3.1** — WireGuard traffic obfuscation
- **VLESS encryption** — XRAY encryption for VLESS protocol

### Transports
- **mKCP** — Reliable UDP-based transport
- **XHTTP** — Modern XRAY transport
- **Rmux** — Improved smux multiplexer

### Services
- **Admin Panel** — Web-based management interface
- **Manager** — Management service for configuring users, nodes, limiters
- **Manager API (HTTP/gRPC)** — HTTP and gRPC API for the Manager
- **Node Manager API** — Service for connecting nodes to remote manager

### Miscellaneous
- **Providers** — Outbound subscriptions from local files, inline lists, or remote URLs (sing-box JSON, Clash YAML, SIP008, share links)
- **Link Parser** — Outbound configured from a share link (VLESS, VMess, Shadowsocks, Trojan, Hysteria, Hysteria2, TUIC, AnyTLS)
- **Extended WireGuard options** — Advanced configuration capabilities
- **Unified Delay** — Unified latency measurement

## 📚 Examples

Configuration examples are available here:

https://github.com/shtorm-7/sing-box-extended/tree/extended/examples

## Support the Project

If you want to support the project, you can donate to the following addresses.

#### Tribute

**[RUB Donate](https://web.tribute.tg/d/JxY)**

**[EUR Donate](https://web.tribute.tg/d/JxZ)**

**[USD Donate](https://web.tribute.tg/d/Jy1)**

#### TRX (Tron)
```
TSWU6VUZ4FcUghYDmbbhK15gRVvhvBgW3F
```
#### TON
```
UQAyD2UuT5kCP6lZQlhFL0hyNibDXNE4nIo_RSLVSYAtD7N1
```
#### Solana
```
CJu8ickwRCwNE71uVFjYf1UveyCkRp9Xo44rhPcQpeFL
```
#### Bitcoin
```
bc1qqx97p8k4dchqkyd47s4vf74hrqdfnmhqvcja7x
```
#### Ethereum
```
0xAcc5919C22F2B3fAa0ec7E8BaD142da5B375FBF6
```

## License

```
Copyright (C) 2022 by nekohasekai <contact-sagernet@sekai.icu>

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.

In addition, no derivative work may use the name or imply association
with this application without prior consent.
```
