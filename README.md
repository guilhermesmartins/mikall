# mikall

A fully decentralized, Miku-inspired alternative to Discord that is **also a
real IRC server** — written entirely in Rust, with no JavaScript anywhere
and no servers anywhere.

- **Serverless P2P**: peers form the network themselves (libp2p — Kademlia
  DHT, GossipSub, mDNS, DCUtR hole punching, peer-run relays).
- **Two frontends, one core**: a native iced GUI and an embedded
  RFC 1459/2812 IRC gateway on `127.0.0.1` — WeeChat and irssi speak to the
  same use cases the GUI does.
- **Text channels, DMs, file sending, voice calls, screen sharing** (mesh
  calls, capped at 8 — an SFU would be a server).
- **Engineering style**: Domain-Driven Design, hexagonal architecture,
  negative-space programming (illegal states unrepresentable), BDD with
  cucumber + Gherkin feature files, unit + property tests.

Read the honest threat model first: [docs/security.md](docs/security.md) —
serverless has real costs (IP exposure, no moderation authority, no account
recovery), and mikall states them instead of hiding them.

## Workspace

| Crate | Role |
|---|---|
| `mikall-domain` | Pure domain core: value objects, aggregates, events. No I/O, no async. |
| `mikall-app` | Use-case services + the hexagon's ports. |
| `mikall-crypto` | Ed25519 identity custody, envelope signing, BLAKE3 content ids. |
| `mikall-net` | libp2p: gossipsub channels, Kademlia directory, mDNS, signed DM/offer/call envelopes, blob + media protocols. |
| `mikall-store` | redb message store + content-addressed blob store. |
| `mikall-irc` | The RFC 1459/2812 gateway on loopback (WeeChat/irssi speak to your node). |
| `mikall-media` | Sealed media frames (ChaCha20-Poly1305), jitter buffer + PLC, voice pipeline; Opus behind `--features hardware-audio`. |
| `mikall-node` | Composition root (`NodeHandle`) + the `mikalld` headless REPL binary. |
| `mikall-ui` | The native iced GUI (`mikall` binary), miku_teal theme. |
| `mikall-bdd` | Cucumber BDD suite + deterministic in-memory multi-node universe. |

Try it on a LAN: run `mikalld` on two machines, `/join #stage` on both, and
chat — or point WeeChat at `127.0.0.1:6667` with `MIKALL_IRC=6667` set.
`/send`, `/call`, and `/share` exercise file transfer and call signaling.

## Developing

```sh
cargo test --workspace     # unit + property + cucumber BDD
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all --check
```

Docs: [architecture](docs/architecture.md) ·
[wire protocol](docs/protocol.md) · [security model](docs/security.md)
