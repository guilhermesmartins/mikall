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
| `mikall-bdd` | Cucumber BDD suite + deterministic in-memory multi-node universe. |

Coming per the milestone plan: `mikall-crypto`, `mikall-net`,
`mikall-store`, `mikall-irc`, `mikall-node`, `mikall-media`, `mikall-ui`.

## Developing

```sh
cargo test --workspace     # unit + property + cucumber BDD
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all --check
```

Docs: [architecture](docs/architecture.md) ·
[wire protocol](docs/protocol.md) · [security model](docs/security.md)
