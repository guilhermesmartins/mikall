# mikall architecture

mikall is a fully decentralized (serverless, P2P) chat application in 100%
Rust: native GUI (iced), an embedded RFC 1459/2812 IRC gateway on localhost,
libp2p networking, E2E-encrypted DMs, P2P voice, screen sharing, and file
transfer. No JavaScript anywhere.

## Hexagonal layout

```
                 driving adapters                 driven adapters
  ┌────────────┐                ┌───────────────┐                ┌──────────────┐
  │ mikall-ui  │──┐          ┌──│  mikall-app   │──ports (traits)│ mikall-net   │ libp2p
  │  (iced)    │  ├─services─┤  │  use cases    │────────────────│ mikall-store │ redb
  ├────────────┤  │          │  │  + ports      │                │ mikall-crypto│ keys/E2E
  │ mikall-irc │──┘          └──│               │                │ mikall-media │ voice/video
  │ (gateway)  │                └───────┬───────┘                └──────────────┘
  └────────────┘                        │
                                ┌───────┴───────┐
                                │ mikall-domain │  pure: no I/O, no async
                                └───────────────┘
```

- `mikall-domain` — value objects, aggregates, domain events. Depends only on
  `thiserror`; CI fails if anything else enters its dependency tree.
- `mikall-app` — use-case services (`ChatService`, `DmService`,
  `IdentityService`, `PresenceService`, `CallService`, `TransferService`) and
  the driven-port traits (`ChatTransport`, `Directory`, `MessageStore`,
  `KeyStore`, `IdGen`, `Clock`, …). The `InboundRouter` is the single place
  transport adapters hand in verified traffic.
- `mikall-bdd` — cucumber features driving the services over an in-memory,
  deterministic multi-node universe, plus the fakes themselves.
- `mikall-node` (M1) — composition root wiring production adapters into a
  `NodeHandle` used identically by both frontends.

## Bounded contexts

| Context | Aggregates | Key value objects |
|---|---|---|
| Identity | `Contact` | `IdentityId`, `Fingerprint`, `Petname`, `TrustLevel` |
| Messaging | `Channel`, `DmThread` | `ChannelName`, `Nickname`, `MessageBody`, `Topic`, `MessageId`, `DmKey` |
| Presence | `Roster` | `AwayMessage`, `PresenceState` |
| Calls | `Call` | `CallId`, `CallRoster` (≤8, mesh limit), `CallPhase` FSM |
| FileTransfer | `Transfer` | `FileName`, `FileManifest`, `ChunkBitmap`, `TransferPhase` FSM |

## Negative-space rules

1. Every value object: private fields, one fallible `parse`/`new`, infallible
   accessors — invalid values cannot exist.
2. Remote bytes become domain `Message`s only through
   `Verified::attest_signature_checked()` — every trust-boundary call site is
   greppable and sits next to a real signature check.
3. State machines (`CallPhase`, `TransferPhase`) match exhaustively; the
   `clippy::wildcard_enum_match_arm` lint is `deny` in domain and app.
4. Bounded collections (`CallRoster` ≤ 8, `ChunkBitmap` bounded by manifest)
   enforce their limits in the constructor, not in callers.
5. `debug_assert!`-backed `check_invariants` runs after every aggregate
   command.

## Testing

- Unit + property tests (`proptest`) live beside the domain code: parser
  round-trips, invalid-input rejection, DAG linearization determinism.
- BDD: Gherkin features under `crates/mikall-bdd/tests/features/` run with
  `fail_on_skipped` — an undefined step is a build failure. The World is a
  multi-node universe over an in-memory bus with deterministic delivery
  order; links can be delayed and flushed to manufacture races and history
  gaps.

See `docs/protocol.md` for the wire design and `docs/security.md` for the
threat model of full decentralization.
