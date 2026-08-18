# Security model of a serverless network

mikall has no servers. That removes every guarantee a trusted operator
normally provides, and this document states the consequences honestly. The
UI links to a plain-language version of this at onboarding.

1. **Identity is a keypair, nothing more.** There is no registration
   authority and **no account recovery**: lose the key, lose the identity;
   leak it, and the thief *is* you. Impersonating a *name* is trivial —
   nicknames are labels, never global truths. Mitigations: TOFU key pinning
   with loud `ContactKeyChanged` alarms, out-of-band fingerprint
   verification (UI dialog, IRC `WHOIS`), an encrypted keystore backup
   prompt at first run. Nothing restores a lost key.

2. **Sybil attacks.** Identities are free, so one attacker can be 10,000
   "users": flooding, DHT keyspace domination, out-voting any naive trust
   scheme. Partial mitigations only: never derive trust from peer counts,
   per-peer rate limits; proof-of-work join tickets and invitation graphs
   are documented future work. This is a fundamental, unsolved cost of
   serverless design.

3. **Eclipse attacks.** An attacker surrounding a node's Kademlia
   neighborhood can censor discovery or serve poisoned records.
   Mitigations: signed DHT records (poison is detectable), diverse lookup
   paths, peer-diversity heuristics, mDNS/LAN and manually added bootstrap
   peers as escape hatches. Not fully solvable in v1.

4. **IP exposure — the big one.** Every peer you gossip, DM, or call with
   learns your IP address, and DHT participation broadcasts your presence.
   Consequences: doxxing, targeted DoS, correlating a pseudonym with a
   household. Discord hides this behind its servers; mesh voice calls make
   it worse — every participant sees every other participant's IP.
   Mitigations: a prominent warning before first connect, a "relay-only /
   hide my IP" mode (latency cost; relays still observe you), call UI
   labeled "direct connection reveals your IP". Tor/mixnet transport is
   future work. **Pseudonymity here is shallow, and the UI says so.**

5. **Metadata leakage.** Even with E2E-encrypted payloads, gossipsub reveals
   who subscribes to which topics and when they talk; DHT lookups reveal
   interest in a channel before joining it. Channel membership graphs are
   effectively public to a monitoring adversary. Mitigations: DM size
   padding buckets, topic ids derived from channel keys, presence beacons
   only inside joined channels. The residual leak is accepted and stated.

6. **No central moderation.** Nobody can delete content network-wide, ban
   anyone globally, or honor takedowns. Spam, abuse, and illegal content
   propagate; relaying peers may unknowingly forward encrypted illegal
   payloads and blob caches may hold them. Mitigations: local blocklists
   (applied at gossipsub scoring), shareable blocklist subscriptions,
   channel-op signed ban/mute records that honest clients enforce —
   **unenforceable against modified clients, stated plainly** — and no
   auto-preview or auto-accept of media from unverified contacts.

7. **Availability = online peers.** No server means no authoritative
   history. If nobody holding a message is online, it is gone; late joiners
   see whatever online peers can backfill; DMs to offline peers queue
   locally and retry. Store-and-forward exists only among channel
   co-members (DAG backfill); friend-peer mailboxes are post-v1. Deletion
   is advisory: other devices hold your (encrypted) messages indefinitely.

8. **Relays observe traffic patterns.** Volunteer circuit relays cannot
   read Noise+E2E payloads but see endpoints, volumes, and timing — and can
   selectively deny service. Mitigations: rotate relays, prefer DCUtR hole
   punching so relays are only a bootstrap path, flag relayed sessions in
   the UI.

9. **The localhost IRC gateway is plaintext.** End-to-end encryption
   terminates at the node; the gateway re-emits plaintext on 127.0.0.1 for
   legacy IRC clients. It binds loopback **only** (the `IrcBindAddr` value
   object cannot represent a non-loopback address), supports an optional
   `PASS`, ships disabled until enabled in settings, and warns at startup
   that other local processes and users on a shared machine can connect —
   set the password there.

10. **Supply chain and updates.** No server also means no trusted update
    channel: releases are minisign-signed and verified before any
    self-update (never auto-executed), the lockfile is committed, git
    dependencies are banned, and `cargo-deny`/`cargo-audit` run in CI. A
    compromised popular build would be a network-wide E2E bypass — treat
    release keys accordingly.

11. **DoS resistance is per-peer.** There is no cloud edge to absorb
    floods. Every node defends itself: per-peer quotas (messages/s, sync
    requests/s, chunk requests/s), gossipsub peer scoring with graylisting,
    bounded queues everywhere, a 64 KiB envelope ceiling, and connection
    caps.

12. **Local data at rest.** The keystore is encrypted (argon2 +
    XChaCha20-Poly1305) behind an optional passphrase; message history is
    not encrypted at rest in v1 (documented — use OS full-disk encryption).

## Trademark note (not security, but a shipping gate)

"Hatsune Miku", her likeness, and official artwork are Crypton Future Media
IP; the Piapro character license does not cover a software mascot implying
endorsement. mikall ships an original teal (#39C5BB) theme and an original
mascot only — no Crypton assets may enter this repository.
