# mikall — UI Design Brief

> Paste this whole document into Claude Design and ask for a new layout.
> Ready-to-use prompts are at the bottom. Everything here describes the
> real, working application — every screen, state, and string maps to
> shipped functionality.

---

## 1. Product snapshot

**mikall** is a fully decentralized alternative to Discord, written in Rust,
that is *also* a real IRC server. There are no servers anywhere: peers form
the network themselves (libp2p — DHT, gossip, mDNS). Every node additionally
serves RFC 1459/2812 IRC on `127.0.0.1`, so WeeChat/irssi users sit in the
same channels as GUI users.

- **Features that exist today**: text channels + DMs (causally ordered,
  signed messages), file sending (content-addressed, verified chunks,
  resumable), voice-call signaling with AEAD-sealed media frames, screen-share
  signaling, presence/away, roles (founder/op/member), local blocklists.
- **Tone**: *stagecraft meets terminal honesty*. A concert-stage teal glow on
  a dark room — but the copy never over-promises. Serverless has real costs
  (your IP is visible to peers, there is no account recovery, bans are
  local) and the UI says so plainly instead of hiding it. Think "the
  virtual-diva aesthetic run by people who read RFCs."
- **Audience**: privacy-minded chat users, IRC veterans, self-hosters,
  Vocaloid-adjacent internet culture. Desktop power users.
- **Frontends are peers**: the GUI is one of two equal frontends (the other
  is IRC). The design should feel at home next to a terminal, not embarrassed
  by it.

## 2. Brand & theme tokens (`miku_teal`)

Dark-first. These are the shipped theme tokens — reuse them exactly; extend
with derived tints/shades as needed.

| Token | Hex | Use |
|---|---|---|
| `bg` | `#0E1B1E` | App background (deep blue-green black) |
| `surface` | `#16333A` | Panels: sidebar, roster, cards, inputs |
| `primary` / teal | `#39C5BB` | Brand, active items, links, focus, own accents |
| `accent` / pink | `#FFA1C9` | Sparingly: author names, highlights, warm accents |
| `text` | `#E8F6F5` | Primary text |
| `muted` | `#8CA6A3` | Secondary text, labels, timestamps |
| `danger` | `#FF5C8A` | Errors, key-change alarms, destructive actions |

- Typography: clean geometric/humanist sans for UI; a monospace face is
  **required** for fingerprints, identity hex, channel names in technical
  contexts, and anything IRC-flavored.
- Iconography/mascot: an **original** "teal twin-tail wave" logomark —
  abstract flowing twin ribbons, leek-green/teal. **Hard legal rule: no
  Hatsune Miku name, likeness, or Crypton Future Media artwork anywhere.**
  Thematic inspiration only.
- Motion: subtle; a gentle glow/pulse on the ringing call state is welcome.

## 3. Screen inventory

### 3.1 Onboarding (first run)

Purpose: create the identity and set expectations honestly.

Must contain:
- Wordmark "mikall" + one-liner: *"serverless chat — your identity is a
  keypair on this device"*.
- **Fingerprint display** (monospace, 8 groups of 4 base32 chars, e.g.
  `k3f9-2xqm-...`), with copy "share it out of band so friends can verify
  you". Make it feel like a collectible ID card, not an error dump.
- Nickname input with **live validation** (valid → teal check "looks good ✔";
  invalid → danger hint: "letters first; letters/digits/`[]\`_^{|}`- after;
  max 16"). IRC grammar, enforced by the domain.
- Primary CTA: **"enter the stage"** (disabled until the nickname is valid).
- A quiet but visible key-backup nudge: lose the key file, lose the identity —
  there is no recovery.

### 3.2 Main shell (three panes — the core screen)

Discord-like, but with mikall's honesty and IRC parity.

**Left sidebar (~220 px, `surface`):**
- Wordmark + the user's own fingerprint in tiny monospace.
- "channels" section: `#channel` list; active item in teal; **unread badges**
  (count chips). Channels are IRC-style `#names`, always lowercase.
- "direct messages" section: partner nick (or short identity hex when no
  nick is known) + unread badges.
- Bottom: "join #channel" input (submit joins or *founds* the channel — on a
  serverless network, joining a name that doesn't exist creates it; a micro-
  hint may say "join or found").
- Self area: nickname, presence dot, mute/deafen/settings affordances.

**Center column:**
- **Topic bar**: channel name (teal), topic text (muted), roster-collapse
  toggle. Room for a small "syncing history…" indicator (shown while the
  node backfills missed messages from peers — a real state).
- **Timeline**: messages grouped by author; author name in pink, body in
  text color; timestamps muted ("advisory" wall-clock — ordering is causal).
  Multi-line messages exist. `/me` action messages render italic.
- **Composer**: single input, Enter to send. Max 4096 bytes; no empty sends.
- Empty state (no channel selected): friendly copy "welcome — join a channel
  to start" + the join affordance.

**Right roster (~180 px, collapsible, `surface`):**
- "members" list with **role sigils**: `~` founder (immovable), `@` operator,
  none = member — keep the sigils, they're IRC parity.
- Presence per member: online / away (with optional away message on hover) /
  offline.
- Per-member trust glyph (see §4 TrustLevel) and click-through to a
  **member card**: full fingerprint (monospace), trust level, actions:
  Verify fingerprint, Message, Block.

**Alarm banner (top of shell, above everything):**
- Highest-priority visual in the app: **key-change (TOFU) alarm** in danger
  pink: *"KEY CHANGED for &lt;peer&gt;: new fingerprint &lt;fp&gt;. Verify out of band
  before trusting."* Dismissible but unmissable. The same slot shows send/join
  errors and "file saved to …" notices at lower severity.

### 3.3 Call overlay / panel

Calls are 1:1 or group **mesh, hard-capped at 8 participants** (a domain
invariant — an SFU would be a server). Design for a max 8-tile grid.

States to design:
- **Ringing (outgoing)**: callee avatar/identicon + "ringing…", cancel.
- **Incoming offer**: caller nick + fingerprint-trust glyph, Accept /
  Decline. Should interrupt gently (banner or toast, not a modal takeover).
- **Connecting**: brief transitional state.
- **Active**: participant tiles (nick, speaking indicator, mic-muted and
  screen-sharing badges), controls: mute, deafen, share screen, hang up.
  A small persistent label: *"direct connection — participants can see your
  IP"* (honesty requirement).
- **Ended**: reason (hung up / declined / last participant left).
- When someone shares a screen: their tile expands or a viewer pane opens
  (16:9 video area with sharer nick + "stop watching").

### 3.4 File-transfer cards

Appear inline in DM timelines and in a transfers drawer/panel:
- **Incoming offer**: file name, size, sender + trust glyph, Accept / Reject.
  Files are never auto-accepted.
- **In progress**: chunk progress (`have/total` chunks — real data), pause is
  out of scope, but transfers **resume automatically** after disconnects.
- **Complete**: "saved to &lt;path&gt;" with open-folder affordance.
- **Failed / rejected** states in danger/muted styling.
- Micro-copy may note: every chunk is hash-verified before it counts.

### 3.5 Settings (modest, one screen)

- Identity: fingerprint (copyable), export/backup key file (danger-adjacent
  framing).
- **IRC gateway**: on/off toggle, port (default 6667), optional password with
  helper text: *"the gateway is plaintext on loopback — set a password on
  shared machines"*. It binds `127.0.0.1` only, by construction; the UI can
  state this as a fact, not an option.
- Privacy note block (static copy): IP visibility, no global moderation,
  bans are local. Link-out styling to a fuller security page.
- Blocklist management (list of blocked identities, unblock action).

## 4. Domain-driven UI states (must all be representable)

| Domain concept | States the UI must show |
|---|---|
| **TrustLevel** (per contact) | `unverified` (neutral/warning glyph) · `tofu-pinned` (default after first sight — subtle) · `verified` (teal check) · `blocked` (danger, content hidden) |
| **Presence** | online (teal dot) · away + optional message (amber/muted) · offline (hollow/grey, last-seen) |
| **CallPhase** | ringing · connecting · active · ended (with reason) |
| **Per-participant media** | mic muted · deafened · sharing screen |
| **TransferPhase** | offered · accepted · transferring (chunk progress) · complete · rejected · failed |
| **Channel role** | founder `~` · op `@` · member |
| **History sync** | "syncing history…" while gaps backfill; message timestamps are advisory |
| **Unread** | per-channel and per-DM counts |

## 5. Voice & content rules

- **Honest decentralization**: never imply guarantees the network can't give.
  Approved phrasings: "bans are local on a decentralized network", "no
  account recovery — back up your key", "direct connection reveals your IP
  to participants", "messages live on peers, not servers".
- **Fingerprints** are the root of trust: always monospace, always groupable
  (`xxxx-xxxx-…`, 8 groups of 4), always copyable. Nicknames are labels, not
  identities — the UI must make verifying easy and impersonation obvious.
- Lowercase product voice ("mikall", "enter the stage"); channel names always
  `#lowercase`.
- Keep IRC parity visible: sigils, `#` names, `/me`, topics.

## 6. Constraints & non-goals

- **Desktop-first** (min ~1024×640; design at 1440×900). Runs as a native
  Rust app (iced/wgpu): favor solid fills, simple borders/rounding, and
  layout achievable with rows/columns/scrollables — no blur-heavy
  glassmorphism or web-only effects as load-bearing elements.
- Dark theme only for now (the palette *is* the brand).
- Keyboard-friendly: composer focus, Enter-to-send, visible focus rings.
- Non-goals: mobile layouts, threads/replies, reactions, rich embeds,
  server/guild multi-tenancy (channels are flat), light theme.

## 7. How to prompt Claude Design with this brief

Paste this document, then ask, for example:

1. *"Using the mikall design brief above, design the **main chat shell** as a
   1440×900 desktop artboard: three panes, topic bar, unread badges, role
   sigils, a TOFU key-change alarm banner visible, and one inline file-offer
   card in the timeline. Use the miku_teal tokens exactly."*
2. *"Design the **call experience** as three artboards — incoming offer,
   active 4-person mesh call with one screen-sharer, and the 8-tile maximum
   grid — following the brief's states and honesty labels."*
3. *"Design the **onboarding screen** and **settings screen** from the brief,
   making the fingerprint feel like a collectible identity card and the IRC
   gateway section feel technical-but-friendly."*

---

*Source of truth: this brief mirrors the shipped app — palette from
`crates/mikall-ui/src/theme.rs`, screens from `crates/mikall-ui/src/main.rs`,
states from `crates/mikall-domain` (`TrustLevel`, `PresenceState`,
`CallPhase`, `TransferPhase`, roles), and copy rules from
`docs/security.md`.*
