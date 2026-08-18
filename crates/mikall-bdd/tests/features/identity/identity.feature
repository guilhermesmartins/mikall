Feature: Identity is a keypair
  On a serverless network an identity is a keypair and nothing more.
  Names are labels; trust is per-contact via TOFU pinning or explicit
  out-of-band fingerprint verification.

  Scenario: A fresh node has a human-checkable fingerprint
    Given a fresh node "miku"
    Then "miku" has a fingerprint of eight groups of four characters

  Scenario: First contact is TOFU-pinned
    Given peers "miku" and "rin" are online and joined "#stage"
    When "rin" posts "hello" in "#stage"
    Then "miku" trusts "rin" at level "tofu"

  Scenario: Out-of-band verification upgrades trust
    Given peers "miku" and "rin" are online and joined "#stage"
    When "rin" posts "hello" in "#stage"
    And "miku" verifies "rin"'s fingerprint out of band
    Then "miku" trusts "rin" at level "verified"

  Scenario: A changed key collapses trust and raises the alarm
    Given peers "miku" and "rin" are online and joined "#stage"
    When "rin" posts "hello" in "#stage"
    And "miku" verifies "rin"'s fingerprint out of band
    And "miku" observes a changed key for "rin"
    Then "miku" trusts "rin" at level "unverified"
    And "miku" saw a key-change alarm for "rin"

  Scenario: Blocking silences a peer
    Given peers "miku" and "rin" are online
    When "miku" blocks "rin"
    And "rin" sends the direct message "yo" to "miku"
    Then "miku" has no direct messages from "rin"
