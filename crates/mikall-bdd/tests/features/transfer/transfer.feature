Feature: File transfer
  Files are content-addressed and chunked; every chunk is verified against
  the signed manifest before it counts, and corrupt chunks are re-fetched.

  Scenario: An accepted transfer completes when all chunks verify
    Given a fresh node "miku"
    And "miku" was offered the file "mix.flac" of 3 chunks by "rin"
    When "miku" accepts the transfer
    And chunk 0 arrives and verifies
    And chunk 1 arrives and verifies
    And chunk 2 arrives and verifies
    Then the transfer is complete

  Scenario: A corrupt chunk never counts and is re-fetched
    Given a fresh node "miku"
    And "miku" was offered the file "mix.flac" of 2 chunks by "rin"
    When "miku" accepts the transfer
    And chunk 0 arrives and verifies
    And chunk 1 arrives corrupted
    Then the transfer reports 1 missing chunk
    When chunk 1 arrives and verifies
    Then the transfer is complete

  Scenario: A rejected offer stays rejected
    Given a fresh node "miku"
    And "miku" was offered the file "mix.flac" of 2 chunks by "rin"
    When "miku" rejects the transfer
    Then accepting the transfer afterwards is rejected
