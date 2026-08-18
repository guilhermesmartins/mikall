Feature: Presence
  Presence is gossip-derived and best-effort: away states travel as beacons
  on joined channels.

  Scenario: Away state reaches channel peers
    Given peers "miku" and "rin" are online and joined "#stage"
    When "miku" sets away with message "rehearsal, brb"
    Then "rin" sees "miku" as away with message "rehearsal, brb"

  Scenario: Returning clears away
    Given peers "miku" and "rin" are online and joined "#stage"
    When "miku" sets away with message "rehearsal, brb"
    And "miku" clears away
    Then "rin" sees "miku" as online
