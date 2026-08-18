Feature: Deterministic ordering without a server
  Every node linearizes the same message DAG to the same display order,
  whatever order gossip delivered it in.

  Scenario: Concurrent posts converge to one order everywhere
    Given peers "miku", "rin" and "len" are online and joined "#stage"
    And network delivery between "miku" and "rin" is delayed
    When "miku" posts "first" in "#stage"
    And "rin" posts "second" in "#stage"
    And network delivery between "miku" and "rin" is restored and held traffic is flushed
    Then all peers display the same history for "#stage"
    And "len" sees 2 messages in "#stage"

  Scenario: A gap is detected and healed when the missing parent arrives
    Given peers "miku" and "rin" are online and joined "#stage"
    And network delivery between "miku" and "rin" is delayed
    When "miku" posts "parent" in "#stage"
    And network delivery between "miku" and "rin" is restored without flushing
    And "miku" posts "child" in "#stage"
    Then "rin" reports a history gap in "#stage"
    When held traffic is flushed
    Then "rin" sees 2 messages in "#stage"
    And all peers display the same history for "#stage"
