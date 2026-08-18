Feature: Direct messages
  DMs are end-to-end encrypted on the wire (crypto adapter); the domain
  guarantees a thread is one unordered pair of distinct identities.

  Scenario: A DM arrives and both sides share the thread history
    Given peers "miku" and "rin" are online
    When "miku" sends the direct message "konnichiwa" to "rin"
    Then "rin" has the direct message "konnichiwa" from "miku"
    And "miku" sees 1 message in the DM thread with "rin"
    And "rin" sees 1 message in the DM thread with "miku"

  Scenario: A DM conversation is causally ordered on both ends
    Given peers "miku" and "rin" are online
    When "miku" sends the direct message "one" to "rin"
    And "rin" sends the direct message "two" to "miku"
    And "miku" sends the direct message "three" to "rin"
    Then "rin" sees the DM history with "miku" as "one, two, three"
    And "miku" sees the DM history with "rin" as "one, two, three"
