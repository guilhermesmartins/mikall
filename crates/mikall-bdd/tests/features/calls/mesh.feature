Feature: Mesh call limit
  Group calls are a full mesh — an SFU would be a server. The 8-participant
  ceiling is a domain invariant, not a UI suggestion.

  Scenario: A ninth participant is unrepresentable
    Given a fresh node "miku"
    When "miku" starts a call and 7 peers join it
    Then the call has 8 participants
    And a 9th participant is rejected with the mesh-limit error

  Scenario: Declining ends a ringing call
    Given a fresh node "miku"
    When "miku" starts a call to "rin"
    And the callee declines the call
    Then the call is ended

  Scenario: The call lifecycle rejects illegal transitions
    Given a fresh node "miku"
    When "miku" starts a call to "rin"
    And the callee accepts the call
    Then accepting the call again is rejected
