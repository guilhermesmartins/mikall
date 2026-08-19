Feature: Solo stage and mid-call invites
  Opening a call with nobody else there is legal — the opener holds the
  stage alone, active immediately, and rings people in as they arrive.
  Leaving is a choice: an invitee declining never tears down a stage
  someone still holds; only hanging up ends it for the opener.

  Scenario: Opening a call alone goes straight to active
    Given a fresh node "miku"
    When "miku" opens a solo call
    Then the call is still active
    And the call has 1 participant

  Scenario: An invited peer accepting joins the stage
    Given a fresh node "miku"
    When "miku" opens a solo call
    And "miku" invites "rin" to the call
    And the callee accepts the call
    Then the call is still active
    And the call has 2 participants

  Scenario: An invitee declining leaves the stage standing
    Given a fresh node "miku"
    When "miku" opens a solo call
    And "miku" invites "rin" to the call
    And the callee declines the call
    Then the call is still active
    And the call has 1 participant

  Scenario: Invites respect the mesh limit
    Given a fresh node "miku"
    When "miku" starts a call and 7 peers join it
    Then the call has 8 participants
    And inviting another peer is refused because the mesh is full
