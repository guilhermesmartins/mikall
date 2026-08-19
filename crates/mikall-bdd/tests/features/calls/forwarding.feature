Feature: Peer-elected share forwarding
  The sharer sends each frame exactly once, regardless of viewer count: a
  seated peer is elected forwarder — a peer ROLE, never a server — and
  fans the sealed bytes out. The sharer is never the forwarder, a
  forwarder is always seated, and losing the forwarder never ends the
  share: direct fan-out is the standing fallback.

  Scenario: Sharing in a room elects the lowest-identity viewer
    Given a fresh node "miku"
    When "miku" starts a call and 3 peers join it
    And "miku" shares the screen in the call
    Then the elected forwarder is the lowest-identity non-sharer peer

  Scenario: The forwarder leaving re-elects a stand-in
    Given a fresh node "miku"
    When "miku" starts a call and 3 peers join it
    And "miku" shares the screen in the call
    And the forwarder leaves the call
    Then the elected forwarder is the lowest-identity non-sharer peer

  Scenario: A two-party share has no forwarder
    Given a fresh node "miku"
    When "miku" starts a call and 1 peers join it
    And "miku" shares the screen in the call
    Then no forwarder is elected and the sharer fans out directly

  Scenario: Losing all but one viewer falls back to direct fan-out
    Given a fresh node "miku"
    When "miku" starts a call and 2 peers join it
    And "miku" shares the screen in the call
    And the forwarder leaves the call
    Then no forwarder is elected and the sharer fans out directly

  Scenario: Stopping the share retires the forwarder
    Given a fresh node "miku"
    When "miku" starts a call and 3 peers join it
    And "miku" shares the screen in the call
    And "miku" stops sharing the screen
    Then no forwarder is elected and the sharer fans out directly
