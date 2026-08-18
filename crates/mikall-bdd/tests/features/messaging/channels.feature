Feature: Channel messaging
  Channels are gossip topics with a causally ordered message DAG instead of
  a server's authoritative history.

  Scenario: A message reaches all channel members
    Given peers "miku" and "rin" are online and joined "#stage"
    When "miku" posts "hello world" in "#stage"
    Then "rin" sees "hello world" in "#stage" from "miku"

  Scenario: The first joiner founds the channel
    Given peers "miku" and "rin" are online and joined "#stage"
    Then "miku" is the founder of "#stage"
    And "rin" is a member of "#stage"

  Scenario: Only operators may set the topic
    Given peers "miku" and "rin" are online and joined "#stage"
    When "rin" tries to set the topic of "#stage" to "rin was here"
    Then the topic change is rejected
    When "miku" sets the topic of "#stage" to "the world is mine"
    Then "rin" sees the topic "the world is mine" for "#stage"

  Scenario: The founder can grant operator status
    Given peers "miku" and "rin" are online and joined "#stage"
    When "miku" grants operator status to "rin" in "#stage"
    And "rin" sets the topic of "#stage" to "now I can"
    Then "miku" sees the topic "now I can" for "#stage"

  Scenario: An empty message is unrepresentable
    Given peers "miku" and "rin" are online and joined "#stage"
    When "miku" tries to post "" in "#stage"
    Then the message is rejected

  Scenario: A message with a carriage return is unrepresentable
    Given peers "miku" and "rin" are online and joined "#stage"
    When "miku" tries to post an IRC injection payload in "#stage"
    Then the message is rejected
