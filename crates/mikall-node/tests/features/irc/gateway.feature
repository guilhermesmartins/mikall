Feature: IRC gateway
  Every node embeds a real RFC 1459/2812 server on loopback so standard IRC
  clients drive the same use cases the GUI does.

  Scenario: WeeChat-style registration
    Given a node with the IRC gateway listening
    When an IRC client connects and registers as "miku"
    Then the client receives numeric 001
    And the client receives numeric 005 containing "NETWORK=mikall"
    And the client receives numeric 376

  Scenario: An invalid nickname is rejected by construction
    Given a node with the IRC gateway listening
    When an IRC client connects and sends nickname "9bad"
    Then the client receives numeric 432

  Scenario: JOIN and PRIVMSG flow through the P2P channel
    Given a node with the IRC gateway listening
    And an IRC client registered as "miku"
    When the client sends "JOIN #stage"
    Then the client receives a JOIN for "#stage"
    And the client receives numeric 331
    And the client receives numeric 353 containing "miku"
    And the client receives numeric 366
    When the client sends "PRIVMSG #stage :konnichiwa sekai"
    Then the node's channel "#stage" contains "konnichiwa sekai"

  Scenario: WHOIS shows the key fingerprint
    Given a node with the IRC gateway listening
    And an IRC client registered as "miku"
    When the client sends "WHOIS miku"
    Then the client receives numeric 320 containing "fingerprint"

  Scenario: A wrong PASS is refused
    Given a node with the IRC gateway requiring password "negi"
    When an IRC client connects with password "wrong" and registers as "miku"
    Then the client receives numeric 464

  Scenario: A correct PASS is accepted
    Given a node with the IRC gateway requiring password "negi"
    When an IRC client connects with password "negi" and registers as "miku"
    Then the client receives numeric 001

  Scenario: A P2P message reaches the IRC client
    Given two connected nodes where the second runs an IRC gateway
    And an IRC client registered as "rin"
    And the client joined "#stage" and the first node joined "#stage"
    When the first node posts "from the p2p side" in "#stage"
    Then the client receives a PRIVMSG in "#stage" saying "from the p2p side"
