module github.com/agriuseatstweets/rehydrate

go 1.13

require (
	github.com/agriuseatstweets/go-pubbers v0.0.0-20200127095615-b190a89ac739
	github.com/confluentinc/confluent-kafka-go-dev v0.0.0-20190802212433-4315eca36bc9
	github.com/dghubble/go-twitter v0.0.0-20190719072343-39e5462e111f
	github.com/dghubble/oauth1 v0.6.0
	github.com/dghubble/sling v1.3.0
)

replace github.com/dghubble/go-twitter => github.com/nandanrao/go-twitter v0.0.0-20200127210325-a8c1c7474a9b
