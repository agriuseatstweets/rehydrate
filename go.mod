module github.com/agriuseatstweets/rehydrate

go 1.13

require (
	github.com/agriuseatstweets/go-pubbers v0.0.0-20200128145300-9d90850e1c78
	github.com/confluentinc/confluent-kafka-go v1.3.0
	github.com/dghubble/go-twitter v0.0.0-20190719072343-39e5462e111f
	github.com/dghubble/oauth1 v0.6.0
)

replace github.com/dghubble/go-twitter => github.com/nandanrao/go-twitter v0.0.0-20200128113326-4705266675e9
