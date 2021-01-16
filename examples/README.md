## Configuration examples

This folder contains various configuration examples, meant to help composing your `~/.kaf/config` file.

## Cheat-sheet
Here are the examples from the [main README](../README.md).

Add a local Kafka with no auth

`kaf config add-cluster local -b localhost:9092`

Select cluster from dropdown list

`kaf config select-cluster`

Describe and List nodes

`kaf node ls`

List topics, partitions and replicas

`kaf topics`

Describe a given topic

`kaf topics describe mqtt.messages.incoming`

List consumer groups

`kaf groups`

Describe a given consumer group

`kafa group describe dispatcher`

Write message into given topic from stdin

`echo test | kaf produce mqtt.messages.incoming`
