package kafka

// Option is an optional configuration of this Gnomock preset. Use available
// Options to configure the container.
type Option func(*P)

// WithVersion sets image version.
func WithVersion(version string) Option {
	return func(o *P) {
		o.Version = version
	}
}

// WithTopics makes sure that the provided topics are available when Kafka is
// up and running.
func WithTopics(topics ...string) Option {
	return func(o *P) {
		o.Topics = append(o.Topics, topics...)
	}
}

// WithMessages makes sure that these messages can be consumed during the test
// once the container is ready.
func WithMessages(messages ...Message) Option {
	return func(o *P) {
		o.Messages = append(o.Messages, messages...)
	}
}

// WithMessagesFile allows to load messages to be sent into Kafka from one or
// multiple files.
func WithMessagesFile(files string) Option {
	return func(o *P) {
		o.MessagesFiles = append(o.MessagesFiles, files)
	}
}
