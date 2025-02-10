package inmemory

type InMemory struct{}

func (b *InMemory) Publish(message string) error {
	// Implement publishing logic here
	return nil
}

func NewInMemoryBroker() *InMemory {
	return &InMemory{}
}
