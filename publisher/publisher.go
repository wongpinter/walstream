package publisher

type Publisher interface {
	Publish(message string) error
}
