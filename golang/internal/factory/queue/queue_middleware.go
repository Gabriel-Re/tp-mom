package queue

import (
	"fmt"
	//"log"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	"github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/factory/rabbitmq"
)

type QueueMiddleware struct {
	name string
	brokerClient *rabbitmq.BrokerClient
}

func NewQueueMiddleware(name string, settings m.ConnSettings) (m.Middleware, error) {
	// Abro un canal para trabajar con la cola y declaro la cola
	brokerClient, err := rabbitmq.NewBrokerClient(settings)
	if err != nil {
		return nil, err
	}

	q := &QueueMiddleware{name: name, brokerClient: brokerClient}

	err = brokerClient.DeclareQueue(name)

	if err != nil {
		if closeErr := q.Close(); closeErr != nil {
			return nil, fmt.Errorf("set up queue: %w; cleanup: %w", err, closeErr)
		}
		return nil, fmt.Errorf("set up queue: %w", err)
	}
	return q, nil
}

func (q *QueueMiddleware) Send(msg m.Message) error {
	if err := q.brokerClient.Publish("", []string{q.name}, msg); err != nil {
		return err
	}
	//log.Printf("queue %q: published without error, body=%q", q.name, msg.Body)
	return nil
}

func (q *QueueMiddleware) StartConsuming(callback func(m.Message, func(), func())) error {
	return q.brokerClient.StartConsuming(q.name, callback)
}

func (q *QueueMiddleware) StopConsuming() error {
	return q.brokerClient.StopConsuming()
}

func (q *QueueMiddleware) Close() error {
	return q.brokerClient.Close()
}
