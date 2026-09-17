package exchange

import (
	"fmt"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	"github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/factory/rabbitmq"
)

type ExchangeMiddleware struct {
	name string
	keys []string
	brokerClient *rabbitmq.BrokerClient
}

func NewExchangeMiddleware(name string, keys []string, settings m.ConnSettings) (m.Middleware, error) {
	// Abro un canal para trabajar con el exchange
	brokerClient, err := rabbitmq.NewBrokerClient(settings)
	if err != nil {
		return nil, err
	}

	keysCopy := make([]string, len(keys))
	copy(keysCopy, keys)
	
	e := &ExchangeMiddleware{name: name, keys: keysCopy, brokerClient: brokerClient}

	err = brokerClient.DeclareExchange(name)

	if err != nil {
		if closeErr := e.Close(); closeErr != nil {
			return nil, fmt.Errorf("set up exchange: %w; cleanup: %w", err, closeErr)
		}
		return nil, fmt.Errorf("set up exchange: %w", err)
	}
	return e, nil
}

func (e *ExchangeMiddleware) Send(msg m.Message) error {
	return e.brokerClient.Publish(e.name, e.keys, msg)
}

func (e *ExchangeMiddleware) StartConsuming(callback func(m.Message, func(), func())) error {
	// Creo la cola privada y los binds
	return e.brokerClient.StartExchangeConsuming(e.name, e.keys, callback)
}

func (e *ExchangeMiddleware) StopConsuming() error {
	return e.brokerClient.StopConsuming()
}

func (e *ExchangeMiddleware) Close() error {
	return e.brokerClient.Close()
}
