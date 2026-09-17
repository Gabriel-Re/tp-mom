package factory

import (
	"github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/factory/exchange"
	"github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/factory/queue"
	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
)

func CreateQueueMiddleware(queueName string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	return queue.NewQueueMiddleware(queueName, connectionSettings)
}

func CreateExchangeMiddleware(name string, keys []string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	return exchange.NewExchangeMiddleware(name, keys, connectionSettings)
}
