package exchange

import (
	"context"
	"fmt"
	"log"
	"sync"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type ExchangeMiddleware struct {
	mu      sync.Mutex
	name    string
	keys    []string
	conn    *amqp.Connection
	channel *amqp.Channel
	closed  bool
}

func NewExchangeMiddleware(name string, keys []string, settings m.ConnSettings) (m.Middleware, error) {
	address := fmt.Sprintf("amqp://guest:guest@%s:%d/", settings.Hostname, settings.Port)
	// Conexion con RabbitMQ
	conn, err := amqp.Dial(address)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	// Copio las keys para que cambios del llamador no modifiquen los destinos.
	keysCopy := make([]string, len(keys))
	copy(keysCopy, keys)
	e := &ExchangeMiddleware{name: name, keys: keysCopy, conn: conn}

	// Abro un canal para trabajar con el exchange
	e.channel, err = conn.Channel()
	if err == nil {
		err = e.channel.ExchangeDeclare(
			name,                // name
			amqp.ExchangeDirect, // type
			false,               // durable
			false,               // auto-deleted
			false,               // internal
			false,               // no-wait
			nil,                 // arguments
		)
	}

	if err != nil {
		if closeErr := e.Close(); closeErr != nil {
			return nil, fmt.Errorf("set up exchange: %w; cleanup: %w", err, closeErr)
		}
		return nil, fmt.Errorf("set up exchange: %w", err)
	}
	return e, nil
}

func (e *ExchangeMiddleware) Send(msg m.Message) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed || e.conn.IsClosed() {
		return fmt.Errorf("send: %w", m.ErrMessageMiddlewareDisconnected)
	}

	// Cada key recibe una publicaciom, si alguna falla, las anteriores ya se enviaron
	for _, key := range e.keys {
		err := e.channel.PublishWithContext(context.Background(),
			e.name, // exchange
			key,    // routing key
			false,  // mandatory
			false,  // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(msg.Body),
			})
		if err != nil {
			if e.conn.IsClosed() {
				return fmt.Errorf("send key %q: %w: %w", key, m.ErrMessageMiddlewareDisconnected, err)
			}
			return fmt.Errorf("send key %q: %w: %w", key, m.ErrMessageMiddlewareMessage, err)
		}
		log.Printf("exchange %q: published without error, key=%q, body=%q", e.name, key, msg.Body)
	}
	return nil
}

func (e *ExchangeMiddleware) StartConsuming(callback func(m.Message, func(), func())) error {
	// TODO: crear la cola privada y los binds
	return fmt.Errorf("consume: %w: exchange consumption not implemented", m.ErrMessageMiddlewareMessage)
}

func (e *ExchangeMiddleware) StopConsuming() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.closed && e.conn.IsClosed() {
		return fmt.Errorf("stop consuming: %w", m.ErrMessageMiddlewareDisconnected)
	}
	// Todavia no se registra ningun consumidor.
	return nil
}

func (e *ExchangeMiddleware) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return nil
	}
	e.closed = true

	// Si no se pudo abrir el canal, solo hay una conex
	if e.channel == nil {
		if err := e.conn.Close(); err != nil {
			return fmt.Errorf("close connection: %w: %w", m.ErrMessageMiddlewareClose, err)
		}
		return nil
	}

	// Intento cerrar ambos
	channelErr := e.channel.Close()
	connectionErr := e.conn.Close()

	if channelErr != nil && connectionErr != nil {
		return fmt.Errorf("%w: close channel: %w; close connection: %w", m.ErrMessageMiddlewareClose, channelErr, connectionErr)
	}
	if channelErr != nil {
		return fmt.Errorf("close channel: %w: %w", m.ErrMessageMiddlewareClose, channelErr)
	}
	if connectionErr != nil {
		return fmt.Errorf("close connection: %w: %w", m.ErrMessageMiddlewareClose, connectionErr)
	}
	return nil
}
