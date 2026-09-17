package queue

import (
	"context"
	"fmt"
	"log"
	"sync"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

const consumerTag = "queue-consumer"

type QueueMiddleware struct {
	mu        sync.Mutex
	consuming bool
	stopping  bool
	name    string
	conn    *amqp.Connection
	channel *amqp.Channel
	closed  bool
}

func NewQueueMiddleware(name string, settings m.ConnSettings) (m.Middleware, error) {
	address := fmt.Sprintf("amqp://guest:guest@%s:%d/", settings.Hostname, settings.Port)
	// Conexion con RabbitMQ
	conn, err := amqp.Dial(address)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}

	q := &QueueMiddleware{name: name, conn: conn}

	// Abro un canal para trabajar con la cola y declaro la cola
	q.channel, err = conn.Channel()
	if err == nil {
		_, err = q.channel.QueueDeclare(
			name,  // name
			false, // durable
			false, // delete when unused
			false, // exclusive
			false, // no-wait
			nil,   // arguments
		)
	}

	if err != nil {
		if closeErr := q.Close(); closeErr != nil {
			return nil, fmt.Errorf("set up queue: %w; cleanup: %w", err, closeErr)
		}
		return nil, fmt.Errorf("set up queue: %w", err)
	}
	return q, nil
}

func (q *QueueMiddleware) Send(msg m.Message) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed || q.conn.IsClosed() {
		return fmt.Errorf("send: %w", m.ErrMessageMiddlewareDisconnected)
	}

	err := q.channel.PublishWithContext(context.Background(),
		"",     // exchange
		q.name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(msg.Body),
		})
	if err != nil {
		if q.conn.IsClosed() {
			return fmt.Errorf("send: %w: %w", m.ErrMessageMiddlewareDisconnected, err)
		}
		return fmt.Errorf("send: %w: %w", m.ErrMessageMiddlewareMessage, err)
	}

	log.Printf("queue %q: published without error, body=%q", q.name, msg.Body)
	return nil
}

func (q *QueueMiddleware) StartConsuming(callback func(m.Message, func(), func())) error {
	if callback == nil {
		return fmt.Errorf("consume: %w: nil callback", m.ErrMessageMiddlewareMessage)
	}
	
	messages, err := q.startConsumer()
	if err != nil {
		return err
	}

	defer func() {
		q.mu.Lock()
		q.consuming = false
		q.mu.Unlock()
	}()

	for delivery := range messages {
		// Callback debe llamar a ack o nack antes de retornar, en la misma goroutine
		var confirmationErr error
		ack := func() {
			if confirmationErr == nil {
				confirmationErr = delivery.Ack(false)
			}
		}
		nack := func() {
			if confirmationErr == nil {
				// Reencolo solo esta entrega para que pueda procesarse otra vez.
				confirmationErr = delivery.Nack(false, true)
			}
		}
		log.Printf("queue %q: recibe body=%q", q.name, delivery.Body)
		callback(m.Message{Body: string(delivery.Body)}, ack, nack)

		if confirmationErr != nil {
			q.mu.Lock()
			closed := q.closed
			q.mu.Unlock()

			if closed {
				// El cierre local interrumpio la confirmacion.
				// RabbitMQ reencola las entregas que quedaron sin confirmar al cerrar el canal
				return nil
			}

			if q.conn.IsClosed() {
				return fmt.Errorf("confirm delivery: %w: %w", m.ErrMessageMiddlewareDisconnected, confirmationErr)
			}
			return fmt.Errorf("confirm delivery: %w: %w", m.ErrMessageMiddlewareMessage, confirmationErr)
		}
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	
	if q.closed || q.stopping {
		return nil
	}
	if q.conn.IsClosed() {
		return fmt.Errorf("consume: %w: deliveries channel closed", m.ErrMessageMiddlewareDisconnected)
	}
	return fmt.Errorf("consume: %w: deliveries channel closed", m.ErrMessageMiddlewareMessage)
}

func (q *QueueMiddleware) startConsumer() (<-chan amqp.Delivery, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	
	if q.closed || q.conn.IsClosed() {
		return nil, fmt.Errorf("consume: %w", m.ErrMessageMiddlewareDisconnected)
	}
	if q.consuming {
		return nil, fmt.Errorf("consume: %w: consumer already running", m.ErrMessageMiddlewareMessage)
	}

	// Canal AMQP
	err := q.channel.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		if q.conn.IsClosed() {
			return nil, fmt.Errorf("set qos: %w: %w", m.ErrMessageMiddlewareDisconnected, err)
		}
		return nil, fmt.Errorf("set qos: %w: %w", m.ErrMessageMiddlewareMessage, err)
	}

	// Canal de go donde recibo los mensajes
	messages, err := q.channel.Consume(
		q.name,      // queue
		consumerTag, // consumer
		false,       // auto-ack
		false,       // exclusive
		false,       // no-local
		false,       // no-wait
		nil,         // args
	)
	if err != nil {
		if q.conn.IsClosed() {
			return nil, fmt.Errorf("consume: %w: %w", m.ErrMessageMiddlewareDisconnected, err)
		}
		return nil, fmt.Errorf("consume: %w: %w", m.ErrMessageMiddlewareMessage, err)
	}

	q.consuming = true
	q.stopping = false
	return messages, nil
}

func (q *QueueMiddleware) StopConsuming() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	
	if q.closed {
		return nil
	}
	if q.conn.IsClosed() {
		return fmt.Errorf("stop consuming: %w", m.ErrMessageMiddlewareDisconnected)
	}
	if !q.consuming || q.stopping {
		return nil
	}

	// Cancel cierra el canal
	// no espero al callback con el lock tomado
	if err := q.channel.Cancel(consumerTag, false); err != nil {
		if q.conn.IsClosed() {
			return fmt.Errorf("stop consuming: %w: %w", m.ErrMessageMiddlewareDisconnected, err)
		}
		return fmt.Errorf("stop consuming: %w: %w", m.ErrMessageMiddlewareMessage, err)
	}
	q.stopping = true
	return nil
}

func (q *QueueMiddleware) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return nil
	}
	q.closed = true

	// Si no se pudo abrir el canal, solo hay una conex
	if q.channel == nil {
		if err := q.conn.Close(); err != nil {
			return fmt.Errorf("close connection: %w: %w", m.ErrMessageMiddlewareClose, err)
		}
		return nil
	}

	// Intento cerrar ambos
	channelErr := q.channel.Close()
	connectionErr := q.conn.Close()
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
