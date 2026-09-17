package rabbitmq

import (
	"context"
	"fmt"
	"log"
	"sync"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

const consumerTag = "queue-consumer"

type BrokerClient struct {
	mu        sync.Mutex
	consuming bool
	stopping  bool
	conn    *amqp.Connection
	channel *amqp.Channel
	closed  bool
}

func NewBrokerClient(settings m.ConnSettings) (*BrokerClient, error) {
	address := fmt.Sprintf("amqp://guest:guest@%s:%d/", settings.Hostname, settings.Port)
	// Conexion con RabbitMQ
	conn, err := amqp.Dial(address)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}

	c := &BrokerClient{conn: conn}
	c.channel, err = conn.Channel()
	if err != nil {
		if closeErr := c.Close(); closeErr != nil {
			return nil, fmt.Errorf("open channel: %w; cleanup: %w", err, closeErr)
		}
		return nil, fmt.Errorf("open channel: %w", err)
	}
	return c, nil
}

func (c *BrokerClient) DeclareQueue(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	_, err := c.channel.QueueDeclare(
		name,  // name
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	return err
}

func (c *BrokerClient) DeclareExchange(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	return c.channel.ExchangeDeclare(
		name,                // name
		amqp.ExchangeDirect, // type
		false,               // durable
		false,               // auto-deleted
		false,               // internal
		false,               // no-wait
		nil,                 // arguments
	)
}

func (c *BrokerClient) Publish(exchange string, keys []string, msg m.Message) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed || c.conn.IsClosed() {
		return fmt.Errorf("send: %w", m.ErrMessageMiddlewareDisconnected)
	}

	// Cada key recibe una publicaciom, si alguna falla, las anteriores ya se enviaron
	for _, key := range keys {
		err := c.channel.PublishWithContext(context.Background(),
			exchange, // exchange
			key,    // routing key
			false,  // mandatory
			false,  // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(msg.Body),
			})
		if err != nil {
			if c.conn.IsClosed() {
				return fmt.Errorf("send key %q: %w: %w", key, m.ErrMessageMiddlewareDisconnected, err)
			}
			return fmt.Errorf("send key %q: %w: %w", key, m.ErrMessageMiddlewareMessage, err)
		}
		log.Printf("exchange %q: published without error, key=%q, body=%q", exchange, key, msg.Body)
	}
	return nil
}

func (c *BrokerClient) StartConsuming(queueName string, callback func(m.Message, func(), func())) error {
	if callback == nil {
		return fmt.Errorf("consume: %w: nil callback", m.ErrMessageMiddlewareMessage)
	}
	
	messages, err := c.startConsumer(queueName)
	if err != nil {
		return err
	}

	defer func() {
		c.mu.Lock()
		c.consuming = false
		c.mu.Unlock()
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
		log.Printf("queue %q: recibe body=%q", queueName, delivery.Body)
		callback(m.Message{Body: string(delivery.Body)}, ack, nack)

		if confirmationErr != nil {
			c.mu.Lock()
			closed := c.closed
			c.mu.Unlock()

			if closed {
				// El cierre local corta la confirmacion
				// RabbitMQ reencola las entregas que quedaron sin confirmar al cerrar el canal
				return nil
			}

			if c.conn.IsClosed() {
				return fmt.Errorf("confirm delivery: %w: %w", m.ErrMessageMiddlewareDisconnected, confirmationErr)
			}
			return fmt.Errorf("confirm delivery: %w: %w", m.ErrMessageMiddlewareMessage, confirmationErr)
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	
	if c.closed || c.stopping {
		return nil
	}
	if c.conn.IsClosed() {
		return fmt.Errorf("consume: %w: deliveries channel closed", m.ErrMessageMiddlewareDisconnected)
	}
	return fmt.Errorf("consume: %w: deliveries channel closed", m.ErrMessageMiddlewareMessage)
}

func (c *BrokerClient) startConsumer(queueName string) (<-chan amqp.Delivery, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	if c.closed || c.conn.IsClosed() {
		return nil, fmt.Errorf("consume: %w", m.ErrMessageMiddlewareDisconnected)
	}
	if c.consuming {
		return nil, fmt.Errorf("consume: %w: consumer already running", m.ErrMessageMiddlewareMessage)
	}

	// Canal AMQP
	err := c.channel.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		if c.conn.IsClosed() {
			return nil, fmt.Errorf("set qos: %w: %w", m.ErrMessageMiddlewareDisconnected, err)
		}
		return nil, fmt.Errorf("set qos: %w: %w", m.ErrMessageMiddlewareMessage, err)
	}

	// Canal de go donde recibo los mensajes
	messages, err := c.channel.Consume(
		queueName,      // queue
		consumerTag, // consumer
		false,       // auto-ack
		false,       // exclusive
		false,       // no-local
		false,       // no-wait
		nil,         // args
	)
	if err != nil {
		if c.conn.IsClosed() {
			return nil, fmt.Errorf("consume: %w: %w", m.ErrMessageMiddlewareDisconnected, err)
		}
		return nil, fmt.Errorf("consume: %w: %w", m.ErrMessageMiddlewareMessage, err)
	}

	c.consuming = true
	c.stopping = false
	return messages, nil
}

func (c *BrokerClient) StopConsuming() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	if c.closed {
		return nil
	}
	if c.conn.IsClosed() {
		return fmt.Errorf("stop consuming: %w", m.ErrMessageMiddlewareDisconnected)
	}
	if !c.consuming || c.stopping {
		return nil
	}

	// Cancel cierra el canal
	// no espero al callback con el lock tomado
	if err := c.channel.Cancel(consumerTag, false); err != nil {
		if c.conn.IsClosed() {
			return fmt.Errorf("stop consuming: %w: %w", m.ErrMessageMiddlewareDisconnected, err)
		}
		return fmt.Errorf("stop consuming: %w: %w", m.ErrMessageMiddlewareMessage, err)
	}
	c.stopping = true
	return nil
}

func (c *BrokerClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}
	c.closed = true

	// Si no se pudo abrir el canal, solo hay una conex
	if c.channel == nil {
		if err := c.conn.Close(); err != nil {
			return fmt.Errorf("close connection: %w: %w", m.ErrMessageMiddlewareClose, err)
		}
		return nil
	}

	// Intento cerrar ambos
	channelErr := c.channel.Close()
	connectionErr := c.conn.Close()
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
