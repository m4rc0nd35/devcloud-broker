package broker

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueDeclareRetry struct {
	Queue        string
	RoutingKey   string
	Exchange     string
	DelaySeconds int64
}

type rabbitMQ struct {
	Connection *amqp.Connection
	tls        bool
	url        string
	name       string
	ca         []byte
	certs      []byte
	key        []byte
}

func NewBroker(url, name string) *rabbitMQ {
	return &rabbitMQ{
		url:  url,
		name: name,
	}
}

func (c *rabbitMQ) SetCerts(ca, certs, key []byte) {
	c.ca = ca
	c.certs = certs
	c.key = key
}

func (c *rabbitMQ) ConnectTCP() bool {
	var err error
	c.tls = false
	cfg := amqp.Config{
		Properties: amqp.Table{
			"connection_name": c.name,
		},
	}

	/* Connect AMQP */
	c.Connection, err = amqp.DialConfig(c.url, cfg)
	if err != nil {
		return false
	}

	return !c.Connection.IsClosed()
}

func (c *rabbitMQ) ConnectTLS() bool {
	var err error
	c.tls = true
	cert, err := tls.X509KeyPair(c.certs, c.key)
	if err != nil {
		log.Println("X509KeyPair ERROR: ", err)
	}

	rootCAs := x509.NewCertPool()
	rootCAs.AppendCertsFromPEM(c.ca)

	tlsConf := &tls.Config{
		RootCAs:      rootCAs,
		Certificates: []tls.Certificate{cert},
	}

	cfg := amqp.Config{
		Properties: amqp.Table{
			"connection_name": c.name,
		},
		TLSClientConfig: tlsConf,
	}

	/* Connect AMQP */
	c.Connection, err = amqp.DialConfig(c.url, cfg)
	if err != nil {
		c.Connection.Close()
		return false
	}

	return c.Connection.IsClosed()
}

func (c *rabbitMQ) Reconnect() {
	go func() {
		for {
			time.Sleep(time.Second * 30)
			if c.Connection.IsClosed() {
				if c.tls {
					c.ConnectTLS()
				} else {
					c.ConnectTCP()
				}
			}
		}
	}()
}

func (c *rabbitMQ) QueueDeclare(queue string) error {
	/* Check connection AMQP */
	if c.Connection.IsClosed() {
		log.Println("Connection is closed")
		return errors.New("connection is closed")
	}

	channel, err := c.Connection.Channel()
	if err != nil {
		return err
	}
	defer channel.Close()

	args := amqp.Table{}
	// args["x-expires"] = 300000

	_, err = channel.QueueDeclare(queue, true, false, false, false, args)
	if err != nil {
		channel.Close()
		return fmt.Errorf("QueueDeclare: %s", err)
	}

	return nil
}

func (c *rabbitMQ) QueueDeclareExchange(queue, routingKey, exchange string, durable, autoDelete bool) error {
	channel, err := c.Connection.Channel()
	if err != nil {
		return err
	}
	defer channel.Close()

	_, err = channel.QueueDeclare(queue, durable, autoDelete, true, false, nil)
	if err != nil {
		return fmt.Errorf("QueueDeclare: %w", err)
	}

	if routingKey != "" {
		err = channel.QueueBind(queue, routingKey, exchange, false, nil)
		if err != nil {
			channel.Close()
			return fmt.Errorf("QueueBind: %s", err)
		}
	}

	return nil
}

func (c *rabbitMQ) PublishExchange(exchange, routingKey string, txt string) error {
	/* Check connection AMQP */
	if c.Connection.IsClosed() {
		return errors.New("connection is clsosed")
	}

	channel, err := c.Connection.Channel()
	if err != nil {
		return err
	}
	defer channel.Close()

	err = channel.PublishWithContext(context.TODO(), exchange, routingKey, false, false,
		amqp.Publishing{ContentType: "application/json", Body: []byte(txt)})
	if err != nil {
		channel.Close()
		return fmt.Errorf("publish: %s", err)
	}

	return nil
}

// QueueDeclareRetry declares a main durable queue, and a corresponding retry queue with dead-letter configuration.
// The retry queue uses a TTL (specified by delayQueue in milliseconds). After this TTL, messages are dead-lettered to the main exchange and routing key.
// - queue: the main queue name
// - routingKey: the routing key for binding and dead-letter
// - exchange: the main exchange (must exist or will be created as a durable direct exchange)
// - delayQueue: message TTL for the retry queue in milliseconds
func (c *rabbitMQ) QueueDeclareRetry(payload QueueDeclareRetry) error {
	/* Check connection AMQP */
	if c.Connection.IsClosed() {
		log.Println("Connection is closed")
		return errors.New("connection is closed")
	}

	channel, err := c.Connection.Channel()
	if err != nil {
		return err
	}
	defer channel.Close()

	err = channel.ExchangeDeclare(
		payload.Exchange,
		"direct",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		channel.Close()
		return fmt.Errorf("ExchangeDeclare: %s", err)
	}

	_, err = channel.QueueDeclare(payload.Queue, true, false, false, false, nil)
	if err != nil {
		channel.Close()
		return fmt.Errorf("QueueDeclare: %s", err)
	}

	err = channel.QueueBind(payload.Queue, payload.RoutingKey, payload.Exchange, false, nil)
	if err != nil {
		channel.Close()
		return fmt.Errorf("QueueBind: %s", err)
	}

	// Retry 5s
	_, err = channel.QueueDeclare(
		fmt.Sprintf("%s.retry", payload.Queue),
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-message-ttl":             payload.DelaySeconds * 1000,
			"x-dead-letter-exchange":    payload.Exchange,
			"x-dead-letter-routing-key": payload.RoutingKey,
		},
	)

	// DLQ final
	_, err = channel.QueueDeclare(
		fmt.Sprintf("%s.dlq", payload.Queue),
		true,
		false,
		false,
		false,
		nil,
	)

	return nil
}

func (c *rabbitMQ) Publish(queue string, txt string) error {
	/* Check connection AMQP */
	if c.Connection.IsClosed() {
		time.Sleep(time.Second * 1)
		return errors.New("connection is clsosed")
	}

	channel, err := c.Connection.Channel()
	if err != nil {
		return err
	}
	defer channel.Close()

	err = channel.PublishWithContext(context.TODO(), "", queue, false, false, amqp.Publishing{ContentType: "application/json", Body: []byte(txt)})
	if err != nil {
		channel.Close()
		return fmt.Errorf("publish: %s", err)
	}

	return nil
}

func (c *rabbitMQ) Consumer(queue, consumerName string, prefetch int, requeue bool, callback func([]byte) bool) error {

	go func() {
		for {
			if c.Connection.IsClosed() {
				time.Sleep(time.Second)
				continue
			}

			ch, err := c.Connection.Channel()
			if err != nil {
				time.Sleep(time.Second)
				continue
			}

			ch.Qos(prefetch, 0, false)

			msgs, err := ch.Consume(queue, consumerName, false, false, false, false, nil)
			if err != nil {
				ch.Close()
				time.Sleep(time.Second)
				continue
			}

			closeChan := make(chan *amqp.Error)
			ch.NotifyClose(closeChan)

			log.Println("consumer started")

			// processamento
			done := make(chan bool)

			go func() {
				for d := range msgs {
					success := callback(d.Body)

					if success {
						d.Ack(false)
					} else {
						d.Nack(false, requeue)
					}
				}
				done <- true
			}()

			// espera morrer
			select {
			case err := <-closeChan:
				log.Println("channel closed", err)
			case <-done:
				log.Println("delivery channel closed")
			}

			ch.Close()
			time.Sleep(time.Second)
		}
	}()
	return nil
}

func (c *rabbitMQ) ConsumerRetry(queue, consumerName string, retryQueue int64, prefetch int, requeue bool, callback func([]byte) bool) error {

	go func() {
		for {
			if c.Connection.IsClosed() {
				time.Sleep(time.Second)
				continue
			}

			ch, err := c.Connection.Channel()
			if err != nil {
				time.Sleep(time.Second)
				continue
			}

			ch.Qos(prefetch, 0, false)

			msgs, err := ch.Consume(queue, consumerName, false, false, false, false, nil)
			if err != nil {
				ch.Close()
				time.Sleep(time.Second)
				continue
			}

			closeChan := make(chan *amqp.Error)
			ch.NotifyClose(closeChan)

			log.Println("consumer started")

			// processamento
			done := make(chan bool)

			go func() {
				for d := range msgs {
					retryCount := int64(0)
					if val, ok := d.Headers["x-retry-count"]; ok {
						retryCount = val.(int64)
					}

					success := callback(d.Body)

					if !success && retryCount < retryQueue {
						err = ch.PublishWithContext(context.TODO(), "", fmt.Sprintf("%s.retry", queue), false, false, amqp.Publishing{
							ContentType: "application/json",
							Body:        d.Body,
							Headers: amqp.Table{
								"x-retry-count": retryCount + 1,
							},
						})
						if err != nil {
							log.Printf("Erro ao publicar mensagem: %s", err)
						}
					} else if !success && retryCount >= retryQueue {
						err = ch.PublishWithContext(context.TODO(), "", fmt.Sprintf("%s.dlq", queue), false, false, amqp.Publishing{
							ContentType: "application/json",
							Body:        d.Body,
							Headers: amqp.Table{
								"x-queue-name":  queue,
								"x-routing-key": d.RoutingKey,
								"x-exchange":    d.Exchange,
							},
						})
						if err != nil {
							log.Printf("Erro ao publicar mensagem: %s", err)
						}
					}

					d.Ack(false)
				}
				done <- true
			}()

			// espera morrer
			select {
			case err := <-closeChan:
				log.Println("channel closed", err)
			case <-done:
				log.Println("delivery channel closed")
			}

			ch.Close()
			time.Sleep(time.Second)
		}
	}()
	return nil
}

func (c *rabbitMQ) ConsumerOLD(queue, consumerName string, prefetch int, requeue bool, callback func([]byte) bool) error {
	/* Check connection AMQP */
	if c.Connection.IsClosed() {
		time.Sleep(time.Second * 1)
		return errors.New("connection is clsosed")
	}

	var err error

	channel, err := c.Connection.Channel()
	if err != nil {
		return err
	}

	channel.Qos(prefetch, 0, false)

	if err != nil {
		return err
	}

	msgs, err := channel.Consume(queue, consumerName, false, false, false, false, nil)

	if err != nil {
		return err
	}

	go func() {
		for d := range msgs {
			go func(delivery amqp.Delivery) {
				success := callback(delivery.Body)

				if !success {
					delivery.Nack(false, requeue)
				} else {
					delivery.Ack(false)
				}
			}(d)
		}
	}()

	return nil
}
