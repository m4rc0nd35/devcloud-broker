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
			if success := callback(d.Body); !success {
				if err := channel.Nack(d.DeliveryTag, false, requeue); err != nil {
					log.Println(err)
				}
			} else {
				channel.Ack(d.DeliveryTag, false)
			}
		}
	}()

	return nil
}
