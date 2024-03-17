# Dev Cloud Broker

### Exemple
```go
amqp := rabbitmq.NewBroker(os.Getenv("AMQP_URI"), os.Getenv("AMQP_NAME"))
amqp.ConnectTCP()
amqp.Reconnect()
amqp.QueueDeclare("queue name")

amqp.Publish("queue name", "body") 

amqp.Consumer("queue name", "consumer name", 10, false, email.WorkerEmail)", "consumer name", 10, false, email.WorkerEmail)
```