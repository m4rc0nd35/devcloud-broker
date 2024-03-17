# Dev Cloud Broker

### Exemple
```go
import (
	"github.com/m4rc0nd35/devcloud-broker/broker"
)

func main() {
    amqp := broker.NewBroker("amqp://guest:guest@localhost:5672", "local_dev")
    amqp.ConnectTCP()
    amqp.Reconnect()
    amqp.QueueDeclare("queue name")

    amqp.Publish("queue name", "body") 

    amqp.Consumer("queue name", "consumer name", 10, false, func(payload []byte) bool {
        return true // ACK
    })
}
```