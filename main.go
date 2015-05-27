package main

import (
	"flag"
	"fmt"
	"github.com/bradhe/epee"
	"github.com/golang/protobuf/proto"
)

const (
	// This is the topic that the stream processor will listen to.
	TopicName = "my-topic"

	// This is the partition that the processor will listen on.
	Partition = 1

	// NOTE: This must be unique to the topic and partition the stream processor is
	// going to consume.
	DefaultClientID = "my-client-1"
)

var (
	stream *epee.Stream

	// Parameterize where Zookeeper lives.
	ZookeeperHost = flag.String("zookeeper-host", "localhost:2181", "zookeeper host")
)

// This type encapsulates the stream processor and will implement the
// epee.StreamProcessor interface.
type MyStreamProcessor struct {
	Total int64
}

// The process method is called once for each message in the queue. If the message
// is successfully processed the related offset will be marked as "processed" so
// that when clients resume later this message doesn't get re-processed.
func (sp *MyStreamProcessor) Process(offset int64, message proto.Message) error {
	counter, ok := message.(*MyCounter)

	if !ok {
		return fmt.Errorf("failed to convert message to application-native type")
	}

	sp.Total += counter.GetCount()
	return nil
}

// The flush method will be periodically called (once every 10 seconds by
// default). This method is used to flush the processor's state so the jobs can
// be resumed if something goes wrong.
func (sp *MyStreamProcessor) Flush() error {
	// TODO: Flush the total to something here.
	return nil
}

func init() {
	// Parse CLI flags.
	flag.Parse()

	zk, err := epee.NewZookeeperClient([]string{*ZookeeperHost})

	if err != nil {
		panic(err)
	}

	// Assuming your Kafka brokers are registered in Zookeeper...
	stream, err = epee.NewStreamFromZookeeper(DefaultClientID, zk)

	if err != nil {
		panic(err)
	}

	// This tells the stream how to deserialize the message in Kafka.
	stream.Register(TopicName, &MyCounter{})
}

func main() {
	stream.Stream(TopicName, Partition, &MyStreamProcessor{})

	// The stream processor is now running in a goroutine in the background. The
	// main thread can continue doing whatever, or we can just sit here and wait.
	stream.Wait()
}
