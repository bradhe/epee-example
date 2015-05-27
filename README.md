Epee Example
===

A quick example of how to use Epee.

# Building

Pretty straight forward. You need `protoc` installed, though!

```bash
$ protoc --go_out=. messages.proto
$ go build
```

You'll need a lot more than that, though, if you want to actually *run* the
example. Namely, a Kafka cluster and a producer and...yeah.
